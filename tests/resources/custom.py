"""Buffered custom sender with per-session batching and at-least-once semantics."""
import asyncio
import contextlib
import hashlib
import os
from typing import Hashable

from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusSender

# ---------------------------------------------------------------------------
# Configuration (env)
# ---------------------------------------------------------------------------

_FAN_SIZE = int(os.getenv('CUSTOM_SESSION_ID_FAN_SIZE', '10'))
_MAX_WAIT_MS = int(os.getenv('CUSTOM_BUFFER_MAX_WAIT_MS', '25'))
_MAX_MSGS = int(os.getenv('CUSTOM_BUFFER_MAX_MESSAGES', '100'))
_SEED_KEY = os.getenv('CUSTOM_SESSION_PROPERTY_SEED', '__kafka_key').encode()

# One buffer per (sender, session_id) so that SDK batches stay homogeneous.
_buffers: dict[tuple[int, Hashable | None], 'MessageBuffer'] = {}
_buffers_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _session_from_seed(seed: str) -> str:
    """Map a seed string deterministically to a session_id in [0, _FAN_SIZE).

    Parameters
    ----------
    seed : str
        Input key (e.g., Kafka key) used to derive the session.

    Returns
    -------
    str
        Session identifier as a stringified integer.
    """
    # usedforsecurity=False keeps this FIPS-friendly for non-crypto hashing.
    i = int(hashlib.md5(seed.encode(), usedforsecurity=False).hexdigest(), 16)
    return str(i % _FAN_SIZE)


# ---------------------------------------------------------------------------
# Buffer
# ---------------------------------------------------------------------------

class MessageBuffer:
    """Per-(sender, session_id) coalescing buffer with per-item acks.

    Messages are held briefly to coalesce into larger Service Bus batches.
    Each enqueued message comes with a Future that is resolved once the batch
    containing that message has been successfully sent.

    Parameters
    ----------
    sender : ServiceBusSender
        The Service Bus sender used to transmit messages.
    max_wait_ms : int
        Maximum time window (milliseconds) to collect messages before flush.
    max_msgs : int
        Maximum number of messages to coalesce before flush.

    Notes
    -----
    - This design preserves at-least-once semantics: ``add_and_wait()`` only
      returns after the message's batch has been sent (or an exception is raised).
    - Batching is keyed by ``(sender, session_id)`` upstream, so each batch is
      homogeneous with respect to session_id, which the SDK requires.
    """

    def __init__(self, sender: ServiceBusSender, max_wait_ms: int, max_msgs: int):
        self.sender = sender
        self.queue: asyncio.Queue[tuple[ServiceBusMessage, asyncio.Future] | None] = asyncio.Queue()
        self.max_wait = max_wait_ms / 1000.0
        self.max_msgs = max_msgs
        self._task = asyncio.create_task(self._run())

    async def add_and_wait(self, msg: ServiceBusMessage) -> None:
        """Enqueue a message and wait until it has been sent.

        Parameters
        ----------
        msg : ServiceBusMessage
            The message to send.

        Raises
        ------
        Exception
            Propagates any send failure from the background flush.
        """
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        await self.queue.put((msg, fut))
        await fut

    async def close(self) -> None:
        """Flush remaining messages and stop the background task."""
        await self.queue.put(None)
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        """Background loop: coalesce windows then flush."""
        while True:
            items = await self._coalesce_window()
            if items is None:
                # Sentinel observed: nothing more to read.
                await self._flush([])  # no-op; keeps structure symmetric
                break
            await self._flush(items)

    async def _coalesce_window(self) -> list[tuple[ServiceBusMessage, asyncio.Future]] | None:
        """Pull the first item, then coalesce until timeout or max size.

        Returns
        -------
        list[(ServiceBusMessage, Future)] | None
            The coalesced items to flush, or None if shutting down.
        """
        first = await self.queue.get()
        if first is None:
            return None

        items: list[tuple[ServiceBusMessage, asyncio.Future]] = [first]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.max_wait

        while len(items) < self.max_msgs:
            timeout = deadline - loop.time()
            if timeout <= 0:
                break
            try:
                nxt = await asyncio.wait_for(self.queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                break

            if nxt is None:
                # Re-enqueue sentinel for outer loop to observe and exit.
                self.queue.put_nowait(None)
                break

            items.append(nxt)

        return items

    async def _flush(self, items: list[tuple[ServiceBusMessage, asyncio.Future]]) -> None:
        """Send items as one or more Service Bus batches.

        Parameters
        ----------
        items : list of tuple(ServiceBusMessage, asyncio.Future)
            Items to send; each Future is resolved on success or set with an
            exception on failure.

        Notes
        -----
        Splits into multiple SDK batches if the first fills up; all Futures
        for a given batch are resolved together after ``send_messages``.
        """
        if not items:
            return

        batch = await self.sender.create_message_batch()
        pending: list[tuple[ServiceBusMessage, asyncio.Future]] = []

        async def flush_batch():
            if not pending:
                return
            try:
                await self.sender.send_messages(batch)
                for _, fut in pending:
                    if not fut.done():
                        fut.set_result(True)
            except Exception as exc:
                for _, fut in pending:
                    if not fut.done():
                        fut.set_exception(exc)

        for msg, fut in items:
            try:
                batch.add_message(msg)
                pending.append((msg, fut))
            except ValueError:
                # Batch full -> send current batch and start a new one.
                await flush_batch()
                batch = await self.sender.create_message_batch()
                batch.add_message(msg)
                pending = [(msg, fut)]

        await flush_batch()


# ---------------------------------------------------------------------------
# Buffer access & shutdown
# ---------------------------------------------------------------------------

async def _get_buffer(sender: ServiceBusSender, session_id: str | None = None) -> MessageBuffer:
    """Retrieve or create a buffer for a given (sender, session_id) pair.

    Parameters
    ----------
    sender : ServiceBusSender
        The sender for which to get a buffer.
    session_id : str, optional
        If provided, messages for that session are batched together. If None,
        all messages for this sender share the same buffer.

    Returns
    -------
    MessageBuffer
        The buffer associated with the given key.
    """
    key = (id(sender), session_id)
    buf = _buffers.get(key)
    if buf is not None:
        return buf
    async with _buffers_lock:
        buf = _buffers.get(key)
        if buf is None:
            buf = MessageBuffer(sender, _MAX_WAIT_MS, _MAX_MSGS)
            _buffers[key] = buf
        return buf


async def close() -> None:
    """Flush and close all active buffers (idempotent)."""
    if not _buffers:
        return
    await asyncio.gather(*(buf.close() for buf in list(_buffers.values())), return_exceptions=True)
    _buffers.clear()


# ---------------------------------------------------------------------------
# Public entrypoint used by the router
# ---------------------------------------------------------------------------

async def custom_sender(sender: ServiceBusSender, topic_name: str, message_body: object, application_properties: dict):
    """Custom sender for Service Bus messages with session assignment and batching.

    The function returns only after the message has been sent, preserving
    at-least-once semantics.

    Parameters
    ----------
    sender : ServiceBusSender
        The Service Bus sender to use for sending the message.
    topic_name : str
        Name of the destination topic (not used for logic; provided for parity).
    message_body : object
        The message body (str, bytes, or already-serialized JSON string).
    application_properties : dict
        Application properties from the original message. The seed property is
        read from this dict to derive the session ID.

    Raises
    ------
    Exception
        Any underlying send error is propagated to the caller.
    """
    # seed = str(application_properties.get(_SEED_KEY))
    # session_id = _session_from_seed(seed)
    session_id = None

    if sender.entity_name == 'ie.topic':
        session_id = '0'

    msg = ServiceBusMessage(
        body=message_body,
        application_properties=application_properties,
        session_id=session_id
    )

    buf = await _get_buffer(sender, session_id=session_id)
    await buf.add_and_wait(msg)
