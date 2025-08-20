"""
An example custom sender with batching and at-least-once semantics.

This module defines a buffered custom sender for Azure Service Bus that
computes a session ID from a Kafka key and batches outgoing messages
for efficiency, while still preserving at-least-once delivery semantics.

The router calls `custom_sender` once per message. The function will not
return until the message has been successfully sent to Service Bus (or an
exception has been raised). This ensures that the router will only mark
the message as completed after actual delivery.

It is assumed that traffic received from Service Bus has been sent via
<https://github.com/cbdq-io/kc-connectors/tree/develop/azure-servicebus-sink-connector>
and therefore has a default of __kafka_key as the seed for configuring
the session ID.

Environment Variables
---------------------
CUSTOM_BUFFER_MAX_WAIT_MS : int, optional
    Maximum wait time in milliseconds before flushing a batch.
    Default is 25 ms.
CUSTOM_BUFFER_MAX_MESSAGES : int, optional
    Maximum number of messages per batch before flushing.
    Default is 100.
CUSTOM_SESSION_ID_FAN_SIZE : int, optional
    Maximum size of an integer session ID.
    Default is 10.
CUSTOM_SESSION_PROPERTY_SEED : str, optional
    The application key to use as a seed for generating the
    session ID.
    Default is __kafka_key
"""
import asyncio
import contextlib
import hashlib
import os
from typing import Dict, Optional, Tuple

from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusSender

_FAN_SIZE = int(os.getenv('CUSTOM_SESSION_ID_FAN_SIZE', '10'))
_MAX_WAIT_MS = int(os.getenv('CUSTOM_BUFFER_MAX_WAIT_MS', '25'))
_MAX_MSGS = int(os.getenv('CUSTOM_BUFFER_MAX_MESSAGES', '100'))

_buffers: Dict[int, 'MessageBuffer'] = {}
_buffers_lock = asyncio.Lock()


class MessageBuffer:
    """
    Per-sender coalescing buffer with per-item flush acknowledgements.

    Messages are enqueued and held for a short time window or until a maximum
    count is reached. They are then sent in batches using the underlying
    ServiceBusSender. Each message has an associated `Future` which is resolved
    only when the batch containing that message has been successfully sent.

    This design ensures that the caller does not receive control back until
    their message is actually on Service Bus.

    Parameters
    ----------
    sender : ServiceBusSender
        The Service Bus sender used to send messages.
    max_wait_ms : int
        The maximum wait time in milliseconds before a batch is flushed,
        even if the batch has not reached the maximum message count.
    max_msgs : int
        The maximum number of messages per batch before flushing.
    """

    def __init__(self, sender: ServiceBusSender, max_wait_ms: int, max_msgs: int):
        self.sender = sender
        self.queue: asyncio.Queue[Optional[Tuple[ServiceBusMessage, asyncio.Future]]] = asyncio.Queue()
        self.max_wait = max_wait_ms / 1000.0
        self.max_msgs = max_msgs
        self._task = asyncio.create_task(self._run())

    async def add_and_wait(self, msg: ServiceBusMessage) -> None:
        """
        Enqueue a message and wait until it has been sent.

        Parameters
        ----------
        msg : ServiceBusMessage
            The message to send.

        Raises
        ------
        Exception
            If the underlying send operation fails, the exception is
            propagated to the caller.
        """
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        await self.queue.put((msg, fut))
        await fut

    async def close(self) -> None:
        """Flush any remaining messages and stop the background task."""
        await self.queue.put(None)
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    async def _run(self):
        """
        Background task that drains the queue and sends batches.

        Notes
        -----
        This loop delegates coalescing to :meth:`_coalesce_items` to keep
        control flow simple and reduce cyclomatic complexity. It exits only
        after a sentinel ``None`` is received and any remaining messages are
        flushed.
        """
        while True:
            items = await self._coalesce_items()
            if items is None:
                # sentinel received; ensure any residuals are flushed
                await self._flush_accumulated([])
                break
            await self._flush_accumulated(items)

    async def _coalesce_items(self):
        """
        Pull the first queued item, then coalesce more until timeout or limit.

        Returns
        -------
        list[tuple[ServiceBusMessage, asyncio.Future]] | None
            A list of (message, future) pairs to flush as a batch window,
            or ``None`` if a sentinel was encountered (meaning shutdown).

        Notes
        -----
        - Respects ``self.max_wait`` as a soft window.
        - Respects ``self.max_msgs`` as a hard cap.
        - If a sentinel ``None`` is pulled mid-window, it is re-enqueued so
          the outer loop can observe it on the next iteration and exit.
        """
        first = await self.queue.get()
        if first is None:
            return None

        items = [first]
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
                # preserve sentinel for the next iteration to terminate the loop
                self.queue.put_nowait(None)
                break

            items.append(nxt)

        return items

    async def _flush_accumulated(self, items):
        """
        Flush a list of queued items by sending them as one or more batches.

        Parameters
        ----------
        items : list of tuple(ServiceBusMessage, asyncio.Future)
            The messages and associated futures to send.

        Notes
        -----
        Splits into multiple Service Bus batches if the first one fills up.
        Each message's future is resolved (success or exception) according
        to the outcome of the batch it was sent in.
        """
        if not items:
            return

        batch = await self.sender.create_message_batch()
        pending = []

        async def flush_batch():
            if not pending:
                return
            try:
                await self.sender.send_messages(batch)
                for _, fut in pending:
                    if not fut.done():
                        fut.set_result(True)
            except Exception as e:
                for _, fut in pending:
                    if not fut.done():
                        fut.set_exception(e)

        for msg, fut in items:
            try:
                batch.add_message(msg)
                pending.append((msg, fut))
            except ValueError:
                # Batch is full: send current batch, then start a new one
                await flush_batch()
                batch = await self.sender.create_message_batch()
                batch.add_message(msg)
                pending = [(msg, fut)]

        await flush_batch()


async def _get_buffer(sender: ServiceBusSender) -> MessageBuffer:
    """
    Retrieve or create a MessageBuffer for a given sender.

    Parameters
    ----------
    sender : ServiceBusSender
        The sender for which to get a buffer.

    Returns
    -------
    MessageBuffer
        The buffer associated with the given sender.
    """
    key = id(sender)
    if key in _buffers:
        return _buffers[key]
    async with _buffers_lock:
        if key not in _buffers:
            _buffers[key] = MessageBuffer(sender, _MAX_WAIT_MS, _MAX_MSGS)
        return _buffers[key]


async def close():
    """
    Flush and close all active buffers.

    This function should be called during graceful shutdown to ensure
    that no messages remain unsent in memory.
    """
    if not _buffers:
        return
    await asyncio.gather(*(buf.close() for buf in list(_buffers.values())), return_exceptions=True)
    _buffers.clear()


async def custom_sender(sender: ServiceBusSender, topic_name: str, message_body: object, application_properties: dict):
    """
    Implement a custom sender for Service Bus messages with session assignment and batching.

    Computes a session ID from the specified application property, creates a
    `ServiceBusMessage`, and enqueues it into a buffer for batched sending.
    The function only returns after the specific message has been successfully
    sent.

    Parameters
    ----------
    sender : ServiceBusSender
        The Service Bus sender to use for sending the message.
    topic_name : str
        The name of the topic to which the message will be sent.
    message_body : object
        The body of the message. Can be str, bytes, or JSON-serializable object.
    application_properties : dict
        Application properties from the original message. Used to compute the
        session ID.

    Raises
    ------
    Exception
        If sending the message fails, the exception is propagated to the caller.
    """
    seed_property_name = os.getenv('CUSTOM_SESSION_PROPERTY_SEED', '__kafka_key').encode()
    seed = str(application_properties.get(seed_property_name))
    i = int(hashlib.md5(seed.encode(), usedforsecurity=False).hexdigest(), 16)
    session_id = str(i % _FAN_SIZE)

    msg = ServiceBusMessage(
        body=message_body,
        application_properties=application_properties,
        session_id=session_id,
    )

    buf = await _get_buffer(sender)
    await buf.add_and_wait(msg)
