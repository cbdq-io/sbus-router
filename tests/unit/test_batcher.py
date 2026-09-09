"""Transport-double tests: no Service Bus, Docker, credentials or asyncio plugin."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from azure.servicebus.exceptions import MessageSizeExceededError

from router import ServiceBusHandler

pytestmark = pytest.mark.unit
TIMEOUT = 2
MIB = 1024 * 1024


@dataclass
class Message:
    size: int
    message_id: str = 'test-message'


class Batch:
    def __init__(self, capacity):
        self.max_size_in_bytes = capacity
        self.messages = []
        self.size_in_bytes = 0

    def add_message(self, message):
        if self.size_in_bytes + message.size > self.max_size_in_bytes:
            raise MessageSizeExceededError()
        self.messages.append(message)
        self.size_in_bytes += message.size


class Sender:
    def __init__(self, capacity=100):
        self.capacity = capacity
        self.created = []
        self.attempts = []
        self.acknowledged = []
        self.create_hook = None
        self.send_hook = None

    async def create_message_batch(self, max_size_in_bytes=None):
        self.created.append(max_size_in_bytes)
        if self.create_hook:
            await self.create_hook(len(self.created))
        capacity = self.capacity if max_size_in_bytes is None else max_size_in_bytes
        if capacity > self.capacity:
            raise ValueError('Batch capacity exceeds the negotiated link limit')
        return Batch(capacity)

    async def send_messages(self, payload):
        self.attempts.append(payload)
        if self.send_hook:
            await self.send_hook(payload)
        self.acknowledged.append(payload)


async def within(awaitable):
    return await asyncio.wait_for(awaitable, timeout=TIMEOUT)


async def results(batcher, *messages):
    return await within(asyncio.gather(
        *(batcher.add_and_wait(message) for message in messages),
        return_exceptions=True,
    ))


@asynccontextmanager
async def worker(sender, *, factory=None, max_messages=256, max_wait_ms=10):
    batcher = ServiceBusHandler._Batcher(
        factory or AsyncMock(return_value=sender), max_wait_ms, max_messages, 'destination.topic'
    )
    try:
        yield batcher
    finally:
        # Clean up even when testing the original broken worker. Retrieve its exception.
        batcher._task.cancel()
        await within(asyncio.gather(batcher._task, return_exceptions=True))


def sent_ids(sender):
    return [
        [m.message_id for m in payload.messages] if isinstance(payload, Batch) else [payload.message_id]
        for payload in sender.acknowledged
    ]


def test_ordinary_messages_are_coalesced():
    async def case():
        sender = Sender()
        async with worker(sender) as batcher:
            assert await results(batcher, Message(20, 'a'), Message(30, 'b')) == [None, None]
            assert len(sender.acknowledged) == 1
            assert isinstance(sender.acknowledged[0], Batch)
            assert sent_ids(sender) == [['a', 'b']]
    asyncio.run(case())


def test_full_batch_rolls_over_without_individual_send():
    async def case():
        sender = Sender()
        async with worker(sender) as batcher:
            assert await results(batcher, Message(60, 'a'), Message(60, 'b')) == [None, None]
            assert all(isinstance(payload, Batch) for payload in sender.acknowledged)
            assert sent_ids(sender) == [['a'], ['b']]
    asyncio.run(case())


@pytest.mark.parametrize('sizes', [(101,), (10, 101, 10), (101, 102), (60, 60, 101, 10)])
def test_oversized_messages_are_sent_individually_in_order(sizes):
    async def case():
        sender = Sender()
        messages = [Message(size, str(i)) for i, size in enumerate(sizes)]
        async with worker(sender) as batcher:
            assert await results(batcher, *messages) == [None] * len(messages)
            assert [i for group in sent_ids(sender) for i in group] == [m.message_id for m in messages]
            for message in messages:
                if message.size > sender.capacity:
                    assert any(payload is message for payload in sender.acknowledged)
            assert all(not isinstance(payload, list) for payload in sender.attempts)
            assert all(payload.messages for payload in sender.attempts if isinstance(payload, Batch))
    asyncio.run(case())


def test_same_worker_processes_later_messages_after_oversized_message():
    async def case():
        sender = Sender()
        async with worker(sender) as batcher:
            assert await results(batcher, Message(101, 'large')) == [None]
            assert not batcher._task.done()
            assert await results(batcher, Message(1, 'later')) == [None]
            assert sent_ids(sender) == [['large'], ['later']]
    asyncio.run(case())


@pytest.mark.parametrize('capacity', [256 * 1024, MIB, 4 * MIB])
def test_batch_capacity_respects_both_link_limit_and_one_mib(capacity):
    async def case():
        sender = Sender(capacity)
        async with worker(sender) as batcher:
            assert await results(batcher, Message(1)) == [None]
            batch = sender.acknowledged[0]
            assert isinstance(batch, Batch)
            assert batch.max_size_in_bytes == min(capacity, MIB)
    asyncio.run(case())


def test_one_mib_payload_uses_individual_send_when_link_allows_four_mib():
    async def case():
        sender = Sender(4 * MIB)
        # The transport double represents encoded size: body plus headers/overhead.
        message = Message(MIB + 100)
        async with worker(sender) as batcher:
            assert await results(batcher, message) == [None]
            assert sender.acknowledged == [message]
    asyncio.run(case())


@pytest.mark.parametrize('large', [False, True])
def test_acknowledgement_waits_for_send_to_finish(large):
    async def case():
        sender = Sender()
        entered, release = asyncio.Event(), asyncio.Event()

        async def hold_send(payload):
            entered.set()
            await release.wait()

        sender.send_hook = hold_send
        async with worker(sender) as batcher:
            task = asyncio.create_task(batcher.add_and_wait(Message(101 if large else 1)))
            try:
                await within(entered.wait())
                assert not task.done()
                release.set()
                assert await within(task) is None
            finally:
                release.set()
                task.cancel()
                await within(asyncio.gather(task, return_exceptions=True))
    asyncio.run(case())


@pytest.mark.parametrize('stage', ['sender', 'create', 'add', 'send_batch', 'send_individual'])
def test_failure_reaches_callers_and_next_delivery_can_succeed(stage):
    async def case():
        sender = Sender()
        failure = RuntimeError(f'Injected {stage} failure')
        factory = AsyncMock(side_effect=[failure, sender]) if stage == 'sender' else None
        first = True

        async def fail_once(*args):
            nonlocal first
            if first:
                first = False
                raise failure

        if stage == 'create':
            sender.create_hook = fail_once
        elif stage in ('send_batch', 'send_individual'):
            sender.send_hook = fail_once
        elif stage == 'add':
            create = sender.create_message_batch

            async def bad_add_then_normal(*args, **kwargs):
                nonlocal first
                batch = await create(*args, **kwargs)
                if first:
                    first = False

                    def fail_add(message):
                        raise failure

                    batch.add_message = fail_add
                return batch

            sender.create_message_batch = bad_add_then_normal

        async with worker(sender, factory=factory) as batcher:
            size = 101 if stage == 'send_individual' else 1
            failed = await results(batcher, Message(size, 'first'), Message(1, 'second'))
            assert failed == [failure, failure]
            assert not batcher._task.done()
            assert await results(batcher, Message(1, 'later')) == [None]
            assert sent_ids(sender)[-1] == ['later']
    asyncio.run(case())


def test_non_size_value_error_is_not_treated_as_oversized_message():
    async def case():
        sender = Sender()
        failure = ValueError('Invalid session or partition metadata')
        original_create = sender.create_message_batch

        async def create(*args, **kwargs):
            batch = await original_create(*args, **kwargs)

            def bad_add(message):
                raise failure

            batch.add_message = bad_add
            return batch

        sender.create_message_batch = create
        async with worker(sender) as batcher:
            assert await results(batcher, Message(1)) == [failure]
            assert not sender.attempts
    asyncio.run(case())


def test_later_failure_does_not_reverse_an_earlier_acknowledgement():
    async def case():
        sender = Sender()
        failure = RuntimeError('Individual send failed')

        async def fail_individual(payload):
            if not isinstance(payload, Batch):
                raise failure

        sender.send_hook = fail_individual
        async with worker(sender) as batcher:
            assert await results(batcher, Message(10, 'a'), Message(101, 'b'), Message(1, 'c')) == [
                None, failure, failure
            ]
            assert sent_ids(sender) == [['a']]
            assert len(sender.attempts) == 2
    asyncio.run(case())


def test_second_batch_creation_failure_fails_only_unacknowledged_items():
    async def case():
        sender = Sender()
        failure = RuntimeError('Could not create next batch')

        async def fail_second(call):
            if call == 2:
                raise failure

        sender.create_hook = fail_second
        async with worker(sender) as batcher:
            assert await results(batcher, Message(60, 'a'), Message(60, 'b'), Message(1, 'c')) == [
                None, failure, failure
            ]
            assert sent_ids(sender) == [['a']]
            assert await results(batcher, Message(1, 'later')) == [None]
    asyncio.run(case())


def test_send_failure_is_not_retried_as_individual_message():
    async def case():
        sender = Sender()
        failure = MessageSizeExceededError()

        async def reject_send(payload):
            raise failure

        sender.send_hook = reject_send
        async with worker(sender) as batcher:
            assert await results(batcher, Message(1)) == [failure]
            assert len(sender.attempts) == 1
            assert isinstance(sender.attempts[0], Batch)
    asyncio.run(case())


def test_closing_flushes_work_and_rejects_new_submissions():
    async def case():
        sender = Sender()
        async with worker(sender, max_wait_ms=100) as batcher:
            task = asyncio.create_task(batcher.add_and_wait(Message(1)))
            await asyncio.sleep(0)
            await within(batcher.close())
            assert await within(task) is None
            await within(batcher.close())
            with pytest.raises(RuntimeError, match='closed'):
                await within(batcher.add_and_wait(Message(1)))
    asyncio.run(case())


@pytest.mark.parametrize('stage', ['coalesce', 'send'])
def test_worker_cancellation_releases_waiters(stage):
    async def case():
        sender = Sender()
        entered = asyncio.Event()

        async def hold(payload):
            entered.set()
            await asyncio.Event().wait()

        if stage == 'send':
            sender.send_hook = hold
        async with worker(sender, max_wait_ms=1000 if stage == 'coalesce' else 1) as batcher:
            task = asyncio.create_task(batcher.add_and_wait(Message(1)))
            try:
                if stage == 'send':
                    await within(entered.wait())
                else:
                    await asyncio.sleep(0.01)
                batcher._task.cancel()
                result = await within(asyncio.gather(task, return_exceptions=True))
                assert isinstance(result[0], RuntimeError)
                with pytest.raises(RuntimeError, match='closed'):
                    await within(batcher.add_and_wait(Message(1)))
            finally:
                task.cancel()
                await within(asyncio.gather(task, return_exceptions=True))
    asyncio.run(case())


def test_cancelled_caller_does_not_kill_worker():
    async def case():
        sender = Sender()
        entered, release = asyncio.Event(), asyncio.Event()

        async def hold(payload):
            entered.set()
            await release.wait()

        sender.send_hook = hold
        async with worker(sender) as batcher:
            task = asyncio.create_task(batcher.add_and_wait(Message(1, 'cancelled')))
            await within(entered.wait())
            task.cancel()
            await within(asyncio.gather(task, return_exceptions=True))
            release.set()
            assert await results(batcher, Message(1, 'later')) == [None]
            assert not batcher._task.done()
    asyncio.run(case())


def test_destination_and_operation_are_in_error_log(caplog):
    async def case():
        sender = Sender()
        failure = RuntimeError('Injected create error')

        async def fail(call):
            raise failure

        sender.create_hook = fail
        factory = AsyncMock(return_value=sender)
        handler = SimpleNamespace(batchers={}, get_sender=factory,
                                  _BATCH_MAX_WAIT_MS=1, _BATCH_MAX_MESSAGES=256)
        batcher = ServiceBusHandler._get_batcher(handler, 'TESTNS', 'destination.topic')
        try:
            assert await results(batcher, Message(1)) == [failure]
            assert 'TESTNS/destination.topic' in caplog.text
            assert 'create_message_batch' in caplog.text
            assert 'RuntimeError' in caplog.text
        finally:
            batcher._task.cancel()
            await within(asyncio.gather(batcher._task, return_exceptions=True))
    asyncio.run(case())
