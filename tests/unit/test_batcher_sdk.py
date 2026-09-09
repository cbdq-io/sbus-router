"""Exercise the installed SDK's real encoder without opening any connections."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from azure.servicebus import ServiceBusMessage, ServiceBusMessageBatch

from router import ServiceBusHandler

pytestmark = pytest.mark.unit
MIB = 1024 * 1024


@pytest.mark.parametrize('with_properties', [False, True])
def test_actual_sdk_encoded_message_that_exceeds_batch_limit_is_sent_individually(with_properties):
    async def case():
        # Simulate a sender on a large-message-enabled link. Batch encoding is real.
        sender = SimpleNamespace(
            create_message_batch=AsyncMock(side_effect=lambda max_size_in_bytes=None: ServiceBusMessageBatch(
                max_size_in_bytes=max_size_in_bytes or 4 * MIB
            )),
            send_messages=AsyncMock(),
        )
        message = ServiceBusMessage(
            b'x' * (MIB - 100 if with_properties else MIB),
            application_properties={'padding': 'y' * 1024} if with_properties else None,
            message_id='large-test',
        )
        batcher = ServiceBusHandler._Batcher(AsyncMock(return_value=sender), 1, 256, 'destination.topic')
        try:
            await asyncio.wait_for(batcher.add_and_wait(message), timeout=2)
            sender.send_messages.assert_awaited_once_with(message)
        finally:
            await asyncio.wait_for(batcher.close(), timeout=2)
    asyncio.run(case())


def test_actual_sdk_batch_keeps_smaller_negotiated_limit():
    async def case():
        limit = 256 * 1024
        sender = SimpleNamespace(
            create_message_batch=AsyncMock(side_effect=lambda max_size_in_bytes=None: ServiceBusMessageBatch(
                max_size_in_bytes=max_size_in_bytes or limit
            )),
            send_messages=AsyncMock(),
        )
        message = ServiceBusMessage(b'ordinary message', message_id='small-test')
        batcher = ServiceBusHandler._Batcher(AsyncMock(return_value=sender), 1, 256, 'destination.topic')
        try:
            await asyncio.wait_for(batcher.add_and_wait(message), timeout=2)
            payload = sender.send_messages.await_args.args[0]
            assert isinstance(payload, ServiceBusMessageBatch)
            assert payload.max_size_in_bytes == limit
            assert payload.size_in_bytes <= limit
            sender.create_message_batch.assert_awaited_once_with()
        finally:
            await asyncio.wait_for(batcher.close(), timeout=2)
    asyncio.run(case())
