#!/usr/bin/env python3
"""
Replays messages from an Azure Service Bus subscription DLQ back to (by default) the same topic.

Works with both non-session and session-enabled subscriptions.
"""
import argparse
import asyncio
import logging
import os
import sys
import time

from azure.servicebus import (ServiceBusMessage, ServiceBusSubQueue,
                              TransportType)
from azure.servicebus.aio import ServiceBusClient

PROG = os.path.basename(__file__)
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(PROG)


def make_args():
    """Construct the command line arguement parser."""
    p = argparse.ArgumentParser(
        PROG,
        description='Replay Azure Service Bus DLQ messages for a subscription.'
    )
    p.add_argument('--connection-string', '-c',
                   default=os.getenv('ROUTER_SOURCE_CONNECTION_STRING'),
                   help='Service Bus connection string. Default: ROUTER_SOURCE_CONNECTION_STRING env var.')
    p.add_argument('--topic', required=True, help='Source topic name containing the subscription.')
    p.add_argument('--subscription', required=True, help='Source subscription name (the DLQ of this is read).')

    p.add_argument('--dest-topic', default=None,
                   help='Destination topic to re-publish to. Default: same as --topic.')
    p.add_argument('--max-messages', type=int, default=0,
                   help='Maximum number of DLQ messages to process (0 = unlimited).')
    p.add_argument('--max-wait', type=float, default=2.0,
                   help='Seconds to wait for a batch before concluding no more messages are available this cycle.')
    p.add_argument('--batch-size', type=int, default=50,
                   help='Max messages to receive per batch (non-session).')
    p.add_argument('--concurrency', type=int, default=1,
                   help='Concurrent DLQ sessions to process (for session-enabled subs).')
    p.add_argument('--sessions', action='store_true',
                   help='Treat the subscription as session-enabled. If not set, non-session receive is used.')
    p.add_argument('--dry-run', action='store_true',
                   help='Do everything except actually send/complete. Useful for inspection.')
    p.add_argument('--reason-filter', default=None,
                   help='Only process messages whose dead-letter reason contains this substring (case-insensitive).')
    p.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    p.add_argument('--entity-endpoint', default=None,
                   help='Override fully qualified namespace, e.g. "my-namespace.servicebus.windows.net". '
                        'If not supplied, it is inferred from the connection string.')
    p.add_argument('--use-websockets', action='store_true',
                   help='Use AMQP over WebSockets (helpful behind firewalls/proxies).')
    return p.parse_args()


def clone_sb_message(src: ServiceBusMessage) -> ServiceBusMessage:
    """
    Build a new ServiceBusMessage from an existing received message.

    You cannot "un-dead-letter" in place; you must re-send.
    """
    # Body
    body = b''.join(list(src.body)) if hasattr(src, 'body') else b''
    msg = ServiceBusMessage(body)

    # Application properties
    if getattr(src, 'application_properties', None):
        msg.application_properties = dict(src.application_properties)

    # System-like props we can set on outgoing messages
    # (You cannot set enqueued_time, sequence_number, lock tokens, etc.)
    for attr in ('content_type', 'correlation_id', 'subject', 'to', 'reply_to',
                 'reply_to_session_id', 'message_id', 'partition_key', 'session_id',
                 'time_to_live'):
        if hasattr(src, attr):
            val = getattr(src, attr)
            if val is not None:
                setattr(msg, attr, val)

    # Scheduling is intentionally not preserved by default. Uncomment if you want to re-schedule:
    # if hasattr(src, 'scheduled_enqueue_time_utc') and src.scheduled_enqueue_time_utc:
    #     msg.scheduled_enqueue_time_utc = src.scheduled_enqueue_time_utc

    return msg


def deadletter_reason_contains(msg, needle: str) -> bool:
    """
    Check if a dead-letter reason constains a string.

    Dead-letter reason + error are exposed via "dead_letter_reason" and "dead_letter_error_description"
    on received messages (SDK 7.10+). Fallback to application_properties just in case.
    """
    if not needle:
        return True

    needle = needle.lower()
    fields = []

    for name in ('dead_letter_reason', 'dead_letter_error_description'):
        if hasattr(msg, name) and getattr(msg, name) is not None:
            fields.append(str(getattr(msg, name)))

    if getattr(msg, 'application_properties', None):
        for k in ('DeadLetterReason', 'DeadLetterErrorDescription'):
            if k in msg.application_properties:
                fields.append(str(msg.application_properties.get(k)))

    hay = ' '.join(fields).lower()
    return needle in hay


async def process_non_session_dlq(sb_client: ServiceBusClient,
                                  src_topic: str,
                                  subscription: str,
                                  dest_topic: str,
                                  args) -> int:
    """Process messages on a non-sessioned subscription that have been dead lettered."""
    total = 0

    async with sb_client:
        logger.debug(f'Creating a receiver for {src_topic}/{subscription}...')
        receiver = sb_client.get_subscription_receiver(
            topic_name=src_topic,
            subscription_name=subscription,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER,
            prefetch_count=args.batch_size
        )
        logger.debug(f'Creating sender for topic {dest_topic}...')
        sender = sb_client.get_topic_sender(topic_name=dest_topic)

        async with receiver, sender:
            logger.debug('Receiver and sender opened.')

            while True:
                if args.max_messages and total >= args.max_messages:
                    break

                remaining = args.max_messages - total if args.max_messages else args.batch_size
                logger.debug('Calling receive_messages(max=%d, wait=%.1fs)...',
                             min(args.batch_size, max(1, remaining)), args.max_wait)
                batch = await receiver.receive_messages(
                    max_message_count=min(args.batch_size, max(1, remaining)),
                    max_wait_time=args.max_wait
                )

                if not batch:
                    break

                for msg in batch:
                    if not deadletter_reason_contains(msg, args.reason_filter):
                        # Skip without completing; just abandon so it stays in DLQ.
                        await receiver.abandon_message(msg)
                        continue

                    out = clone_sb_message(msg)
                    try:
                        if not args.dry_run:
                            await sender.send_messages(out)
                            await receiver.complete_message(msg)
                        else:
                            # In dry-run, just abandon to keep the message for future operations
                            logger.info('DRY RUN: would send+complete message_id=%s', getattr(msg, 'message_id', None))
                            await receiver.abandon_message(msg)
                        total += 1
                    except Exception as e:
                        logging.exception('Failed to replay a message; deferring to keep it safe. %s', e)
                        try:
                            await receiver.defer_message(msg)
                        except Exception:
                            logging.exception('Failed to defer message; attempting abandon as fallback.')
                            try:
                                await receiver.abandon_message(msg)
                            except Exception:
                                logging.exception('Failed to abandon message.')

    return total


async def _process_one_session(sb_client: ServiceBusClient,
                               src_topic: str,
                               subscription: str,
                               dest_topic: str,
                               args) -> int:
    """
    Accept the next available DLQ session and drain it.

    Returns the count of replayed messages.
    """
    count = 0
    async with sb_client:
        # Accept *a* DLQ session (next available)
        session_receiver = await sb_client.accept_next_session(
            topic_name=src_topic,
            subscription_name=subscription,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER
        )
        sender = sb_client.get_topic_sender(topic_name=dest_topic)
        async with session_receiver, sender:
            # Drain messages for this session
            while True:
                if args.max_messages and count >= args.max_messages:
                    break
                msgs = await session_receiver.receive_messages(
                    max_message_count=args.batch_size, max_wait_time=args.max_wait
                )
                if not msgs:
                    break

                for msg in msgs:
                    if not deadletter_reason_contains(msg, args.reason_filter):
                        await session_receiver.abandon_message(msg)
                        continue

                    out = clone_sb_message(msg)

                    try:
                        if not args.dry_run:
                            await sender.send_messages(out)
                            await session_receiver.complete_message(msg)
                        else:
                            await session_receiver.abandon_message(msg)
                        count += 1
                    except Exception as e:
                        logging.exception('Session replay failed; deferring message. %s', e)
                        try:
                            await session_receiver.defer_message(msg)
                        except Exception:
                            logging.exception('Failed to defer; attempting abandon.')
                            try:
                                await session_receiver.abandon_message(msg)
                            except Exception:
                                logging.exception('Failed to abandon.')
    return count


async def process_session_dlq(sb_client: ServiceBusClient,
                              src_topic: str,
                              subscription: str,
                              dest_topic: str,
                              args) -> int:
    """
    Loops accepting sessions from the DLQ until none are available.

    If --concurrency>1, it will try to run multiple sessions in parallel (best effort).
    """
    total = 0
    # We repeatedly try to accept sessions. When no session is available, accept_next_session raises.
    # We'll treat that as completion.
    sem = asyncio.Semaphore(max(1, args.concurrency))

    async def worker():
        nonlocal total
        while True:
            await sem.acquire()
            try:
                count = await _process_one_session(sb_client, src_topic, subscription, dest_topic, args)
                if count == 0:
                    # No messages -> likely we're done with this session; try next.
                    # If there are no more sessions, the next accept will raise and break outer loop.
                    pass
                total += count
            except Exception as e:
                # When no sessions remain, the SDK typically raises a ServiceBusError with reason=SessionCannotBeLocked.
                logging.debug('No more DLQ sessions or error while accepting one: %s', e)
                break
            finally:
                sem.release()

    # Kick off N workers which will each drain sessions until none remain
    tasks = [asyncio.create_task(worker()) for _ in range(max(1, args.concurrency))]
    await asyncio.gather(*tasks, return_exceptions=True)
    return total


async def main_async(args):
    """Execute main processing asyncronously."""
    if not args.connection_string:
        logging.error(
            'ERROR: Connection string not provided. Use --connection-string or set ROUTER_SOURCE_CONNECTION_STRING.'
        )
        return 2

    dest_topic = args.dest_topic or args.topic
    start = time.time()

    transport_type = TransportType.AmqpOverWebsocket if args.use_websockets else TransportType.Amqp

    sb_client = ServiceBusClient.from_connection_string(
        conn_str=args.connection_string,
        transport_type=transport_type,
        logging_enable=False
    )

    try:
        if args.sessions:
            count = await process_session_dlq(sb_client, args.topic, args.subscription, dest_topic, args)
        else:
            count = await process_non_session_dlq(sb_client, args.topic, args.subscription, dest_topic, args)
    finally:
        await sb_client.close()

    dur = time.time() - start
    logging.info('Done. Replayed %d message(s) in %.2fs%s.',
                 count, dur, ' [DRY RUN]' if args.dry_run else '')
    return 0


def main():
    """Execute if called from the CLI."""
    args = make_args()
    logger.setLevel(args.log_level)
    try:
        rc = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logging.warning('Interrupted.')
        rc = 130
    sys.exit(rc)


if __name__ == '__main__':
    main()
