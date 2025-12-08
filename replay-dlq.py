#!/usr/bin/env python
"""
A basic script to replay messages from the topic/subscription.

It is strongly recommended to specify the --to and --from values as this will
protect you from infinity processing if messages return to the dead-letter
queue.

SYNOPSIS
--------
usage: replay-dlq.py [-h] [--dry-run] [--from TS_FROM] [--to TS_TO] [--sessions [SESSIONS]] [-v] topic subscription

Replay Service Bus DLQ Messages

positional arguments:
  topic                 Topic name
  subscription          Subscription name

options:
  -h, --help            show this help message and exit
  --dry-run             Inspect messages only — do not replay or complete
  --from TS_FROM        Only process messages enqueued after this ISO-8601 timestamp
  --to TS_TO            Only process messages enqueued before this ISO-8601 timestamp
  --sessions [SESSIONS]
                        Enable or disable subscription sessions (only used for testing).
  -v, --verbose         Enable DEBUG logging
"""
import argparse
import datetime
import logging
import os

from azure.servicebus import (NEXT_AVAILABLE_SESSION, ServiceBusClient,
                              ServiceBusMessage, ServiceBusSubQueue)
from azure.servicebus.exceptions import ServiceBusError


def parse_iso8601(ts: str):
    """Parse ISO-8601 timestamp or return None."""
    if ts is None:
        return None
    return datetime.datetime.fromisoformat(ts)


def get_client() -> ServiceBusClient:
    """Create a client from the connection string environment variable."""
    conn_str = os.environ.get('ROUTER_SOURCE_CONNECTION_STRING')
    return ServiceBusClient.from_connection_string(conn_str)


def message_in_time_range(msg, ts_from, ts_to):
    """Return True if the message is within the given time range."""
    enq = msg.enqueued_time_utc

    if ts_from and enq < ts_from:
        return False
    if ts_to and enq > ts_to:
        return False

    return True


def detect_session_mode(client: ServiceBusClient, topic: str, subscription: str, logger=None):
    """
    Detect whether a subscription requires sessions.

    Uses a safe non-destructive probe based only on the data-plane API.
    """
    try:
        receiver = client.get_subscription_receiver(
            topic_name=topic,
            subscription_name=subscription,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER,
        )
    except ServiceBusError as exc:
        # If it fails immediately with a session error, it's session-enabled
        if 'session' in str(exc).lower():
            return True
        raise

    with receiver:
        try:
            receiver.receive_messages(
                max_message_count=1,
                max_wait_time=2
            )
            # If messages were received, we don't complete them.
            # After lock expiry, they return to the DLQ.
            return False  # Non-session subscription

        except ServiceBusError as exc:
            # This is the key check
            if 'session' in str(exc).lower():
                return True

            raise


def replay_dead_letter_messages(topic: str,
                                subscription: str,
                                dry_run: bool,
                                ts_from: datetime.datetime,
                                ts_to: datetime.datetime,
                                sessions_enabled: bool,
                                logger: logging.Logger):
    """Replay dead-letter messages on a topic/subscription."""
    if sessions_enabled is None:
        client = get_client()

        with client:
            sessions_enabled = detect_session_mode(
                client=client,
                topic=topic,
                subscription=subscription,
                logger=logger
            )

    if not sessions_enabled:
        replay_dead_letter_messages_non_sessioned(
            topic=topic,
            subscription=subscription,
            dry_run=dry_run,
            ts_from=ts_from,
            ts_to=ts_to,
            logger=logger
        )
    else:
        # For subscriptions with sessions enabled.
        replay_dead_letter_messages_sessioned(
             topic=topic,
             subscription=subscription,
             dry_run=dry_run,
             ts_from=ts_from,
             ts_to=ts_to,
             logger=logger
        )


def replay_dead_letter_messages_sessioned(topic: str, subscription: str, dry_run: bool,
                                          ts_from: datetime.datetime, ts_to: datetime.datetime,
                                          logger: logging.Logger):
    """
    Replay dead-letter messages on a topic/subscription.

    For sessioned subscriptions.
    """
    client = get_client()
    total_seen = 0
    total_replayed = 0
    total_filtered_out = 0

    with client:
        while True:
            try:
                receiver = client.get_subscription_receiver(
                    topic_name=topic,
                    subscription_name=subscription,
                    sub_queue=ServiceBusSubQueue.DEAD_LETTER,
                    session_id=NEXT_AVAILABLE_SESSION
                )
            except Exception:
                logger.info('No more DLQ sessions remaining.')
                break

            sender = client.get_topic_sender(topic)

            with receiver, sender:
                session_id = receiver.session_id
                logger.info(
                    f'{"Dry-run: inspecting" if dry_run else "Replaying"} '
                    f'DLQ messages for {topic}/{subscription}/{session_id}'
                )

                if ts_from:
                    logger.info(f'Filtering messages FROM: {ts_from.isoformat()}')
                if ts_to:
                    logger.info(f'Filtering messages TO:   {ts_to.isoformat()}')

                while True:
                    messages = receiver.receive_messages(
                        max_message_count=100,
                        max_wait_time=5,
                    )
                    if not messages:
                        break

                    for msg in messages:
                        total_seen += 1
                        enq_time = msg.enqueued_time_utc

                        if not message_in_time_range(msg, ts_from, ts_to):
                            total_filtered_out += 1
                            logger.debug(
                                f'Skipping {msg.message_id} (enqueued {enq_time}) — outside time range'
                            )
                            continue

                        logger.debug(f'Processing message {msg.message_id}')

                        if dry_run:
                            logger.info(f'Dry-run: would replay message ID: {msg.message_id}')
                            continue

                        try:
                            new_msg = ServiceBusMessage(
                                body=b''.join(msg.body),
                                application_properties=msg.application_properties,
                                content_type=msg.content_type,
                                subject=msg.subject,
                                correlation_id=msg.correlation_id,
                                message_id=msg.message_id,
                                session_id=msg.session_id,
                            )

                            sender.send_messages(new_msg)
                            receiver.complete_message(msg)
                            total_replayed += 1

                        except Exception as exc:
                            logger.error(f'Failed to replay {msg.message_id}: {exc}')
                            receiver.abandon_message(msg)

    logger.info(f'Total messages inspected:   {total_seen}')
    logger.info(f'Total messages filtered out: {total_filtered_out}')
    if dry_run:
        logger.info('Dry-run complete. No messages were replayed.')
    else:
        logger.info(f'Total messages replayed: {total_replayed}')


def replay_dead_letter_messages_non_sessioned(topic: str, subscription: str, dry_run: bool, ts_from: datetime.datetime,
                                              ts_to: datetime.datetime, logger: logging.Logger):
    """
    Replay dead-letter messages on a topic/subscription.

    For non-sessioned subscriptions.
    """
    client = get_client()
    total_seen = 0
    total_replayed = 0
    total_filtered_out = 0

    with client:
        receiver = client.get_subscription_receiver(
            topic_name=topic,
            subscription_name=subscription,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER,
        )
        sender = client.get_topic_sender(topic)

        with receiver, sender:
            logger.info(
                f'{"Dry-run: inspecting" if dry_run else "Replaying"} DLQ messages for '
                f'{topic}/{subscription}'
            )

            if ts_from:
                logger.info(f'Filtering messages FROM: {ts_from.isoformat()}')
            if ts_to:
                logger.info(f'Filtering messages TO:   {ts_to.isoformat()}')

            while True:
                messages = receiver.receive_messages(
                    max_message_count=100,
                    max_wait_time=5,
                )
                if not messages:
                    break

                for msg in messages:
                    total_seen += 1
                    enq_time = msg.enqueued_time_utc

                    if not message_in_time_range(msg, ts_from, ts_to):
                        total_filtered_out += 1
                        logger.debug(
                            f'Skipping message {msg.message_id} (enqueued: {enq_time}) '
                            '— outside time range'
                        )
                        continue

                    logger.debug(
                        f'Processing message {msg.message_id} enqueued at {enq_time}'
                    )

                    if dry_run:
                        logger.info(f'Dry-run: would replay message ID: {msg.message_id}')
                        continue

                    try:
                        new_msg = ServiceBusMessage(
                            body=b''.join(msg.body),
                            application_properties=msg.application_properties,
                            content_type=msg.content_type,
                            subject=msg.subject,
                            correlation_id=msg.correlation_id,
                            message_id=msg.message_id,
                            session_id=msg.session_id,
                        )

                        sender.send_messages(new_msg)
                        receiver.complete_message(msg)
                        total_replayed += 1
                        logger.debug(f'Replayed message ID: {msg.message_id}')

                    except Exception as exc:
                        logger.error(f'Failed to replay {msg.message_id}: {exc}')
                        receiver.abandon_message(msg)

    logger.info(f'Total messages inspected:   {total_seen}')
    logger.info(f'Total messages filtered out: {total_filtered_out}')

    if dry_run:
        logger.info('Dry-run complete. No messages were replayed.')
    else:
        logger.info(f'Total messages replayed: {total_replayed}')


def get_logger(verbose: bool) -> logging.Logger:
    """Configure the logging to either be info or debug."""
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    logger = logging.getLogger('replay-dlq')

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    return logger


def str2bool(v: str) -> bool:
    """Convert a string to a boolean."""
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    raise argparse.ArgumentTypeError('Boolean value expected.')


def parse_args():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser(description='Replay Service Bus DLQ Messages')
    parser.add_argument('--dry-run', action='store_true',
                        help='Inspect messages only — do not replay or complete')
    parser.add_argument('--from', dest='ts_from', help='Only process messages enqueued after this ISO-8601 timestamp')
    parser.add_argument('--to', dest='ts_to', help='Only process messages enqueued before this ISO-8601 timestamp')
    parser.add_argument(
        '--sessions',
        type=str2bool,
        nargs='?',
        const=True,
        default=None,
        help='Enable or disable subscription sessions (only used for testing).'
    )
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable DEBUG logging')
    parser.add_argument('topic', help='Topic name')
    parser.add_argument('subscription', help='Subscription name')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    logger = get_logger(args.verbose)
    ts_from = parse_iso8601(args.ts_from)
    ts_to = parse_iso8601(args.ts_to)

    replay_dead_letter_messages(
        topic=args.topic,
        subscription=args.subscription,
        dry_run=args.dry_run,
        ts_from=ts_from,
        ts_to=ts_to,
        sessions_enabled=args.sessions,
        logger=logger
    )
