#!/usr/bin/env python
"""
A script for deleting DLQ messages from a topic/subscription.

Synopsis
--------
nukedlq.py [-h] [-d | -v] [-c CONNECTION_STRING] [-p PERIOD] -s SUBSCRIPTION -t TOPIC

options:
  -h, --help            show this help message and exit
  -d, --debug           Debug level of logging.
  -v, --verbose         Info level of logging.
  -c CONNECTION_STRING, --connection-string CONNECTION_STRING
                        The connection string for the Service Bus namespace [env:ROUTER_SOURCE_CONNECTION_STRING].
  -p PERIOD, --period PERIOD
                        The period of time to start removing messages (default is 24 hours).
  -s SUBSCRIPTION, --subscription SUBSCRIPTION
                        The name of the subscription.
  -t TOPIC, --topic TOPIC
                        The name of the topic.
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timezone

import isodate
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode

logging.basicConfig()
parser = argparse.ArgumentParser()
logger = logging.getLogger(parser.prog)
group = parser.add_mutually_exclusive_group()
group.add_argument(
    '-d', '--debug',
    help='Debug level of logging.',
    action='store_true'
)
group.add_argument(
    '-v', '--verbose',
    help='Info level of logging.',
    action='store_true'
)
parser.add_argument(
    '-c', '--connection-string',
    help='The connection string for the Service Bus namespace [env:ROUTER_SOURCE_CONNECTION_STRING].',
    default=os.environ.get('ROUTER_SOURCE_CONNECTION_STRING', '')
)
parser.add_argument(
    '-p', '--period',
    help='The period of time to start removing messages (default is 24 hours).',
    default='PT24H'
)
parser.add_argument(
    '-s', '--subscription',
    help='The name of the subscription.',
    required=True
)
parser.add_argument(
    '-t', '--topic',
    help='The name of the topic.',
    required=True
)


def _parse_cutoff_time(period: str) -> datetime:
    try:
        cutoff_period = isodate.parse_duration(period)
    except Exception as e:
        logger.error(f'Invalid ISO 8601 duration: {period}. Error: {e}')
        sys.exit(2)
    return datetime.now(timezone.utc) - cutoff_period


def _should_delete(peeked_msg, cutoff_time: datetime) -> bool:
    return peeked_msg.enqueued_time_utc < cutoff_time


def _delete_matching_message(receiver, target_time: datetime) -> bool:
    for msg in receiver.receive_messages(max_message_count=1):
        if msg.enqueued_time_utc == target_time:
            return True
    return False


def _handle_peeked_message(receiver, peeked_msg, cutoff_time: datetime) -> bool:
    if not _should_delete(peeked_msg, cutoff_time):
        return False
    return _delete_matching_message(receiver, peeked_msg.enqueued_time_utc)


def _process_peeked_messages(receiver, cutoff_time: datetime, max_peek: int) -> int:
    messages_deleted = 0
    last_sequence_number = None

    while True:
        peeked = receiver.peek_messages(
            max_message_count=max_peek,
            sequence_number=last_sequence_number + 1 if last_sequence_number else None
        )
        if not peeked:
            break

        for peeked_msg in peeked:
            last_sequence_number = peeked_msg.sequence_number
            if _handle_peeked_message(receiver, peeked_msg, cutoff_time):
                messages_deleted += 1

    return messages_deleted


def nuke_dead_letter_messages(connection_str: str, topic_name: str, subscription_name: str, period: str) -> int:
    """
    Nuke DLQ messages on a subscription.

    Parameters
    ----------
    connection_str : str
        The connection string to connect to the Service Bus namespace.

    topic_name : str
        The name of the topic.

    subscription_name : str
        The name of the subscription.

    period : str
        The cut off point to not delete messages later than as an ISO 8601
        duration.

    Returns
    -------
    int
        The number of dead letter messages that we removed.
    """
    cutoff_time = _parse_cutoff_time(period)
    max_peek = 100
    client = ServiceBusClient.from_connection_string(connection_str)

    receiver = client.get_subscription_receiver(
        topic_name=topic_name,
        subscription_name=subscription_name,
        sub_queue='deadletter',
        receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE
    )

    messages_deleted = _process_peeked_messages(receiver, cutoff_time, max_peek)

    receiver.close()
    client.close()
    return messages_deleted


if __name__ == '__main__':
    args = parser.parse_args()
    # Configuration
    if args.debug:
        logger.setLevel(logging.DEBUG)
    elif args.verbose:
        logger.setLevel(logging.INFO)

    messages_deleted = nuke_dead_letter_messages(
        connection_str=args.connection_string,
        topic_name=args.topic,
        subscription_name=args.subscription,
        period=args.period
    )
    logger.info(
        f'Deleted {messages_deleted:,} DLQ messages from subscription "{args.subscription}" older than {args.period}.'
    )
