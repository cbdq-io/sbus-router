#!/usr/bin/env python
"""A utility script to replay messages on the DLQ topic."""
import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path

from proton import Disposition, Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

from router import ConnectionStringHelper


class RuntimeParams:
    """
    A class for parsing the runtime parameters.

    Attributes
    ----------
    connection_string : str
        The connection string to attach to the Service Bus namespace.
    dlq_topic_name : str
        The name of the DLQ topic.
    log_level : int
        The log level.  Will either be logging.DEBUG, logging.INFO or
        logging.WARN.

    Parameters
    ----------
    args : list
        A list of strings to parse.  The default is sys.argv.
    """

    def __init__(self, args: list = sys.argv[1:]):
        self.prog = self.get_prog()
        parser = argparse.ArgumentParser(prog=self.prog)
        log_level_group = parser.add_mutually_exclusive_group()
        log_level_group.add_argument(
            '-d', '--debug',
            help='Set log level to DEBUG.',
            action='store_true'
        )
        log_level_group.add_argument(
            '-v', '--verbose',
            help='Set log level to INFO.',
            action='store_true'
        )
        parser.add_argument(
            '-c', '--connection-string',
            help='The connection string of the Service Bus namespace.',
            default=os.environ.get(
                'ROUTER_SOURCE_CONNECTION_STRING',
                None
            )
        )
        parser.add_argument(
            '-n', '--name',
            help='The name of the DLQ topic.',
            default=os.environ.get('ROUTER_DLQ_TOPIC', None),
        )
        parser.add_argument(
            '-s', '--subscription',
            help='The name of the subscription to read the DLQ topic with.',
            required=True
        )
        parser.add_argument(
            '-t', '--timeout',
            help='How long to wait for another message on the DLQ in seconds.',
            type=int,
            default=10
        )
        args = parser.parse_args(args)

        if args.debug:
            log_level = logging.DEBUG
        elif args.verbose:
            log_level = logging.INFO
        else:
            log_level = logging.WARN

        logging.basicConfig()
        logger = logging.getLogger(self.prog)
        logger.setLevel(log_level)
        self.logger = logger
        self.connection_string = args.connection_string
        self.dlq_topic_name = args.name
        self.timeout = args.timeout
        self.subscription = args.subscription

    def get_prog(self) -> str:
        """
        Get a suitable prog name for this script.

        Return
        ------
        str
            A suitable prog name for this script.
        """
        prog = Path(os.path.basename(__file__))
        prog = prog.with_suffix('')
        return str(prog)


class DLQReplayHandler(MessagingHandler):
    """
    A class to replay DLQ messages.

    Parameters
    ----------
    dlq_topic_name : str
        The name of the DLQ topic.
    connection : str
        The URL to connect to the Service Bus namespace.
    timeout : int
        How long to wait for messages to arrive before exiting.
    """

    def __init__(self, dlq_topic_name, connection: ConnectionStringHelper, timeout, logger: logging.Logger):
        super().__init__()
        self.dlq_topic_name = dlq_topic_name
        self.connection = connection
        self.timeout = timeout
        self.last_message_time = None
        self.reactor = None
        self.logger = logger
        self.senders = {}
        self.message_hashes = []

    def get_message_hash(self, message: Message) -> str:
        """
        Get the MD5 hash of a message contents.

        Parameters
        ----------
        message : proton.Message
            The message to get a hash of.

        Returns
        -------
        str
            An MD5 hash of the message contents.
        """
        body = message.body

        if type(body) is str:
            body = body.encode()

        return hashlib.md5(body, usedforsecurity=False).hexdigest()

    def on_start(self, event):
        """Execute a start event."""
        self.reactor = event.reactor
        self.logger.debug(f'Creating a connection for "{self.connection.netloc()}".')
        self.conn = event.container.connect(
            url=self.connection.netloc(),
            allowed_mechs='PLAIN',
            password=self.connection.key_value(),
            user=self.connection.key_name()
        )
        self.logger.debug(f'Creating a receiver for "{self.dlq_topic_name}...".')
        self.receiver = event.container.create_receiver(self.conn, source=self.dlq_topic_name)
        self.logger.debug(f'Receiver for "{self.dlq_topic_name}" created successfully.')
        self.last_message_time = event.reactor.now
        event.reactor.schedule(1.0, self)

    def on_message(self, event):
        """Execute a message event."""
        message = event.message
        delivery = event.delivery
        self.logger.debug(f'Received message: {message.body}')
        hash = self.get_message_hash(message)

        if hash in self.message_hashes:
            self.logger.warning('A message with the same hash has already been processed.')
            delivery.update(Disposition.REJECTED)
            return

        # Extract the source topic
        source_topic = message.properties.get('source_topic')
        if not source_topic:
            self.logger.error("Message does not have a 'source_topic' property. Rejecting.")
            delivery.update(Disposition.REJECTED)
            return

        self.logger.debug(f'Forwarding message to source topic: {source_topic}')
        try:
            # Reuse or create a sender for the topic
            if source_topic not in self.senders:
                self.senders[source_topic] = event.container.create_sender(self.conn, source_topic)
                self.logger.debug(f'Created new sender for topic {source_topic}.')

            # Send the message
            forward_message = Message(
                body=message.body,
                properties=message.properties
            )
            self.senders[source_topic].send(forward_message)
            self.logger.debug(f'Message successfully forwarded to {source_topic}.')

            # Mark the message as accepted
            delivery.update(Disposition.ACCEPTED)
            self.message_hashes.append(hash)
        except Exception as e:
            self.logger.error(f'Failed to forward message to {source_topic}: {e}')
            # Reject the message if forwarding fails
            delivery.update(Disposition.REJECTED)
        finally:
            # Update the last message time
            self.last_message_time = event.reactor.now

    def on_timer_task(self, event):
        """Check for timeout on consuming from the DLQ."""
        if event.reactor.now - self.last_message_time > self.timeout:
            self.logger.info(f'Timeout ({self.timeout}s) reached with no messages.')
            self.reactor.stop()
        else:
            # Reschedule the timer task
            event.reactor.schedule(1.0, self)

    def on_connection_close(self, event):
        """Handle connection closed event."""
        for sender in self.senders.values():
            sender.close()
        self.logger.info('Connection closed and senders cleaned up.')


if __name__ == '__main__':
    runtime = RuntimeParams()
    helper = ConnectionStringHelper(runtime.connection_string)
    replayer = DLQReplayHandler(
        f'{runtime.dlq_topic_name}/Subscriptions/{runtime.subscription}',
        helper,
        runtime.timeout,
        runtime.logger
    )
    Container(replayer).run()
