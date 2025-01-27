#!/usr/bin/env python
import argparse
import json
import logging
import os
import time

from proton._events import Event
from proton._exceptions import ProtonException
from proton.handlers import MessagingHandler
from proton.reactor import Container

logging.basicConfig()
logger = logging.getLogger(os.path.basename(__file__))


class CheckTopicExists(MessagingHandler):
    def __init__(self, url: str, topic_name: str) -> None:
        super(CheckTopicExists, self).__init__()
        self.url = url
        self.topic_name = topic_name
        self.sender = None
        self.topic_exists = False

    def on_start(self, event: Event) -> None:
        try:
            logger.debug(f'Opening a connection to <{self.url}>...')
            self.conn = event.container.connect(self.url)
            logger.debug(f'Connection made for <{self.url}>.')
            logger.debug(f'Creating a sender for "{self.topic_name}"...')
            self.sender = event.container.create_sender(self.conn, self.topic_name)
            logger.debug(f'Sender created for "{self.topic_name}".')
        except ProtonException as e:
            logger.warning(e)
            event.container.stop()

    def on_link_error(self, event: Event):
        logger.warning(f'Unable to create a sender for <{self.topic_name}>.')
        event.connection.close()

    def on_sendable(self, event):
        logger.info(f'The {self.topic_name} topic is ready on the emulator.')
        self.topic_exists = True
        event.connection.close()


class ConnectionStringParser:
    """
    Parse a connection string to extract the hostname and credentials.

    Attributes
    ----------
    hostname : str
        The hostname to connect to.
    netloc : str
        The hostname and port to connect to (e.g. foo.servicebus.windows.net:5671 or localhost:5672).
    password : str
        The password to use for credentials.
    port : int
        The port to use when connecting.
    scheme : str
        The protocol to use to connect to the server (e.g. amqp or amqps).
    username : str
        The user name to use for credentials.

    Parameters
    ----------
    connection_string : str
        The Service Bus connection string.
    """

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

        if 'UseDevelopmentEmulator=true' in connection_string:
            self.port = 5672
            self.scheme = 'amqp'
        else:
            self.port = 5671
            self.scheme = 'amqps'

        logger.debug(f'Port is {self.port}.')
        logger.debug(f'Scheme is {self.scheme}')

        self.hostname = self.get_component('endpoint').split('/')[-1]
        logger.debug(f'Hostname is {self.hostname}.')
        self.netloc = f'{self.hostname}:{self.port}'
        logger.debug(f'Netloc is "{self.netloc}".')
        self.username = self.get_component('SharedAccessKeyName')
        self.password = self.get_component('SharedAccessKey')

    def get_component(self, component_name: str) -> str:
        """
        Get a named component from the connection string.

        Parameters
        ----------
        component_name : str
            The name of the component to be extracted (e.g. Endpoint).  Is
            non-case sensitive.

        Returns
        -------
        str
            The value of the component or None if component is not found.
        """
        response = None
        search_name = component_name.lower()
        logger.debug(f'Searching for "{search_name}" in the connection string.')
        components = self.connection_string.split(';')

        for component in components:
            items = component.split('=')

            if items[0].lower() != search_name:
                logger.debug(f'Ignoring "{items[0]}".')
                continue

            response = component.removeprefix(f'{items[0]}=')
            break

        return response


parser = argparse.ArgumentParser()
default_connection_string = 'Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;'
default_connection_string += 'SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
parser.add_argument(
    '-a', '--attempts',
    help='The number of attempts to connect.',
    type=int,
    default=60
)
parser.add_argument(
    '-c', '--connection-string',
    help='The connection string of the Service Bus emulator.',
    default=default_connection_string
)
group = parser.add_mutually_exclusive_group()
group.add_argument(
    '-d', '--debug',
    help='Set DEBUG level logging.',
    action='store_true'
)
group.add_argument(
    '-v', '--verbose',
    help='Set INFO level logging.',
    action='store_true'
)
parser.add_argument(
    '-w', '--wait',
    help='The time to wait between attempts (in seconds).',
    type=int,
    default=1
)
parser.add_argument(
    '-f', '--file',
    help='The path to the Service Bus emulator config file.',
    required=True
)

args = parser.parse_args()

if args.debug:
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG)
elif args.verbose:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.WARN)

logger.info(f'Log level set to {logging.getLevelName(logger.getEffectiveLevel())}')
connection_string = ConnectionStringParser(args.connection_string)
MAX_ATTEMPTS = args.attempts

with open(args.file, 'rt') as stream:
    emulator_config = json.load(stream)

topics = emulator_config['UserConfig']['Namespaces'][0]['Topics']

for topic in topics:
    topic_name = topic['Name']
    attempt = 1
    is_connected = False

    while attempt <= args.attempts:
        handler = CheckTopicExists(connection_string.netloc, topic_name)
        container = Container(handler)
        container.run()
        is_connected = handler.topic_exists

        if is_connected:
            break

        time.sleep(args.wait)
        attempt += 1
