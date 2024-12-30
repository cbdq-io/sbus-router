#!/usr/bin/env python
"""
A configurable router for Azure Service Bus messages.

LICENCE
-------
BSD 3-Clause License

Copyright (c) 2024, Cloud Based DQ Ltd.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
import itertools
import json
import logging
import os
import re
import sys
from string import Template
from urllib.parse import urlparse

import jmespath
import jsonschema
import jsonschema.exceptions
from azure.core.utils import parse_connection_string
from prometheus_client import Counter, Summary, start_http_server
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

PROCESSING_TIME = Summary('message_processing_seconds', 'The time spent processing messages.')
DLQ_COUNT = Counter('dlq_message_count', 'The number of messages sent to the DLQ.')


def get_logger(logger_name: str, log_level=os.getenv('LOG_LEVEL', 'WARN')) -> logging.Logger:
    """
    Provide a generic logger.

    Parameters
    ----------
    logger_name : str
        The name of the logger.
    log_level : str, optional
        The log level to set the logger to, by
        default os.getenv('LOG_LEVEL', 'WARN').

    Returns
    -------
    logging.Logger
        A logger that can be used to provide logging.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level=log_level)
    return logger


logging.basicConfig()
logger = get_logger(__file__)
__version__ = '0.2.0'


class ConnectionStringHelper:
    """
    A class for handling Azure Service Bus Connection Strings.

    Attributes
    ----------
    sbus_connection_string : str
        The value provided by the constructor.

    Parameters
    ----------
    connection_string : str
        An Azure Service Bus connection string.
    """

    def __init__(self, sbus_connection_string: str) -> None:
        self.sbus_connection_string = sbus_connection_string
        self.port(5671)
        self.protocol('amqps')
        self.parse()
        url = f'{self.protocol()}://{self.key_name()}:{self.key_value()}'
        url += f'@{self.hostname()}:{self.port()}'
        self.amqp_url(url)

    def amqp_url(self, amqp_url: str = None) -> str:
        """
        Get or set an AMQP URL.

        Parameters
        ----------
        amqp_url : str, optional
            The URL to be set, by default None

        Returns
        -------
        str
            The set URL.
        """
        if amqp_url is not None:
            self._amqp_url = amqp_url
        return self._amqp_url

    def hostname(self, hostname: str = None) -> str:
        """
        Get or set the host name.

        Parameters
        ----------
        hostname : str, optional
            The host name to be set, by default None

        Returns
        -------
        str
            The set host name.
        """
        if hostname is not None:
            self._hostname = hostname
        return self._hostname

    def key_name(self, key_name: str = None) -> str:
        """
        Get or set the key name.

        Parameters
        ----------
        key_name : str
            The key name to be set.

        Returns
        -------
        str
            The key name that is set.
        """
        if key_name is not None:
            self._key_name = key_name
        return self._key_name

    def key_value(self, key_value: str = None) -> str:
        """
        Get or set the key value.

        Parameters
        ----------
        key_value : str
            The key value to be set.

        Returns
        -------
        str
            The key value that is set.
        """
        if key_value is not None:
            self._key_value = key_value
        return self._key_value

    def parse(self) -> None:
        """
        Parse the connection string.

        Raises
        ------
        ValueError
            If mandatory components are missing in the connection string.
        """
        conn_str_components = dict(parse_connection_string(self.sbus_connection_string))
        use_development_emulator = conn_str_components.get('usedevelopmentemulator', 'False').capitalize()
        use_development_emulator = use_development_emulator == 'True'

        if use_development_emulator:
            self.port(5672)
            self.protocol('amqp')

        try:
            endpoint = conn_str_components['endpoint']
            url = urlparse(endpoint)
            self.hostname(url.netloc)
            self.key_name(conn_str_components['sharedaccesskeyname'])
            self.key_value(conn_str_components['sharedaccesskey'])
        except KeyError:
            raise ValueError(f'Connection string "{self.sbus_connection_string}" is invalid.')

    def port(self, port: int = None) -> int:
        """
        Get or set the port number.

        Parameters
        ----------
        port : int
            The port number to be set.

        Returns
        -------
        int
            The port number that is set.
        """
        if port is not None:
            self._port = port

        return self._port

    def protocol(self, protocol: str = None) -> str:
        """
        Get or set the protocol to be used for connecting to AMQP.

        Valid values are amqp or amqps.

        Parameters
        ----------
        protocol : str, optional
            The protocol to be used, by default None

        Returns
        -------
        str
            The protocol that has been set.
        """
        if protocol is not None:
            self._protocol = protocol

        return self._protocol


class RouterRule:
    """
    A class for handling a rule.

    Parameters
    ----------
    name : str
        The name of the rule.
    definition : str
        The rule definition (as a JSON string).

    Attributes
    ----------
    definition : str
        The definition as passed to the constructor.
    destination_namespaces : str
        The destination namespaces for messages matching this rule.
    destination_topics : str
        The destination  topics for the messages matching this rule.
    jmespath : str
        A JMESPath string for checking against a JSON payload.
    regexp : str
        A regular expression for comparing against the message, or if a
        JMESPath string was provided, it will be used to compare against
        the value returned from that.
    source_topic : str
        The name of the source topic that makes up some the the matching
        criteria for the rule.
    """

    def __init__(self, name: str, definition: str) -> None:
        self.definition = definition
        self.name(name)
        parsed_definition = self.parse_definition(definition)

        if parsed_definition['destination_namespaces']:
            self.destination_namespaces = parsed_definition['destination_namespaces'].split(',')
        else:
            self.destination_namespaces = []

        if parsed_definition['destination_topics']:
            self.destination_topics = parsed_definition['destination_topics'].split(',')
        else:
            self.destination_topics = []

        self.jmespath = parsed_definition.get('jmespath', None)
        self.regexp = parsed_definition.get('regexp', None)
        self.source_subscription = parsed_definition['source_subscription']
        self.source_topic = parsed_definition['source_topic']
        self.parsed_definition = parsed_definition

    def decode_message(self, message: bytes) -> str:
        """
        Decode bytes to a string.

        Parameters
        ----------
        message : bytes
            The message as a collection of bytes.

        Returns
        -------
        str
            The message decoded to be a string.
        """
        if isinstance(message, str):
            return message
        elif isinstance(message, memoryview):
            message = message.tobytes()

        return message.decode('utf-8')

    def get_data(self, message: object) -> list:
        """
        Get the data required from the message to do a comparison.

        If the message is binary, convert it to a string.  If the rule has a
        jmespath then try and parse the message into JSON (not a failure if
        this fails) and return the data from the jmes path for comparison.

        Parameters
        ----------
        message : object (str/bytes)
            The message to be parsed.

        Returns
        -------
        list
            The data for comparison.
        """
        message = self.decode_message(message)

        if not self.jmespath:
            return [message]

        try:
            message = json.loads(message)
        except json.decoder.JSONDecodeError:
            return []

        result = jmespath.search(self.jmespath, message)

        if isinstance(result, list):
            return list(itertools.chain.from_iterable(result))
        elif result is None:
            return []

        return [result]

    def is_data_match(self, message: str) -> bool:
        """
        Check if the message content matches a regexp.

        Parameters
        ----------
        message : str
            The message to be checked.

        Returns
        -------
        bool
            Does the data match.
        """
        data = self.get_data(message)
        print(f'data is "{data}".')
        prog = re.compile(self.regexp)

        if data and any(prog.search(element) for element in data):
            return True

        return False

    def is_match(self, source_topic_name: str, message: str) -> tuple:
        """
        Check if the provided message and source topics match this rule.

        Parameters
        ----------
        source_topic_name : str
            The name of the topic that this message was consumed from.
        message : str
            The message that was consumed from the topic.

        Returns
        -------
        tuple[bool, destination_topic, destination_namespace]
            A tuple containing if the rule is a match to the message, the
            destination namespaces(s) and the destination topics(s).
        """
        print(f'{source_topic_name}:{self.source_topic}')
        if source_topic_name == self.source_topic:
            if not self.regexp:
                return True, self.destination_namespaces, self.destination_topics
            elif self.is_data_match(message):
                return True, self.destination_namespaces, self.destination_topics

        # If we got here, it ain't a match.
        return (False, None, None)

    def name(self, name: str = None) -> str:
        """
        Get or set the rule name.

        Parameters
        ----------
        name : str, optional.
            The name of the rule being set.

        Returns
        -------
        str
            The name of the set rule.
        """
        if name is not None:
            self._name = name
        return self._name

    def parse_definition(self, definition: str) -> dict:
        """
        Parse the rule definition.

        Parameters
        ----------
        definition : str
            The rule definition (as a JSON string).

        Returns
        -------
        dict
            A parsed definition of the JSON string.

        Raises
        ------
        json.decoder.JSONDecodeError
            If the provided string can't be parsed as JSON.
        jsonschema.exceptions.ValidationError
            If the string was parsed as JSON, but doesn't comply with the
            schema of the rules.
        """
        with open('rule-schema.json', 'r') as stream:
            schema = json.load(stream)

        try:
            instance = json.loads(definition)
            jsonschema.validate(instance=instance, schema=schema)
        except json.decoder.JSONDecodeError as ex:
            logger.error(f'{self.name()} ("{definition}") is not valid JSON.  {ex}')
            sys.exit(2)
        except jsonschema.exceptions.ValidationError as ex:
            logger.error(f'{self.name()} is not valid {ex}')
            sys.exit(2)

        return instance


class ServiceBusNamespaces:
    """A class for holding details of Service Bus namespaces."""

    def __init__(self) -> None:
        self._namespaces = {}

    def add(self, name: str, connection_string: str) -> None:
        """
        Add a namespace.

        Parameters
        ----------
        name : str
            The name of the namespace (e.g. "gbdev").
        connection_string : str
            The connection string for connecting to the namespace.
        """
        helper = ConnectionStringHelper(connection_string)
        self._namespaces[name] = helper.amqp_url()

    def count(self) -> int:
        """
        Get the number of defined service bus namespaces.

        Returns
        -------
        int
            The count of service bus namespaces instances that have been
            defined in the config.
        """
        return len(self._namespaces)

    def get(self, name: str) -> str:
        """
        Get the connection string of a namespace by name.

        Parameters
        ----------
        name : str
            The assigned name (as given in the add method) of the namespace.

        Returns
        -------
        str
            The connection string of the namespace.

        Raises
        ------
        ValueError
            If the provided name is not known.
        """
        conn_str = self._namespaces[name]
        return conn_str


class EnvironmentConfigParser:
    """
    Parse the environment variables for configuration.

    Parameters
    ----------
    environ : dict, optional
        The dictionary to consume variables from, by default is os.environ.
    """

    def __init__(self, environ: dict = dict(os.environ)) -> None:
        self._environ = environ

    def get_dead_letter_queue(self) -> str:
        """
        Return the DLQ topic name.

        Returns
        -------
        str
            The name of the DLQ topic.
        """
        return self._environ['ROUTER_DLQ_TOPIC']

    def get_prefixed_values(self, prefix: str) -> list:
        """
        Get values from the environment that match a prefix.

        Parameters
        ----------
        prefix : str
            The prefix to look for.

        Returns
        -------
        list
            A list of list items, where each item contains two string
            elements representing the key and the value.
        """
        response = []

        for key in sorted(self._environ.keys()):
            if key.startswith(prefix):
                response.append([key, self._environ[key]])

        return response

    def get_prometheus_port(self) -> int:
        """
        Get the prometheus port.

        If no port is specified, default to 8000.

        Returns
        -------
        int
            The port to be used with Prometheus.
        """
        port = self._environ.get('ROUTER_PROMETHEUS_PORT', '8000')
        return int(port)

    def get_rules(self) -> list[RouterRule]:
        """
        Extract a list of routing rules from the environment.

        Returns
        -------
        list
            A list of RouterRule objects.
        """
        response = []

        for item in self.get_prefixed_values('ROUTER_RULE_'):
            name = item[0].replace('ROUTER_RULE_', '')
            template = Template(item[1])
            definition = template.safe_substitute(os.environ)
            response.append(RouterRule(name, definition))

        return response

    def get_source_url(self) -> str:
        """
        Get the URL of the source Service Bus Namespace.

        Returns
        -------
        str
            A URL suitable for AMQP connection.
        """
        connection_string = self._environ['ROUTER_SOURCE_CONNECTION_STRING']
        helper = ConnectionStringHelper(connection_string)
        return helper.amqp_url()

    def service_bus_namespaces(self) -> ServiceBusNamespaces:
        """
        Get the Service Bus namespaces as defined in the environment.

        Returns
        -------
        ServiceBusNamespaces
            The Service Bus namspaces as defined in the configuration.

        Raises
        ------
        ValueError
            Raised if no namespaces have been defined.
        """
        env_values = self.get_prefixed_values('ROUTER_NAMESPACE_')
        response = ServiceBusNamespaces()

        for key_value_pair in env_values:
            key = key_value_pair[0]
            value = key_value_pair[1]
            key_elements = key.split('_')

            if len(key_elements) == 5 and key.endswith('CONNECTION_STRING'):
                name = key_elements[2]
                response.add(name, value)

        if response.count() == 0:
            raise ValueError('No namespace configuration found in environment.')

        return response

    def topics_and_subscriptions(self) -> list:
        """
        Extract a dictionary of the topics and subscriptions.

        Returns
        -------
        list
            A list of dictionaries.  Each dictionary will contain two
            elements called topic and subscription.
        """
        response = []
        found_values = []
        rules = self.get_rules()

        for rule in rules:
            value = f'{rule.source_subscription}:{rule.source_topic}'

            if value not in found_values:
                response.append(
                    {
                        'subscription': rule.source_subscription,
                        'topic': rule.source_topic
                    }
                )
                found_values.append(value)

        return response


class Router(MessagingHandler):
    """An implementation for the MessagingHandler."""

    def __init__(self, config: EnvironmentConfigParser):
        super().__init__()
        self.config = config
        self.connections = {}
        self.dlq_topic = config.get_dead_letter_queue()
        self.namespaces = config.service_bus_namespaces()
        self.rules = config.get_rules()
        self.senders = {}
        self.sources = []
        self.source_namespace_url = config.get_source_url()
        self.parse_config_data()
        start_http_server(config.get_prometheus_port())

    def forward_message(self, message: Message, destination_namespaces: list, destination_topics: list):
        """
        Forward a message to specified topics synchronously.

        Parameters
        ----------
        message : proton.Message
            The message to forward.
        destination_namespaces : list
            Namespaces where the message will be forwarded.
        destination_topics : list
            Topics where the message will be forwarded.
        """
        for idx, namespace_name in enumerate(destination_namespaces):
            destination_topic = destination_topics[idx]
            sender_name = f'{namespace_name}/{destination_topic}'
            sender = self.senders[sender_name]
            sender.send(message)
            logger.debug(f'Message sent to {destination_topic} in namespace {namespace_name}.')

    def handle_dlq_message(self, message: Message, source_topic_name: str) -> None:
        """
        Send a message to the DLQ.

        Parameters
        ----------
        message : proton.Message
            The message that needs to be sent to the DLQ.
        source_topic_name : str
            The name of the DLQ topic.
        """
        properties = message.properties

        if properties is None:
            properties = {}

        properties['source_topic'] = source_topic_name
        message.properties = properties
        message.send(self.senders['DLQ'])
        logger.warning('Message sent to the DLQ.')
        DLQ_COUNT.inc()

    def on_message(self, event):
        """Handle a message event."""
        message = event.message
        logger.debug(f'Message event "{message.body}" from {event.link.source.address}.')
        source_topic = event.link.source.address.split('/')[0]
        self.process_message(source_topic, message)

    def on_sendable(self, event):
        """Respond to a sendable event."""
        logger.debug('Sendable event.')

    def on_start(self, event):
        """Respond to a start event."""
        logger.debug('Start event.')

        for url in self.connections.keys():
            hostname = urlparse(url).hostname
            logger.debug(f'Creating a connection for {hostname}...')
            connection = event.container.connect(url, allowed_mechs='PLAIN')
            self.connections[url] = connection
            logger.info(f'Successfully created a connection for {hostname}.')

        connection = self.connections[self.source_namespace_url]

        for source in self.sources:
            logger.debug(f'Creating a receiver for "{source}".')
            event.container.create_receiver(connection, source=source)

        logger.debug(f'Creating a sender for the DLQ "{self.dlq_topic}"...')
        dlq_sender = event.container.create_sender(connection, target=self.dlq_topic)

        for sender in self.senders.keys():
            logger.debug(f'Creating a sender for "{sender}".')
            namespace, topic = tuple(sender.split('/'))
            url = self.namespaces.get(namespace)
            connection = self.connections[url]
            self.senders[sender] = event.container.create_sender(connection, target=topic)

        self.senders['DLQ'] = dlq_sender

    def parse_config_data(self) -> dict:
        """Create a dict of connections with the URL as a key."""
        url_list = [self.source_namespace_url]
        service_bus_namespaces = self.config.service_bus_namespaces()
        sources = []

        for rule in self.rules:
            source = f'{rule.source_topic}/Subscriptions/{rule.source_subscription}'
            sources.append(source)

            for idx, dest_namespace in enumerate(rule.destination_namespaces):
                url = service_bus_namespaces.get(dest_namespace)
                url_list.append(url)
                dest_topic = rule.destination_topics[idx]
                sender = f'{dest_namespace}/{dest_topic}'
                self.senders[sender] = None

        # Make URL list unique values.
        url_list = list(set(url_list))

        for url in url_list:
            self.connections[url] = None

        self.sources = list(set(sources))

    @PROCESSING_TIME.time()
    def process_message(self, source_topic: str, message: Message) -> int:
        """
        Process the received message asynchronously.

        Parameters
        ----------
        source_topic : str
            The name of the topic where the message was received from.
        message: proton.Message
            The message to be processed.

        Returns
        -------
        int
            Zero if message is matched to a rule and forwarded successfully,
            One if message is matched, but an error occurs during forwarding.
            Two if the message is not matched to any rules.
        """
        logger.debug('Process message...')

        for rule in self.rules:
            is_match, destination_namespaces, destination_topics = rule.is_match(source_topic, message.body)

            if is_match:
                try:
                    logger.debug(f'Matched message successfully to rule {rule.name()}.')
                    self.forward_message(message, destination_namespaces, destination_topics)
                    logger.debug(f'Message forwarded to destinations: {destination_topics}')
                    return 0
                except Exception as e:
                    logger.error(f'Failed to forward message: {e}')
                    return 1

        self.handle_dlq_message(message, source_topic)
        return 2


if __name__ == '__main__':
    logger.info(f'Starting version "{__version__}".')
    router = Router(EnvironmentConfigParser())
    container = Container(router)
    container.run()
