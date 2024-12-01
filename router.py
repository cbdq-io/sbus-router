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
import asyncio
import json
import logging
import os
import re
import sys
from urllib.parse import urlparse

import jmespath
import jsonschema
import jsonschema.exceptions
from azure.core.utils import parse_connection_string
from proton import Event, Message
from proton.handlers import MessagingHandler
from proton.reactor import Container


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
__version__ = '0.1.0'


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
            return result
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
        except json.decoder.JSONDecodeError:
            logger.error(f'{self.name()} ("{definition}") is not valid JSON.')
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
        return os.environ['ROUTER_DLQ_TOPIC']

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

    def get_rules(self) -> list:
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
            definition = item[1]
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
        connection_string = os.environ['ROUTER_SOURCE_CONNECTION_STRING']
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


class AsyncSubscriptionHandler(MessagingHandler):
    """
    An implementation for the MessagingHandler.

    Parameters
    ----------
    url : str
        The URL to the subscription for the topic.
    topic : str
        The name of the topic to subscribe to.
    subscription : str
        The name of the subscription to subscribe to.
    """

    def __init__(self, url: str, topic: str, subscription: str, rules: list[RouterRule]):
        super().__init__()
        self.url = url
        self.topic = topic
        self.subscription = subscription
        self.rules = rules
        config = EnvironmentConfigParser()
        self.namespaces = config.service_bus_namespaces()
        self.dlq = config.get_dead_letter_queue()
        self.connections = {}
        self.senders = {}

    def get_or_create_sender(self, url: str, topic: str):
        """
        Get or create a sender for the specified URL and topic within the same Container.

        Parameters
        ----------
        url : str
            The Service Bus or broker URL.
        topic : str
            The topic to which the sender will send messages.

        Returns
        -------
        Sender
            A sender object for the topic.
        """
        if topic not in self.senders:
            try:
                # Retrieve or create the connection
                if url not in self.connections:
                    logger.debug(f'Creating connection for URL: {url}')
                    connection = self.container.connect(url)
                    self.connections[url] = connection
                else:
                    connection = self.connections[url]

                # Create a session on the connection
                logger.debug(f'Creating session for connection: {url}')
                session = connection.session()

                # Create the sender on the session
                logger.debug(f'Creating sender for topic: {topic} on connection: {url}')
                sender = session.sender(topic)
                self.senders[topic] = sender
                logger.debug(f'Successfully created sender for topic: {topic}')
            except Exception as e:
                logger.error(f'Failed to create sender for topic {topic}: {e}')
                raise

        return self.senders[topic]

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
            url = self.namespaces.get(namespace_name)
            if not url:
                raise ValueError(f'Namespace {namespace_name} not found.')

            destination_topic = destination_topics[idx]
            sender = self.get_or_create_sender(url, destination_topic)
            sender.send(message)
            logger.debug(f'Message sent to {destination_topic} in namespace {namespace_name}.')

    def on_start(self, event):
        """
        Initialise the connection to the topic/subscription on the namespace.

        Parameters
        ----------
        event : proton.Event
            The event that needs to be handled.
        """
        logger.debug(f'Connecting to "{self.url}"')
        connection = event.container.connect(self.url)
        self.connections[self.url] = connection
        logger.debug(f'Creating receiver for {self.topic}/Subscriptions/{self.subscription}')
        event.container.create_receiver(connection, source=f'{self.topic}/Subscriptions/{self.subscription}')
        logger.debug(f'Connected successfully to {self.topic}/{self.subscription}')

    def on_message(self, event: Event):
        """
        Handle a message event.

        Parameters
        ----------
        event : proton.Event
            The event that needs to be handled.
        """
        message = event.message
        logger.debug(f'Message received on {self.topic}/{self.subscription}: {message.body}')

        try:
            result = self.process_message(message)

            if result == 0:
                event.delivery.settle()
                logger.debug(f'Message successfully processed and acknowledged: {message.body}')
            elif result == 1:
                logger.error(f'Message matched but forwarding failed, will retry: {message.body}')
                event.delivery.abort()
            else:
                event.delivery.settle()
                logger.debug(f'Message sent to DLQ and acknowledged: {message.body}')
        except Exception as e:
            logger.error(f'Error processing message: {e}')

    def process_message(self, message: Message) -> int:
        """
        Process the received message asynchronously.

        Parameters
        ----------
        message: proton.Message
            The message to be processed.

        Returns
        -------
        int
            Zero if message is matched to a rule and forwarded successfully,
            One if message is matched, but an error occurs during forwarding.
            Two if the message is not matched to any rules.
        """
        for rule in self.rules:
            is_match, destination_namespaces, destination_topics = rule.is_match(self.topic, message.body)

            if is_match:
                try:
                    logger.debug(f'Matched message successfully to rule {rule.name()}.')
                    self.forward_message(message, destination_namespaces, destination_topics)
                    logger.debug(f'Message forwarded to destinations: {destination_topics}')
                    return 0
                except Exception as e:
                    logger.error(f'Failed to forward message: {e}')
                    return 1

        logger.warning(f'No rules matched for message: {message.body}')
        self.forward_message(message, [self.url], [self.dlq])
        return 2


async def listen_to_subscription(topic: str, subscription: str):
    """
    Listen to a subscription on a topic.

    Parameters
    ----------
    topic : str
        The name of the topic to be listened on.
    subscription : str
        The name of the subscription that is associated with the topic.
    """
    config = EnvironmentConfigParser()
    url = config.get_source_url()
    handler = AsyncSubscriptionHandler(
        url,
        topic,
        subscription,
        config.get_rules()
    )
    container = Container(handler)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, container.run)


async def main():
    """Configure the async tasks."""
    config = EnvironmentConfigParser()
    tasks = []

    for ts in config.topics_and_subscriptions():
        tasks.append(listen_to_subscription(ts['topic'], ts['subscription']))

    if len(tasks) == 0:
        logger.error('Zero tasks created for processing.')
        sys.exit(2)

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    logger.info(f'Starting version "{__version__}".')
    asyncio.run(main())
