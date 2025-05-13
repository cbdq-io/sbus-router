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
import importlib
import json
import logging
import os
import re
import signal
import sys
from collections import defaultdict
from string import Template

import jmespath
import jsonschema
import jsonschema.exceptions
from azure.servicebus import (NEXT_AVAILABLE_SESSION, ServiceBusMessage,
                              ServiceBusReceivedMessage)
from azure.servicebus.aio import (AutoLockRenewer, ServiceBusClient,
                                  ServiceBusReceiver)
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.exceptions import OperationTimeoutError
from prometheus_client import Counter, Summary, start_http_server

__version__ = '0.5.0'
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


logging.basicConfig(format='%(levelname)s [%(filename)s:%(lineno)d] %(message)s')
logger = get_logger(__file__)
custom_sender_string = os.getenv('ROUTER_CUSTOM_SENDER')

if custom_sender_string:
    logger.info(f'Configuring custom sender "{custom_sender_string}".')
    module_path, func_name = custom_sender_string.split(':')
    module = importlib.import_module(module_path)
    custom_sender = getattr(module, func_name)
else:
    custom_sender = None


async def extract_message_body(message: ServiceBusMessage) -> str:
    """
    Extract and return the message body as a UTF-8 string.

    Uses `message.body_type` to handle different encoding scenarios.

    Parameters
    ----------
    message : ServiceBusMessage
        The message received from Azure Service Bus.

    Returns
    -------
    str
        The extracted message body as a string.

    Raises
    ------
    TypeError
        If the body type is unsupported.
    """
    body_type = message.body_type

    if body_type == AmqpMessageBodyType.DATA:
        # Body is binary data (bytes or memoryview)
        return b''.join(message.body).decode()

    if body_type == AmqpMessageBodyType.SEQUENCE:
        # Body is a sequence (list of values) -> Convert to a JSON string
        return json.dumps(message.body)

    if body_type == AmqpMessageBodyType.VALUE:
        # Body is a single value (string, integer, JSON object) -> Convert to str
        return str(message.body)

    raise TypeError(f'Unsupported message body type: {body_type}')


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
    destination_namespaces : list
        The destination namespaces for messages matching this rule.
    destination_topics : list
        The destination topics for the messages matching this rule.
    jmespath : str
        A JMESPath string for checking against a JSON payload.
    regexp : str
        A regular expression for comparing against the message.
    source_topic : str
        The name of the source topic that makes up some of the matching criteria for the rule.
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

        self.is_session_required = parsed_definition.get('is_session_required', False)
        self.jmespath = parsed_definition.get('jmespath', None)
        self.regexp = parsed_definition.get('regexp', None)

        if self.regexp:
            self.prog = re.compile(self.regexp)
        else:
            self.prog = None

        self.source_subscription = parsed_definition['source_subscription']
        self.source_topic = parsed_definition['source_topic']
        self.parsed_definition = parsed_definition

    def flatten_list(self, data: list) -> list:
        """
        Flatten a possibly deeply nested list.

        Parameters
        ----------
        data : list
            The list to be flattened.

        Returns
        -------
        list
            A flattened list.
        """
        flat_list = []
        for item in data:
            if isinstance(item, list):
                flat_list.extend(self.flatten_list(item))  # Recursively flatten it
            else:
                flat_list.append(item)
        return flat_list

    def get_data(self, message_body: str) -> list:
        """
        Extract the relevant data from the message body for rule evaluation.

        If the rule has a JMESPath, it attempts to extract relevant data.

        Parameters
        ----------
        message_body : str
            The message body as a string.

        Returns
        -------
        list
            The extracted data for comparison.
        """
        if not self.jmespath:
            return [message_body]

        try:
            message_json = json.loads(message_body)
        except json.decoder.JSONDecodeError:
            return []

        result = jmespath.search(self.jmespath, message_json)

        if isinstance(result, list):
            return self.flatten_list(result)
        if result is None:
            return []

        return [result]

    def is_data_match(self, message_body: str) -> bool:
        """
        Check if the message content matches a regular expression.

        Parameters
        ----------
        message_body : str
            The message body to be checked.

        Returns
        -------
        bool
            True if the message matches, otherwise False.
        """
        data = self.get_data(message_body)
        return any(self.prog.search(element) for element in data) if data else False

    def is_match(self, source_topic_name: str, message_body: str) -> tuple:
        """
        Check if the provided message and source topics match this rule.

        Parameters
        ----------
        source_topic_name : str
            The name of the topic that this message was consumed from.
        message_body : str
            The message body as a string.

        Returns
        -------
        tuple[bool, list, list]
            A tuple containing:
            - Whether the rule matches the message
            - The destination namespaces
            - The destination topics
        """
        if source_topic_name == self.source_topic:
            if not self.regexp or self.is_data_match(message_body):
                return True, self.destination_namespaces, self.destination_topics

        return False, None, None

    def name(self, name: str = None) -> str:
        """
        Get or set the rule name.

        Parameters
        ----------
        name : str, optional
            The name of the rule being set.

        Returns
        -------
        str
            The rule name.
        """
        if name is not None:
            self._name = name
        return self._name

    def parse_definition(self, definition: str) -> dict:
        """
        Parse and validate the rule definition.

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
            If the string was parsed as JSON, but doesn't comply with the schema of the rules.
        """
        with open('rule-schema.json', 'r') as schema_file:
            schema = json.load(schema_file)

        try:
            instance = json.loads(definition)
            jsonschema.validate(instance=instance, schema=schema)
        except json.decoder.JSONDecodeError as ex:
            logger.error(f'Rule "{self.name()}" contains invalid JSON: {ex}')
            sys.exit(2)
        except jsonschema.exceptions.ValidationError as ex:
            logger.error(f'Rule "{self.name()}" does not conform to schema: {ex}')
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
        self._namespaces[name] = connection_string

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
        return self._namespaces[name]

    def get_all_namespaces(self) -> dict:
        """
        Return a dictionary of all namespaces.

        The key is the namespace name, the value is the connection string.
        """
        return self._namespaces


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

    def get_source_connection_string(self) -> str:
        """Get the connection string of the source namespace."""
        return self._environ['ROUTER_SOURCE_CONNECTION_STRING']

    def max_tasks(self) -> int:
        """Get the max number of tasks per source subscription."""
        return int(self._environ.get('ROUTER_MAX_TASKS', '1'))

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

    def topics_and_subscriptions(self) -> list[tuple]:
        """
        Extract a dictionary of the source topics and subscriptions.

        Returns
        -------
        list[tuple]
            A list of tuples.  Each tuple contains two elements.  The first
            is the topic name, the second is the subscription name.
        """
        response = []
        rules = self.get_rules()

        for rule in rules:
            instance = (rule.source_topic, rule.source_subscription)

            if instance not in response:
                response.append(instance)

        return response


class ServiceBusHandler:
    """
    A handler to process async Service Bus messages.

    Parameters
    ----------
    config : EnvironmentConfigParser
        The configuration as set by environment variables.
    """

    def __init__(self, config: EnvironmentConfigParser):
        self.source_connection_string = config.get_source_connection_string()
        self.namespaces = config.service_bus_namespaces().get_all_namespaces()  # Used for sending, not receiving
        self.input_topics = config.topics_and_subscriptions()
        self.rules = config.get_rules()
        self.max_tasks = config.max_tasks()
        logger.info(f'Starting {self.max_tasks} task(s) per subscription.')

        for idx, rule in enumerate(self.rules):
            logger.info(f'Rule parsing order {idx} {rule.name()}')

        self.source_client = None  # Used for receiving
        self.clients = {}  # Used for sending
        self.senders = {}
        self.lock_renewer = AutoLockRenewer()
        self.sender_locks = defaultdict(asyncio.Lock)
        self.sender_send_locks = defaultdict(asyncio.Lock)
        self.rules_by_topic = defaultdict(list)

        for rule in self.rules:
            self.rules_by_topic[rule.source_topic].append(rule)

    async def close(self):
        """Gracefully close all clients and senders."""
        logger.warning('Closing all connections on shutdown.')
        await asyncio.gather(*(sender.close() for sender in self.senders.values()))
        await asyncio.gather(*(client.close() for client in self.clients.values()))
        if self.source_client:
            await self.source_client.close()

    async def get_receiver(self, topic_name: str, subscription_name: str) -> ServiceBusReceiver:
        """Get a receiver for a topic/subscription."""
        if self.is_session_required(topic_name, subscription_name):
            receiver = self.source_client.get_subscription_receiver(
                topic_name=topic_name,
                subscription_name=subscription_name,
                session_id=NEXT_AVAILABLE_SESSION,
                auto_lock_renewer=self.lock_renewer,
                max_auto_renew_duration=300,
                max_wait_time=5,
                prefetch_count=20
            )
            logger.debug(f'Created a receiver for {topic_name}/{subscription_name} ({receiver.session.session_id})')
        else:
            logger.debug(f'Creating a non-sessioned receiver for {topic_name}/{subscription_name}...')
            receiver = self.source_client.get_subscription_receiver(
                topic_name=topic_name,
                subscription_name=subscription_name
            )

        return receiver

    async def get_sender(self, namespace: str, topic: str):
        """
        Retrieve or create a sender for the given namespace and topic.

        Parameters
        ----------
        namespace : str
            The name of the destination namespace.
        topic : str
            The name of the destination topic.
        """
        key = (namespace, topic)

        async with self.sender_locks[key]:
            sender = self.senders.get(key)

            if sender is not None:
                return sender

            logger.debug(f'Creating a new sender for {namespace}/{topic}.')
            client = self.clients.get(namespace)

            if not client:
                raise ValueError(f'Namespace "{namespace}" not found in configuration.')

            sender = client.get_topic_sender(topic)

            try:
                await sender.__aenter__()  # Initialize sender
            except Exception as e:
                logger.error(f'Failed to enter sender context for {namespace}/{topic}: {e}')
                raise

            # Only store the sender after successful init
            self.senders[key] = sender
            return sender

    def is_session_required(self, source_topic: str, source_subscription: str) -> bool:
        """Check if a source topic/subscription requires sessions or not."""
        for rule in self.rules:
            if rule.source_topic == source_topic and rule.source_subscription == source_subscription:
                return rule.is_session_required

        return False

    @PROCESSING_TIME.time()
    async def process_message(self, source_topic: str, message: ServiceBusReceivedMessage,
                              receiver: ServiceBusReceiver):
        """
        Process the received message asynchronously.

        Parameters
        ----------
        source_topic : str
            The name of the topic where the message was received from.
        message : azure.servicebus.ServiceBusMessage
            The message to be processed.
        receiver : azure.servicebus.ServiceBusReceiver
            The receiver that the message came in on.
        """
        message_body = await extract_message_body(message)

        for rule in self.rules_by_topic.get(source_topic, []):
            is_match, destination_namespaces, destination_topics = rule.is_match(
                source_topic,
                message_body
            )

            if is_match:
                try:
                    logger.debug(f'Successfully matched message to {rule.name()}.')
                    await self.send_message(destination_namespaces, destination_topics, message_body,
                                            message.application_properties)
                    await receiver.complete_message(message)
                    return
                except Exception as e:
                    logger.error(f'Failed to send message: {e}')
                    await receiver.abandon_message(message)
                    return

        # No matching rule: Send to DLQ
        logger.warning(f'No rules match message from {source_topic}, sending to the DLQ.')

        try:
            await receiver.dead_letter_message(
                reason='No rules match this message.',
                error_description=f'Message {message_body} could not be processed.',
                message=message
            )
            DLQ_COUNT.inc()
        except Exception as e:
            logger.error(f'Failed to send message to DLQ: {e}')
            await receiver.abandon_message(message)

    async def receive_and_process(self, topic_name, subscription_name):
        """Receive messages, process them, and forward."""
        if not self.source_client:
            logger.error('Source client is not initialized, cannot receive messages.')
            return

        while True:
            try:
                async with await self.get_receiver(topic_name, subscription_name) as receiver:
                    async for message in receiver:
                        await self.process_message(
                            topic_name,
                            message,
                            receiver,
                        )
            except OperationTimeoutError:
                logger.debug(f'Timed out on {topic_name}/{subscription_name}.')
            except Exception as e:
                logger.error(f'Unknown exception {e} on {topic_name}/{subscription_name}.')

    async def run(self):
        """Start all receivers."""
        receive_tasks = []

        for topic, subscription in self.input_topics:
            for i in range(0, self.max_tasks):
                task = asyncio.create_task(self.receive_and_process(topic, subscription))
                receive_tasks.append(task)

        await asyncio.gather(*receive_tasks)

    async def send_message(self, namespaces: list, topics: list, message_body: str, application_properties: dict):
        """
        Send a message to the correct namespace and topic.

        Parameters
        ----------
        namespaces : list[str]
            The namespaces this message should be sent to.
        topics : list[str]
            The topics this message should be sent to.
        message_body : str
            The body of the message to be sent.
        application_properties : dict
            The application properties of the original message.
        """
        for idx, namespace_name in enumerate(namespaces):
            topic_name = topics[idx]
            key = (namespace_name, topic_name)
            sender = await self.get_sender(namespace_name, topic_name)

            async with self.sender_send_locks[key]:
                try:
                    if custom_sender:
                        await custom_sender(sender, topic_name, message_body, application_properties)
                    else:
                        await sender.send_messages(message_body, application_properties=application_properties)
                except Exception as e:
                    logger.error(f'Failed to send message with sender {key}: {e}')
                    raise

    async def start(self):
        """Initialize Service Bus client for receiving and clients for sending."""
        logger.debug('Creating connection for source namespace.')
        self.source_client = ServiceBusClient.from_connection_string(self.source_connection_string)

        for namespace, conn_str in self.namespaces.items():
            logger.debug(f'Creating connection for destination namespace: {namespace}.')
            self.clients[namespace] = ServiceBusClient.from_connection_string(conn_str)


async def main():
    """Configure the handler."""
    handler = ServiceBusHandler(EnvironmentConfigParser())

    try:
        await handler.start()
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def shutdown():
            logger.warning('Shutdown signal received. Cleaning up...')
            stop_event.set()

        loop.add_signal_handler(signal.SIGTERM, shutdown)
        loop.add_signal_handler(signal.SIGINT, shutdown)  # Handle CTRL+C

        await handler.run()  # Start processing messages
        await stop_event.wait()  # Wait for shutdown signal

    finally:
        await handler.close()  # Ensure all connections are properly closed


if __name__ == '__main__':
    logger.info(f'Starting version "{__version__}".')
    config = EnvironmentConfigParser()
    start_http_server(config.get_prometheus_port())
    asyncio.run(main())
