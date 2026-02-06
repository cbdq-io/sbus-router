#!/usr/bin/env python
"""
A configurable router for Azure Service Bus messages.

LICENCE
-------
BSD 3-Clause License

Copyright (c) 2024 - 2026, Cloud Based DQ Ltd.

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
import contextlib
import datetime
import importlib
import inspect
import json
import logging
import os
import re
import signal
import sys
import time
from collections import defaultdict
from string import Template
from typing import Callable, Optional

import jmespath
import jsonschema
import jsonschema.exceptions
from azure.servicebus import (NEXT_AVAILABLE_SESSION, ServiceBusMessage,
                              ServiceBusReceivedMessage)
from azure.servicebus.aio import (AutoLockRenewer, ServiceBusClient,
                                  ServiceBusReceiver, ServiceBusSender)
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.exceptions import (MessageAlreadySettled,
                                         OperationTimeoutError,
                                         ServiceBusConnectionError,
                                         ServiceBusError, SessionLockLostError)
from prometheus_client import Counter, start_http_server

__version__ = '2.0.0'
DLQ_COUNT = Counter('dlq_message_count', 'The number of messages sent to the DLQ.')
IGNORABLE_SETTLEMENT_EXCEPTIONS = (
    MessageAlreadySettled,
    SessionLockLostError,
    OperationTimeoutError
)


def get_logger(logger_name: str, log_level=os.getenv('LOG_LEVEL', 'WARN')) -> logging.Logger:
    """
    Provide a generic logger.

    Parameters
    ----------
    logger_name : str
        The name of the logger.
    log_level : str, optional
        The log level to set the logger to, by default os.getenv('LOG_LEVEL', 'WARN').

    Returns
    -------
    logging.Logger
        A logger that can be used to provide logging.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level=log_level)
    return logger


logging.basicConfig(
    format=os.environ.get(
        'LOG_FORMAT',
        '%(levelname)s [%(filename)s:%(lineno)d] %(message)s'
    )
)
logger = get_logger(__file__)

# -------------------------------------------------------------------------------------------------------------
# Optional: pure, synchronous message transformer
# Configure with ROUTER_CUSTOM_TRANSFORMER="module:function"
# Contract: def transform(msg: ServiceBusMessage, topic_name: str, logger: logging.Logger) -> ServiceBusMessage
# No I/O, no await; either mutate and return the same instance or return a new one.
# -------------------------------------------------------------------------------------------------------------
_transformer: Optional[Callable[[ServiceBusMessage, str], ServiceBusMessage]] = None
_transformer_module = None
_transformer_spec = os.getenv('ROUTER_CUSTOM_TRANSFORMER')
if _transformer_spec:
    try:
        module_path, func_name = _transformer_spec.split(':', 1)
        _transformer_module = importlib.import_module(module_path)
        candidate = getattr(_transformer_module, func_name)
        if inspect.iscoroutinefunction(candidate):
            raise TypeError('Custom transformer must be a synchronous function (no await).')
        _transformer = candidate  # type: ignore[assignment]
        logger.info(f'Configured custom transformer "{_transformer_spec}".')
    except Exception as e:
        logger.error(f'Failed to configure custom transformer "{_transformer_spec}": {e}')
        _transformer = None
        _transformer_module = None


class BatchParseOutcome:
    """
    The outcome from the BatchParser.parse method.

    Attributes
    ----------
    matched_rules_counts : dict[str, int]
        A dictionary of how many rule names have been matched.
    non_routable_messages : list[ServiceBusMessage]
        The messages that don't match any router rules and therefore
        should be dead-lettered.
    routable_messages : dict[str, dict[str, list[ServiceBusMessage]]]
        A dictionary that describes the messages that can be routed.  The
        first str is the destination key, the second level key is the
        name of the destination topic which gives a list of messages
        to be sent to it.
    """

    matched_rules_counts: dict[str, int]
    settlement_plan: list[tuple[str, ServiceBusReceivedMessage]]
    routable_messages: dict[str, dict[str, list[tuple[ServiceBusReceivedMessage, ServiceBusMessage]]]]

    def __init__(self):
        self.settlement_plan = []
        self.matched_rules_counts = {}
        self.routable_messages = {}

    def increment_matched_rule(self, rule_name: str) -> None:
        """Increment the count for the given rule name."""
        if rule_name not in self.matched_rules_counts:
            self.matched_rules_counts[rule_name] = 0

        self.matched_rules_counts[rule_name] += 1


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

        self.session_count = parsed_definition.get('session_count', 0)
        self.jmespath = parsed_definition.get('jmespath', None)

        if self.jmespath:
            self.jmespath_expr = jmespath.compile(self.jmespath)
        else:
            self.jmespath_expr = None

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
                flat_list.extend(self.flatten_list(item))
            else:
                flat_list.append(item)
        return flat_list

    def get_data(self, message_body: str, message_data: Optional[dict]) -> list:
        """
        Extract the relevant data from the message body for rule evaluation.

        If the rule has a JMESPath, it attempts to extract relevant data.

        Parameters
        ----------
        message_body : str
            The message body as a string.
        message_data : dict | None
            If the message was JSON, this is a data representation of the message (parsed JSON).

        Returns
        -------
        list
            The extracted data for comparison.
        """
        if not self.jmespath:
            return [message_body]

        if message_data is None:
            return []

        result = self.jmespath_expr.search(message_data)

        if isinstance(result, list):
            return self.flatten_list(result)
        if result is None:
            return []

        return [result]

    def is_data_match(self, message_body: str, message_data: Optional[dict]) -> bool:
        """
        Check if the message content matches a regular expression.

        Parameters
        ----------
        message_body : str
            The message body to be checked.
        message_data : dict | None
            If the message was JSON, this is a data representation of the message (parsed JSON).

        Returns
        -------
        bool
            True if the message matches, otherwise False.
        """
        if not self.prog:
            return True

        data = self.get_data(message_body, message_data)
        return any(self.prog.search(element) for element in data) if data else False

    def is_match(self, source_topic_name: str, message_body: str, message_data: Optional[dict]) -> tuple:
        """
        Check if the provided message and source topics match this rule.

        Parameters
        ----------
        source_topic_name : str
            The name of the topic that this message was consumed from.
        message_body : str
            The message body as a string.
        message_data : dict | None
            If the message was JSON, this is a data representation of the message (parsed JSON).

        Returns
        -------
        tuple[bool, list, list]
            (is_match, destination_namespaces, destination_topics)
        """
        if source_topic_name == self.source_topic and self.is_data_match(message_body, message_data):
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


class BatchParser:
    """
    Parse batches of messages to destinations.

    Attributes
    ----------
    rules : list[RouterRules]
        The rules provided by the constructor.

    Parameters
    ----------
    rules : list[RouterRules]
        The rules to check the messages against.
    """

    def __init__(self, rules: list[RouterRule]):
        self.rules = rules
        self.router_timestamp_app_property_name = os.environ.get('ROUTER_TIMESTAMP_APP_PROPERTY_NAME')

    def get_message_body(self, message: ServiceBusReceivedMessage) -> str:
        """Get the message body from a received message as a string."""
        body_type = message.body_type

        if body_type == AmqpMessageBodyType.DATA:
            return b''.join(message.body).decode()

        if body_type == AmqpMessageBodyType.SEQUENCE:
            return json.dumps(message.body)

        if body_type == AmqpMessageBodyType.VALUE:
            return str(message.body)

        raise TypeError(f'Unsupported message body type: {body_type}')

    def get_message_body_data(self, body: str) -> str:
        """Parse the body as a JSON string."""
        try:
            return json.loads(body)
        except json.decoder.JSONDecodeError:
            return None

    def output_message(self, input_message: ServiceBusReceivedMessage, body: str, dest_topic: str) -> ServiceBusMessage:
        """Clone/enrich a message for sending from a received message."""
        application_properties = dict(input_message.application_properties or {})

        if self.router_timestamp_app_property_name:
            application_properties[self.router_timestamp_app_property_name] = datetime.datetime.now(datetime.UTC) \
                .isoformat(timespec='milliseconds') \
                .replace('+00:00', 'Z')

        application_properties['__src_enqueued_time_utc'] = input_message \
            .enqueued_time_utc.isoformat(timespec='milliseconds') \
            .replace('+00:00', 'Z')
        output_message = ServiceBusMessage(
            body=body,
            application_properties=application_properties,
            session_id=input_message.session_id
        )

        if _transformer:
            try:
                out = _transformer(output_message, dest_topic, logger)

                if isinstance(out, ServiceBusMessage):
                    return out
            except Exception as e:
                logger.error(f'Custom transformer raised an exception: {e}')

        return output_message

    def parse(self, source_topic_name: str, input_messages: list[ServiceBusReceivedMessage]) -> BatchParseOutcome:
        """
        Parse input messages and populate the BatchParseOutcome.

        Parameters
        ----------
        source_topic_name : str
            The name of the source topic.
        input_messages : list[ServiceBusReceivedMessage]
            The messages to be parsed.

        Returns
        -------
        BatchParseOutcome
            The routable and non-routable messages.
        """
        response = BatchParseOutcome()

        for message in input_messages:
            data = None
            body = self.get_message_body(message)
            matched = False

            for rule in self.rules:
                if rule.jmespath and data is None:
                    data = self.get_message_body_data(body)

                is_match, dest_namespaces, dest_topics = rule.is_match(source_topic_name, body, data)

                if is_match:
                    response.increment_matched_rule(rule.name())

                    for dest_namespace in dest_namespaces:
                        if dest_namespace not in response.routable_messages:
                            response.routable_messages[dest_namespace] = {}

                        for dest_topic in dest_topics:
                            if dest_topic not in response.routable_messages[dest_namespace]:
                                response.routable_messages[dest_namespace][dest_topic] = []

                            response.routable_messages[dest_namespace][dest_topic].append(
                                (
                                    message,
                                    self.output_message(message, body, dest_topic)
                                )
                            )

                    response.settlement_plan.append(('complete', message))
                    matched = True
                    break

            if not matched:
                logger.warning(f'No rules match message from {source_topic_name}, sending to the DLQ.')
                response.settlement_plan.append(('deadletter', message))
                DLQ_COUNT.inc()

        return response


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
            The name of the namespace (e.g. 'gbdev').
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
            The count of service bus namespaces instances that have been defined in the config.
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

    def get_prefetch_count(self) -> int:
        """
        Get the number of messages to be prefetched by the client.

        Returns
        -------
        int
            The number of messages to be prefetched. If not provided, default is 100.
        """
        return int(self._environ.get('ROUTER_PREFETCH_COUNT', '100'))

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
            A list of list items, where each item contains two string elements representing the key and the value.
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

    def get_ts_app_prop_name(self) -> str:
        """Return the name of the specified timestamp application property."""
        return self._environ.get('ROUTER_TIMESTAMP_APP_PROPERTY_NAME')

    def init_rules_usage(self) -> dict:
        """
        Initialise a dictionary representing the usage of all defined rules.

        All values are set to zero.

        Returns
        -------
        dict[str, Counter]
            Each rule name will be a key to a value that is a Prometheus Counter.
        """
        response = {}
        for rule in self.get_rules():
            response[rule.name()] = Counter(
                f'{rule.name()}_usage_count',
                f'How often the {rule.name()} rule has been matched.'
            )
        return response

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
        Extract a list of the source topics and subscriptions.

        Returns
        -------
        list[tuple]
            A list of tuples (topic_name, subscription_name, session_count).
        """
        response = []
        rules = self.get_rules()

        for rule in rules:
            instance = (rule.source_topic, rule.source_subscription, rule.session_count)

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
        self.config = config
        self.namespaces = config.service_bus_namespaces().get_all_namespaces()  # For sending
        self.input_topics = config.topics_and_subscriptions()
        self.rules = config.get_rules()
        self.ts_app_prop_name = config.get_ts_app_prop_name()

        for idx, rule in enumerate(self.rules):
            logger.info(f'Rule parsing order {idx} {rule.name()}')

        self.source_client = None  # Used for receiving
        self.clients = {}          # Used for sending (per destination namespace)
        self.senders = {}          # Cache of senders keyed by (namespace, topic)
        self.sender_create_locks = defaultdict(asyncio.Lock)
        self.sender_use_locks = defaultdict(asyncio.Lock)
        self.rules_by_topic = defaultdict(list)
        self.rules_usage = config.init_rules_usage()
        self.keep_alive_tasks = []
        self.shutdown_event = asyncio.Event()

        for rule in self.rules:
            self.rules_by_topic[rule.source_topic].append(rule)

    async def _close_many(self, items):
        if not items:
            return
        await asyncio.gather(*(item.close() for item in items), return_exceptions=True)

    async def close(self) -> None:
        """Gracefully close all batchers, clients and senders."""
        logger.warning('Closing all connections on shutdown.')

        # Close senders and destination clients
        await self._close_many(self.senders.values())
        await self._close_many(self.clients.values())

        # Stop keep-alive tasks
        self.shutdown_event.set()
        for task in self.keep_alive_tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # Close the source client
        if self.source_client:
            await self.source_client.close()

    async def drop_sender(self, namespace: str, topic: str):
        """Evict a broken sender."""
        key = (namespace, topic)
        sender = self.senders.pop(key, None)
        if sender:
            with contextlib.suppress(Exception):
                await sender.close()

    async def get_receiver(self, topic_name: str, subscription_name: str,
                           is_session_enabled: bool) -> ServiceBusReceiver:
        """Get a receiver for a topic/subscription."""
        if is_session_enabled:
            receiver = self.source_client.get_subscription_receiver(
                topic_name=topic_name,
                subscription_name=subscription_name,
                session_id=NEXT_AVAILABLE_SESSION,
                max_wait_time=1,
                prefetch_count=0
            )
            sid = getattr(receiver.session, 'session_id', 'unknown')
            logger.debug(f'Created a receiver for {topic_name}/{subscription_name} ({sid})')
        else:
            logger.debug(f'Creating a non-sessioned receiver for {topic_name}/{subscription_name}...')
            receiver = self.source_client.get_subscription_receiver(
                topic_name=topic_name,
                subscription_name=subscription_name,
                prefetch_count=self.config.get_prefetch_count(),
                max_wait_time=1
            )

        return receiver

    async def get_sender(self, namespace: str, topic: str) -> ServiceBusSender:
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

        async with self.sender_create_locks[key]:
            sender = self.senders.get(key)

            if sender is not None:
                return sender

            client = self.clients.get(namespace)

            if not client:
                raise ValueError(f'Namespace "{namespace}" not found in configuration.')

            sender = client.get_topic_sender(topic)
            await sender.__aenter__()
            self.senders[key] = sender
            return sender

    async def process_messages(self, source_topic: str, messages: list[ServiceBusReceivedMessage],
                               receiver: ServiceBusReceiver) -> None:
        """Process received messages."""
        batch_parser = BatchParser(self.rules_by_topic[source_topic])
        parsed = batch_parser.parse(source_topic, messages)

        for namespace, topics in parsed.routable_messages.items():
            for topic, pairs in topics.items():

                if not pairs:
                    continue

                sender = await self.get_sender(namespace, topic)
                messages = []

                for _, message in pairs:
                    messages.append(message)

                if not messages:
                    continue

                try:
                    key = (namespace, topic)

                    async with self.sender_use_locks[key]:
                        await self.send_messages(sender, messages)
                except (ServiceBusConnectionError, ServiceBusError):
                    await self.drop_sender(namespace, topic)
                    raise

        await self.settle_outcome(receiver, parsed)

    async def receive_and_process(self, topic_name, subscription_name, session_enabled: bool = False):
        """Receive messages, process them, and forward."""
        if not self.source_client:
            logger.error('Source client is not initialized')
            return

        max_message_count = int(os.environ.get('ROUTER_BATCH_MAX_MESSAGES', '100'))
        max_wait_time = 5

        if session_enabled:
            max_message_count = int(os.environ.get('ROUTER_SESSION_MAX_MESSAGES', '25'))
            max_message_count = max(max_message_count, 25)
            max_wait_time = 1

        while not self.shutdown_event.is_set():
            try:
                async with await self.get_receiver(
                    topic_name, subscription_name, session_enabled
                ) as receiver:
                    renewer = None

                    try:
                        if session_enabled and receiver.session:
                            renewer = AutoLockRenewer(max_lock_renewal_duration=300)
                            renewer.register(receiver, receiver.session)

                        while not self.shutdown_event.is_set():
                            try:
                                messages = await receiver.receive_messages(
                                    max_message_count=max_message_count,
                                    max_wait_time=max_wait_time
                                )
                            except OperationTimeoutError:
                                continue

                            if not messages:
                                continue

                            await self.process_messages(topic_name, messages, receiver)

                    finally:
                        if renewer:
                            await renewer.close()

            except OperationTimeoutError:
                logger.debug(
                    f'Receiver timeout on {topic_name}/{subscription_name}, recreating receiver'
                )
                continue

            except SessionLockLostError:
                logger.info(
                    f'Session lock lost on {topic_name}/{subscription_name}, reacquiring session'
                )
                continue

            except (ServiceBusError, ServiceBusConnectionError) as e:
                logger.info(
                    f'Service Bus connection dropped on {topic_name}/{subscription_name}, reconnecting: {e}'
                )
                await asyncio.sleep(2)
                continue

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    f'Unexpected receiver error on {topic_name}/{subscription_name}'
                )
                await asyncio.sleep(5)

    async def run(self):
        """Start all receivers."""
        receive_tasks = []
        await self.wait_for_amqp_ready()

        for topic, subscription, session_count in self.input_topics:
            session_enabled = session_count > 0

            if not session_enabled:
                task = asyncio.create_task(self.receive_and_process(topic, subscription))
                receive_tasks.append(task)
            else:
                for _ in range(0, session_count):
                    task = asyncio.create_task(self.receive_and_process(topic, subscription, session_enabled))
                    receive_tasks.append(task)

        await asyncio.gather(*receive_tasks)

    async def send_messages(self, sender: ServiceBusSender, messages: list[ServiceBusMessage]):
        """Send a list of messages to the provided sender."""
        if not messages:
                return

        batch = await sender.create_message_batch()

        for message in messages:
            try:
                batch.add_message(message)
                continue
            except ValueError:
                # Current batch is full OR message too large
                pass

            # Flush current batch if it has data
            if len(batch) > 0:
                await sender.send_messages(batch)

            # Start a new batch
            batch = await sender.create_message_batch()

            try:
                batch.add_message(message)
            except ValueError:
                # Message is too large to fit even in an empty batch
                # Send it individually (this is required by the SDK contract)
                await sender.send_messages(message)

        # Final flush (only if batch has data)
        if len(batch) > 0:
            await sender.send_messages(batch)

    async def settle_outcome(self, receiver: ServiceBusReceiver, outcome: BatchParseOutcome):
        """Complete routed messages and dead-letter non-routed."""
        for action, msg in outcome.settlement_plan:
            try:
                if action == 'deadletter':
                    await receiver.dead_letter_message(
                        msg,
                        reason='NoRuleMatch',
                        error_description='No rules match this message. Please check the message body.'
                    )
                else:
                    await receiver.complete_message(msg)
            except IGNORABLE_SETTLEMENT_EXCEPTIONS:
                continue
            except ServiceBusConnectionError:
                raise
            except ServiceBusError:
                logger.exception(f'Fatal error during settlement action={action}.')
                raise

    async def start(self):
        """Initialize Service Bus client for receiving and clients for sending."""
        logger.debug('Creating connection for source namespace.')
        self.source_client = ServiceBusClient.from_connection_string(self.source_connection_string)

        for namespace, conn_str in self.namespaces.items():
            logger.debug(f'Creating connection for destination namespace: {namespace}.')
            self.clients[namespace] = ServiceBusClient.from_connection_string(conn_str)

    async def wait_for_amqp_ready(self, timeout=30):
        """Test that AMQP/ServiceBus is actually ready."""
        start = time.time()
        topic_name, subscription_name, _ = self.input_topics[0]

        while True:
            if time.time() - start > timeout:
                logger.error('AMQP did not become ready within timeout; continuing anyway.')
                return

            try:
                async with self.source_client.get_subscription_receiver(
                    topic_name=topic_name,
                    subscription_name=subscription_name,
                    sub_queue='deadletter',
                    prefetch_count=1
                ) as receiver:
                    await receiver.peek_messages(max_message_count=1)

                logger.info('AMQP management link is ready.')
                return

            except Exception as e:
                logger.warning(f'Waiting for AMQP readiness: {e}')
                await asyncio.sleep(1)


async def main(config: EnvironmentConfigParser):
    """Configure and run the Service Bus handler with graceful shutdown."""
    handler = ServiceBusHandler(config)
    await handler.start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown():
        logger.warning('Shutdown signal received...')
        handler.shutdown_event.set()
        stop_event.set()

    loop.add_signal_handler(signal.SIGTERM, shutdown)
    loop.add_signal_handler(signal.SIGINT, shutdown)

    run_task = asyncio.create_task(handler.run())

    try:
        await stop_event.wait()
    finally:
        logger.warning('Waiting for router task to finish...')
        run_task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await run_task

        await handler.close()
        logger.warning('Shutdown complete.')


if __name__ == '__main__':
    logger.info(f'Starting version "{__version__}".')
    config = EnvironmentConfigParser()
    start_http_server(config.get_prometheus_port())
    asyncio.run(main(config))
