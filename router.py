#!/usr/bin/env python
"""
A configurable router for Azure Service Bus messages.

LICENCE
-------
BSD 3-Clause License

Copyright (c) 2024,2025, Cloud Based DQ Ltd.

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
from logging import Logger
from string import Template
from typing import Callable, Optional

import jmespath
import jsonschema
import jsonschema.exceptions
from azure.servicebus import ServiceBusMessage, ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.exceptions import OperationTimeoutError
from prometheus_client import Counter, Summary, start_http_server

__version__ = '1.0.0'
PROCESSING_TIME = Summary('message_processing_seconds', 'The time spent processing messages.')
DLQ_COUNT = Counter('dlq_message_count', 'The number of messages sent to the DLQ.')


def get_logger(logger_name: str, log_level=os.getenv('LOG_LEVEL', 'WARN')) -> Logger:
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
    Logger
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

# --------------------------------------------------------------------------------------
# Optional: pure, synchronous message transformer
# Configure with ROUTER_CUSTOM_TRANSFORMER="module:function"
# Contract: def transform(
#     msg: ServiceBusMessage,
#     topic_name: str,
#     logger: Logger
# ) -> ServiceBusMessage
# No I/O, no await; either mutate and return the same instance or return a new one.
# --------------------------------------------------------------------------------------
_transformer: Optional[
    Callable[[ServiceBusMessage, str, Logger], ServiceBusMessage]
] = None
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
        return b''.join(message.body).decode()

    if body_type == AmqpMessageBodyType.SEQUENCE:
        return json.dumps(message.body)

    if body_type == AmqpMessageBodyType.VALUE:
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
    """

    def __init__(self, name: str, definition: str, max_tasks: int) -> None:
        self.definition = definition
        self.name(name)
        parsed_definition = self.parse_definition(definition)
        self.parsed_definition = parsed_definition

        if parsed_definition['destination_namespaces']:
            self.destination_namespaces = parsed_definition['destination_namespaces'].split(',')
        else:
            self.destination_namespaces = []

        if parsed_definition['destination_topics']:
            self.destination_topics = parsed_definition['destination_topics'].split(',')
        else:
            self.destination_topics = []

        self.jmespath = parsed_definition.get('jmespath', None)

        if self.jmespath:
            self.jmespath_expr = jmespath.compile(self.jmespath)
        else:
            self.jmespath_expr = None

        self.session_id_list = parsed_definition.get('session_id_list')

        if self.session_id_list is not None:
            # Schema guarantees array[str], but normalize defensively
            self.session_id_list = [str(s) for s in self.session_id_list]

        if self.session_id_list is not None:
            # Session rules do NOT use max_tasks
            self.max_tasks = None
        else:
            self.max_tasks = int(parsed_definition['max_tasks'])

        self.regexp = parsed_definition.get('regexp', None)

        if self.regexp:
            self.prog = re.compile(self.regexp)
        else:
            self.prog = None

        self.source_subscription = parsed_definition['source_subscription']
        self.source_topic = parsed_definition['source_topic']

    @property
    def is_session_rule(self) -> bool:
        """Is this a rule for a session enabled source."""
        return self.session_id_list is not None

    @property
    def is_non_session_rule(self) -> bool:
        """Is this a rule for a non-session enabled source."""
        return self.session_id_list is None

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

    def get_disable_lock_renewal(self) -> bool:
        """Check if lock renewal is to be disabled."""
        return self._environ.get('ROUTER_DISABLE_LOCK_RENEWAL', '0').lower() in ('1', 'true', 'yes')

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
            response.append(RouterRule(name, definition, self.max_tasks()))

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

    def topics_and_subscriptions(self) -> list:
        """
        Extract a list of the source topics and subscriptions.

        Returns
        -------
        list[tuple]
            A list of tuples (topic_name, subscription_name).
        """
        response = []
        rules = self.get_rules()

        for rule in rules:
            instance = (rule.source_topic, rule.source_subscription, rule.max_tasks)

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

    # -------------------------
    # Batching configuration
    # -------------------------
    _BATCH_MAX_WAIT_MS = int(os.getenv('ROUTER_BATCH_MAX_WAIT_MS', '10'))
    _BATCH_MAX_MESSAGES = int(os.getenv('ROUTER_BATCH_MAX_MESSAGES', '256'))

    class _Batcher:
        """
        Per-destination coalescing buffer with per-item acknowledgements.

        Queues tuples of (ServiceBusMessage, Future) and flushes them as one or
        more Service Bus batches once either:
          - max_wait has elapsed, or
          - max_messages have been collected.

        The Future resolves only after the send for the batch that contains
        that message completes (success or exception).
        """

        def __init__(self, handler: 'ServiceBusHandler', namespace: str, sender_factory: Callable,
                     max_wait_ms: int, max_msgs: int, topic_name: str):
            self.handler = handler
            self.namespace = namespace
            self.sender_factory = sender_factory
            self.queue = asyncio.Queue()
            self.max_wait = max_wait_ms / 1000.0
            self.max_msgs = max_msgs
            self.topic_name = topic_name
            self._task = asyncio.create_task(self._run())

        async def add_and_wait(self, msg: ServiceBusMessage) -> None:
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            await self.queue.put((msg, fut))
            await fut

        async def close(self) -> None:
            await self.queue.put(None)
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

        async def _run(self):
            while True:
                items = await self._coalesce()
                if items is None:
                    break

                # Always (re)acquire a valid sender
                self._sender = await self.sender_factory()
                await self._flush(items)

        async def _coalesce(self):
            first = await self.queue.get()
            if first is None:
                return None

            items = [first]
            deadline = asyncio.get_running_loop().time() + self.max_wait

            while len(items) < self.max_msgs:
                timeout = deadline - asyncio.get_running_loop().time()
                if timeout <= 0:
                    break
                try:
                    nxt = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    break
                if nxt is None:
                    # re-queue sentinel for the outer loop to exit next round
                    self.queue.put_nowait(None)
                    break
                items.append(nxt)

            return items

        async def _flush(self, items):
            if not items:
                return

            batch = await self._sender.create_message_batch()
            pending: list[tuple[ServiceBusMessage, asyncio.Future]] = []

            async def flush_batch():
                if not pending:
                    return
                try:
                    await self._sender.send_messages(batch)
                    for _, fut in pending:
                        if not fut.done():
                            fut.set_result(True)
                except Exception as e:
                    await self.handler._invalidate_sender(self.namespace, self.topic_name)

                    for _, fut in pending:
                        if not fut.done():
                            fut.set_exception(e)

            for msg, fut in items:
                try:
                    batch.add_message(msg)
                    pending.append((msg, fut))
                except ValueError:
                    # current batch full -> send and start a new one
                    await flush_batch()
                    batch = await self._sender.create_message_batch()
                    batch.add_message(msg)
                    pending = [(msg, fut)]

            await flush_batch()

    def __init__(self, config: EnvironmentConfigParser):
        self.source_connection_string = config.get_source_connection_string()
        self.config = config

        # Destination namespaces (for sending)
        self.namespaces = config.service_bus_namespaces().get_all_namespaces()

        # Load rules FIRST (required for session detection)
        self.rules = config.get_rules()
        self.session_inputs = []      # (topic, subscription, session_id)
        self.nonsession_inputs = []  # (topic, subscription, max_tasks)

        for rule in self.rules:
            if rule.is_session_rule:
                for session_id in rule.session_id_list:
                    self.session_inputs.append(
                        (rule.source_topic, rule.source_subscription, session_id)
                    )
            else:
                self.nonsession_inputs.append(
                    (rule.source_topic, rule.source_subscription, rule.max_tasks)
                )

        logger.info(
            f'Configured {len(self.session_inputs)} session subscriptions '
            f'and {len(self.nonsession_inputs)} non-session subscriptions.'
        )

        self.input_topics = []

        for topic, subscription, _ in self.nonsession_inputs:
            key = (topic, subscription)
            if key not in self.input_topics:
                self.input_topics.append(key)

        for topic, subscription, _ in self.session_inputs:
            key = (topic, subscription)
            if key not in self.input_topics:
                self.input_topics.append(key)

        self.max_tasks = config.max_tasks()
        self.ts_app_prop_name = config.get_ts_app_prop_name()
        self.disable_lock_renewal = config.get_disable_lock_renewal()

        for idx, rule in enumerate(self.rules):
            logger.info(f'Rule parsing order {idx} {rule.name()}')

        # Service Bus clients and senders
        self.source_client = None
        self.clients = {}
        self.senders = {}

        # Batching
        self.batchers: dict[tuple[str, str], ServiceBusHandler._Batcher] = {}
        self.sender_locks = defaultdict(asyncio.Lock)

        # Rules indexed by topic
        self.rules_by_topic = defaultdict(list)
        for rule in self.rules:
            self.rules_by_topic[rule.source_topic].append(rule)

        # Metrics and lifecycle
        self.rules_usage = config.init_rules_usage()
        self.keep_alive_tasks = []
        self.shutdown_event = asyncio.Event()
        self.dlq_last_warned = defaultdict(lambda: 0)

    async def _close_many(self, items):
        if not items:
            return
        await asyncio.gather(*(item.close() for item in items), return_exceptions=True)

    async def close(self) -> None:
        """Gracefully close all batchers, clients and senders."""
        logger.warning('Closing all connections on shutdown.')

        await self._close_many(getattr(self, 'batchers', {}).values())
        for sender in self.senders.values():
            with contextlib.suppress(Exception):
                await sender.__aexit__(None, None, None)

        # exit destination clients
        for client in self.clients.values():
            with contextlib.suppress(Exception):
                await client.__aexit__(None, None, None)

        # exit source client
        if self.source_client:
            with contextlib.suppress(Exception):
                await self.source_client.__aexit__(None, None, None)

        self.shutdown_event.set()

        for task in self.keep_alive_tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def get_non_session_receiver(self, topic_name: str, subscription_name: str) -> ServiceBusReceiver:
        """Get a receiver for a topic/subscription."""
        if not self.source_client:
            raise RuntimeError('Source ServiceBusClient not initialized')

        logger.debug(f'Creating a non-sessioned receiver for {topic_name}/{subscription_name}...')
        return self.source_client.get_subscription_receiver(
            topic_name=topic_name,
            subscription_name=subscription_name,
            prefetch_count=self.config.get_prefetch_count(),
            max_wait_time=1
        )

    async def get_session_receiver(
        self,
        topic_name: str,
        subscription_name: str,
        session_id: str,
    ) -> ServiceBusReceiver:
        """Get a receiver for a topic/subscription/session."""
        if not self.source_client:
            raise RuntimeError('Source ServiceBusClient not initialized')

        logger.debug(
            f'Created session receiver for '
            f'{topic_name}/{subscription_name} (session_id={session_id})'
        )

        return self.source_client.get_subscription_receiver(
            topic_name=topic_name,
            subscription_name=subscription_name,
            session_id=session_id,
            max_wait_time=5,
            prefetch_count=0,
        )

    async def _invalidate_sender(self, namespace: str, topic: str):
        """
        Invalidate a broken sender.

        IMPORTANT: Senders are disposable and ServiceBusClient MUST remain alive
        """
        key = (namespace, topic)

        async with self.sender_locks[key]:
            sender = self.senders.pop(key, None)
            if sender:
                with contextlib.suppress(Exception):
                    await sender.__aexit__(None, None, None)
                logger.warning(f'Invalidated sender for {namespace}/{topic}')

    async def get_sender(self, namespace: str, topic: str):
        """Retrieve or create a sender for the given namespace and topic."""
        key = (namespace, topic)

        async with self.sender_locks[key]:
            sender = self.senders.get(key)
            if sender:
                # If the underlying handler is missing/dead, recreate the sender
                if getattr(sender, '_handler', None) is None or getattr(sender, '_closed', False):
                    await self._invalidate_sender(namespace, topic)
                else:
                    return sender

            client = self.clients.get(namespace)
            if not client:
                raise ValueError(f'Namespace "{namespace}" not found in configuration.')

            sender = client.get_topic_sender(topic)
            await sender.__aenter__()
            await asyncio.sleep(0)
            self.senders[key] = sender
            return sender

    def _get_transformer(self) -> Optional[Callable[[ServiceBusMessage, str, Logger], ServiceBusMessage]]:
        """Return the configured synchronous transformer, if any."""
        return _transformer

    async def keep_source_connection_alive(self, interval=240):
        """Ping DLQ via peek to check for presence of dead-letter messages."""
        while not self.shutdown_event.is_set():
            for topic_name, subscription_name in self.input_topics:
                logger.debug(f'Checking DLQ for {topic_name}/{subscription_name}')

                try:
                    async with self.source_client.get_subscription_receiver(
                        topic_name=topic_name,
                        subscription_name=subscription_name,
                        sub_queue='deadletter',
                        prefetch_count=1
                    ) as receiver:
                        msgs = await receiver.peek_messages(max_message_count=1)
                        current_time = time.time()
                        key = (topic_name, subscription_name)

                        if msgs:
                            last_warned = self.dlq_last_warned[key]

                            if current_time - last_warned >= 3600:
                                logger.warning(f'DLQ has messages for {topic_name}/{subscription_name}')
                                self.dlq_last_warned[key] = current_time
                except Exception as e:
                    logger.error(f'Error checking DLQ for {topic_name}/{subscription_name}: {e}')

            await asyncio.sleep(interval)

    @staticmethod
    def _maybe_parse_json_for_topic(rules_for_topic: list, body: str) -> Optional[dict]:
        """Parse a JSON message once per topic if any rule needs JSON."""
        needs_json = any(r.jmespath for r in rules_for_topic)
        if not needs_json:
            return None
        try:
            return json.loads(body)
        except json.decoder.JSONDecodeError:
            return None

    @PROCESSING_TIME.time()
    async def process_message(self, source_topic: str, message: ServiceBusReceivedMessage,
                              receiver: ServiceBusReceiver):
        """
        Process the received message asynchronously.

        Parameters
        ----------
        source_topic : str
            The name of the topic where the message was received from.
        message : ServiceBusReceivedMessage
            The message to be processed.
        receiver : ServiceBusReceiver
            The receiver that the message came in on.
        """
        renew_task = None
        message_body = await extract_message_body(message)
        rules_for_topic = self.rules_by_topic.get(source_topic, [])
        message_data = self._maybe_parse_json_for_topic(rules_for_topic, message_body)

        if receiver.session is None and not self.disable_lock_renewal:
            renew_task = asyncio.create_task(self._renew_message_lock(receiver, message))

        for rule in rules_for_topic:
            is_match, destination_namespaces, destination_topics = rule.is_match(
                source_topic,
                message_body,
                message_data
            )

            if is_match:
                try:
                    logger.debug(f'Successfully matched message to {rule.name()}.')
                    self.rules_usage[rule.name()].inc()

                    if receiver.session is not None:
                        # SESSION MODE: no batching, send immediately
                        await self.send_message_unbatched(
                            destination_namespaces,
                            destination_topics,
                            message_body,
                            message.application_properties,
                            message.enqueued_time_utc,
                            session_id=message.session_id,
                        )
                    else:
                        # NON-SESSION MODE: batching allowed
                        await self.send_message(
                            destination_namespaces,
                            destination_topics,
                            message_body,
                            message.application_properties,
                            message.enqueued_time_utc,
                            session_id=message.session_id,
                        )

                    await self.safe_complete(receiver, message)
                    return
                except Exception as e:
                    logger.error(f'Failed to send message ({",".join(destination_namespaces)}): {e}')
                    await self.safe_abandon(receiver, message)
                    return
                finally:
                    if renew_task:
                        renew_task.cancel()

        # No matching rule: Send to DLQ
        logger.warning(f'No rules match message from {source_topic}, sending to the DLQ.')

        try:
            await receiver.dead_letter_message(
                reason='No rules match this message.',
                error_description='No rules match this message. Please check the message body.',
                message=message
            )
            DLQ_COUNT.inc()
        except Exception as e:
            logger.error(f'Failed to send message to DLQ: {e}')
            await self.safe_abandon(receiver, message)
        finally:
            if renew_task:
                renew_task.cancel()

    async def _receive_loop(self, topic_name, subscription_name, receiver):
        """Receive in small batches instead of relying on the async iterator."""
        while not self.shutdown_event.is_set():
            messages = await receiver.receive_messages(
                max_message_count=int(os.getenv('MAX_RECEIVER_MESSAGE_COUNT', '50')),
                max_wait_time=int(os.getenv('MAX_RECEIVER_MESSAGE_WAIT_TIME', '1'))
            )

            if not messages:
                continue

            for message in messages:
                await self.process_message(topic_name, message, receiver)

    async def receive_and_process(self, topic_name, subscription_name):
        """Receive messages, process them, and forward ONE session or non-session run."""
        if not self.source_client:
            logger.error('Source client is not initialized, cannot receive messages.')
            return

        renew_task = None
        try:
            async with await self.get_non_session_receiver(topic_name, subscription_name) as receiver:
                await self._receive_loop(topic_name, subscription_name, receiver)

        except OperationTimeoutError:
            logger.debug(f'Timed out on {topic_name}/{subscription_name}.')
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f'Unknown exception {e} on {topic_name}/{subscription_name}.')
        finally:
            if renew_task:
                renew_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await renew_task

    async def receive_and_process_session(self, topic_name, subscription_name, session_id):
        """Receive a process messages from a session enabled receiver."""
        if not self.source_client:
            logger.error('Source client is not initialized, cannot receive messages.')
            return

        renew_task = None
        try:
            async with await self.get_session_receiver(topic_name, subscription_name, session_id) as receiver:
                if not self.disable_lock_renewal:
                    renew_task = asyncio.create_task(self._renew_session_lock(receiver))

                await self._receive_loop(topic_name, subscription_name, receiver)

        except OperationTimeoutError:
            logger.debug(f'Timed out on {topic_name}/{subscription_name} (session_id={session_id}).')
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f'Unknown exception {e} on {topic_name}/{subscription_name} (session_id={session_id}).')
        finally:
            if renew_task:
                renew_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await renew_task

    async def _run_sessions(self):
        """Continuously run receivers for session-enabled subscriptions."""
        while not self.shutdown_event.is_set():
            tasks = []

            for topic, subscription, max_tasks in self.session_inputs:
                for _ in range(max_tasks):
                    tasks.append(
                        asyncio.create_task(
                            self.receive_and_process(topic, subscription)
                        )
                    )

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _run_nonsessions(self):
        """Control receivers for non-session subscriptions."""
        while not self.shutdown_event.is_set():
            tasks = []

            for topic, subscription, max_tasks in self.nonsession_inputs:
                for _ in range(max_tasks):
                    tasks.append(
                        asyncio.create_task(
                            self.receive_and_process(topic, subscription)
                        )
                    )

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED
            )

            for t in pending:
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await t

    async def run(self):
        """Run session and non-session receivers correctly."""
        await self.wait_for_amqp_ready()

        runners = []

        if self.session_inputs:
            runners.append(asyncio.create_task(self._run_sessions()))

        if self.nonsession_inputs:
            runners.append(asyncio.create_task(self._run_nonsessions()))

        await asyncio.gather(*runners)

    async def _renew_message_lock(self, receiver: ServiceBusReceiver, message: ServiceBusReceivedMessage):
        """Renew message locks for non-sessioned receivers."""
        while not self.shutdown_event.is_set():
            try:
                await receiver.renew_message_lock(message)
            except Exception as e:
                logger.error(f'Message lock renewal failed: {e}')
                return
            await asyncio.sleep(10)

    async def _renew_session_lock(self, receiver: ServiceBusReceiver):
        """Renew session locks."""
        while not self.shutdown_event.is_set():
            try:
                await receiver.session.renew_lock()
            except Exception as e:
                message = f'Session lock renewal failed on {receiver.session.session_id}'
                logger.error(f'{message}: {e}')
                return
            await asyncio.sleep(10)

    def _get_batcher(self, namespace: str, topic: str) -> 'ServiceBusHandler._Batcher':
        """Get a batcher."""
        key = (namespace, topic)

        if key not in self.batchers:
            async def factory():
                return await self.get_sender(namespace, topic)

            self.batchers[key] = ServiceBusHandler._Batcher(
                handler=self,
                namespace=namespace,
                sender_factory=factory,
                max_wait_ms=self._BATCH_MAX_WAIT_MS,
                max_msgs=self._BATCH_MAX_MESSAGES,
                topic_name=topic,
            )
        return self.batchers[key]

    def _build_message(self, body: str, application_properties: dict, topic_name: str, session_id: Optional[str]):
        """
        Construct a ServiceBusMessage and apply the optional synchronous transformer.

        Parameters
        ----------
        body : str
            The body of the new message.
        application_properties | dict
            The headers for the message.
        topic_name : str
            The topic to which the message is to be sent.

        Returns
        -------
        ServiceBusMessage
            The message to be sent.
        """
        if self.ts_app_prop_name:
            ts = datetime.datetime.now(datetime.UTC).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            logger.debug(f'Setting application property "{self.ts_app_prop_name}" to "{ts}".')
            application_properties[self.ts_app_prop_name] = ts

        msg = ServiceBusMessage(body=body, application_properties=application_properties, session_id=session_id)
        transformer = self._get_transformer()

        if transformer:
            try:
                out = transformer(msg, topic_name, logger)
                if isinstance(out, ServiceBusMessage):
                    return out
            except Exception as e:
                logger.error(f'Custom transformer raised an exception: {e}')

        return msg

    async def safe_abandon(self, receiver: ServiceBusReceiver, message: ServiceBusMessage) -> bool:
        """Catch and retry when attempting to abandon messages."""
        for attempt in range(3):
            try:
                await receiver.abandon_message(message)
                return True
            except Exception as e:
                logger.error(f'Abandon failed (attempt {attempt}): {e}')
                await asyncio.sleep(0.25)

        return False

    async def safe_complete(self, receiver: ServiceBusReceiver, message: ServiceBusMessage) -> bool:
        """Catch and retry when attempting to complete messages."""
        attempts = 3

        for attempt in range(attempts):
            try:
                await receiver.complete_message(message)
                return True
            except Exception as e:
                logger.info(f'Complete failed (attempt {attempt + 1}): {e}')
                await asyncio.sleep(0.25)

        logger.error(f'COMPLETE FAILED after {attempts} retries.')
        return False

    async def send_message(self,
                           namespaces: list,
                           topics: list,
                           message_body: str,
                           application_properties: dict,
                           src_enqueued_time_utc: datetime.datetime,
                           session_id: Optional[str]):
        """
        Send a message to the correct namespace and topic (batched by default).

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
        src_enqueued_time_utc : datetime
            The timestamp of when the message was enqueued on the source namespace.
        """
        # enqueue on each relevant destination batcher and await per-item flush
        timestamp = src_enqueued_time_utc.isoformat(timespec='milliseconds') + 'Z'
        props = dict(application_properties or {})
        props['__src_enqueued_time_utc'] = timestamp
        await asyncio.gather(*[
            self._get_batcher(ns, topics[idx]).add_and_wait(
                self._build_message(message_body, dict(props), topics[idx], session_id)
            )
            for idx, ns in enumerate(namespaces)
        ])

    async def send_message_unbatched(
        self,
        namespaces: list,
        topics: list,
        message_body: str,
        application_properties: dict,
        src_enqueued_time_utc: datetime.datetime,
        session_id: Optional[str],
    ):
        """Send data for session enabled streams."""
        timestamp = src_enqueued_time_utc.isoformat(timespec='milliseconds') + 'Z'
        base_props = dict(application_properties or {})
        base_props['__src_enqueued_time_utc'] = timestamp

        for idx, ns in enumerate(namespaces):
            topic = topics[idx]
            client = self.clients[ns]

            async with client.get_topic_sender(topic) as sender:
                msg = self._build_message(
                    message_body,
                    dict(base_props),  # defensive copy
                    topic,
                    session_id
                )
                await sender.send_messages(msg)

    async def start(self):
        """Initialize Service Bus client for receiving and clients for sending."""
        logger.debug('Creating connection for source namespace.')
        self.source_client = ServiceBusClient.from_connection_string(self.source_connection_string)
        await self.source_client.__aenter__()

        for namespace, conn_str in self.namespaces.items():
            logger.debug(f'Creating connection for destination namespace: {namespace}.')
            client = ServiceBusClient.from_connection_string(conn_str)
            await client.__aenter__()
            self.clients[namespace] = client

        self.keep_alive_tasks.append(asyncio.create_task(self.keep_source_connection_alive()))

    async def wait_for_amqp_ready(self, timeout=30):
        """Test that AMQP/ServiceBus is actually ready."""
        start = time.time()
        topic_name, subscription_name = self.input_topics[0]

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


async def main():
    """Configure the handler."""
    handler = ServiceBusHandler(EnvironmentConfigParser())
    await handler.start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown():
        logger.warning('Shutdown signal received. Cleaning up...')
        stop_event.set()

    loop.add_signal_handler(signal.SIGTERM, shutdown)
    loop.add_signal_handler(signal.SIGINT, shutdown)

    run_task = asyncio.create_task(handler.run())

    try:
        await stop_event.wait()
    finally:
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
        await handler.close()


if __name__ == '__main__':
    logger.info(f'Starting version "{__version__}".')
    config = EnvironmentConfigParser()
    start_http_server(config.get_prometheus_port())
    asyncio.run(main())
