import json
import logging
import time

import pytest
import testinfra
from proton import Message
from proton._exceptions import Timeout
from proton.utils import BlockingConnection
from pytest_bdd import given, parsers, scenario, then, when

from replay_dlq import Container, DLQReplayHandler, RuntimeParams
from router import ConnectionStringHelper, get_logger

logger = get_logger(__name__, logging.DEBUG)


@scenario('data-flow.feature', 'Inject a Message and Confirm the Destination')
def test_inject_a_message_and_confirm_the_destination():
    """Inject a Message and Confirm the Destination."""


@scenario('data-flow.feature', 'Replay DLQ Message')
def test_replay_dlq_message():
    """Replay DLQ Message."""


@given(parsers.parse('the input topic is {input_topic}'), target_fixture='input_topic')
def _(input_topic: str):
    """the input topic is <input_topic>."""
    return input_topic


@given('the landing Service Bus Emulator', target_fixture='connection_details')
def _():
    """the landing Service Bus Emulator."""
    conn_str = 'Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;'
    conn_str += 'SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
    conn_str = ConnectionStringHelper(conn_str)
    return conn_str


@given(parsers.parse('the message contents is {input_data_file}'), target_fixture='message_body')
def _(input_data_file: str):
    """the message contents is <input_data_file>."""
    input_data_file_name = f'tests/resources/input-data/{input_data_file}'

    with open(input_data_file_name, 'rt') as stream:
        data = json.load(stream)

    return json.dumps(data)


@given(parsers.parse('the output topic is {output_topic}'), target_fixture='output_topic')
def _(output_topic: str):
    """the output topic is <output_topic>."""
    return output_topic


@when('the DLQ messags are replayed')
def _(connection_details: ConnectionStringHelper):
    """the DLQ messags are replayed."""
    runtime = RuntimeParams(
        [
            '--connection-string', connection_details.sbus_connection_string,
            '--name', 'dlq',
            '--subscription', 'dlq_replay'
        ]
    )
    replayer = DLQReplayHandler(
        f'{runtime.dlq_topic_name}/Subscriptions/{runtime.subscription}',
        connection_details,
        5,
        logger
    )
    Container(replayer).run()


@when('the input message is sent')
def _(connection_details: ConnectionStringHelper, input_topic: str, output_topic: str, message_body: str):
    """the input message is sent."""
    if output_topic == 'N/A':
        conn = BlockingConnection(connection_details.amqp_url())
        sender = conn.create_sender(input_topic)
        sender.send(Message(body=message_body))
        pytest.skip(f'Output topic is "{output_topic}".')


@then('the DLQ count is 2')
def _():
    """the DLQ count is 2."""
    host = testinfra.get_host('docker://router')
    cmd = host.run('curl localhost:8000')
    assert 'dlq_message_count_total 2.0' in cmd.stdout


def is_message_valid(message: Message, expected_body: str, topic_name: str) -> bool:
    """
    Check the validity of the received message.

    Parameters
    ----------
    message : proton.Message
        The message to be validated.
    expected_body : str
        The message body that is expected to be recieved.
    topic_name : str
        The name of the topic that the message was received from.

    Returns
    -------
    bool
        True if the message is as expected, false otherwise.
    """
    response = True

    if topic_name == 'dlq':
        if 'source_topic' not in message.properties:
            logger.error('No "source_topic" properties in DLQ message.')
            response = False

    if message.body != expected_body:
        logger.error(f'Expected message "{expected_body}".')
        logger.error(f'Actual message "{message.body}".')
        response = False

    return response


@then('the expected output message is received')
def _(connection_details: ConnectionStringHelper, input_topic: str, output_topic: str, message_body: str):
    """The expected output message is received."""
    conn = BlockingConnection(connection_details.amqp_url())
    receiver = conn.create_receiver(f'{output_topic}/Subscriptions/test')
    logger.debug(f'Receiver for "{output_topic}" created at {time.time()}.')

    time.sleep(0.5)  # Add delay to ensure the receiver is fully ready

    sender = conn.create_sender(input_topic)
    logger.debug(f'Sending message "{message_body}" to "{input_topic}" at {time.time()}.')
    sender.send(Message(body=message_body))

    for attempt in range(10):  # Retry receiving for up to 10 seconds
        try:
            logger.debug(f'Attempt {attempt + 1} to receive message from "{output_topic}".')
            message = receiver.receive(timeout=1)
            logger.debug(f'Message received: "{message.body}" at {time.time()}.')
            receiver.accept()
            break
        except Timeout as e:
            logger.warning(f'Receive attempt {attempt + 1} timed out: {e}')
    else:
        pytest.fail(f'Message not received within the retry limit from "{output_topic}".')

    conn.close()
    assert is_message_valid(message, message_body, output_topic)
