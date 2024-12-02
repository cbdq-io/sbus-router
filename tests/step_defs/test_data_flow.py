import json
import logging
import time

import pytest
from proton import Message
from proton._exceptions import Timeout
from proton.utils import BlockingConnection
from pytest_bdd import given, parsers, scenario, then, when

from router import ConnectionStringHelper, get_logger

logger = get_logger(__name__, logging.DEBUG)


@scenario('data-flow.feature', 'Inject a Message and Confirm the Destination')
def test_inject_a_message_and_confirm_the_destination():
    """Inject a Message and Confirm the Destination."""


@given(parsers.parse('the input topic is {input_topic}'), target_fixture='input_topic')
def _(input_topic: str):
    """the input topic is <input_topic>."""
    return input_topic


@given('the landing Service Bus Emulator', target_fixture='aqmp_url')
def _():
    """the landing Service Bus Emulator."""
    conn_str = 'Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;'
    conn_str += 'SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
    conn_str = ConnectionStringHelper(conn_str)
    return conn_str.amqp_url()


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


@when('the input message is sent')
def _(aqmp_url: str, input_topic: str, output_topic: str, message_body: str):
    """the input message is sent."""
    if output_topic == 'N/A':
        conn = BlockingConnection(aqmp_url)
        sender = conn.create_sender(input_topic)
        sender.send(Message(body=message_body))
        pytest.skip(f'Output topic is "{output_topic}".')


@then('the expected output message is received')
def _(aqmp_url: str, input_topic: str, output_topic: str, message_body: str):
    """The expected output message is received."""
    conn = BlockingConnection(aqmp_url)
    receiver = conn.create_receiver(f'{output_topic}/Subscriptions/test')
    logger.debug(f'Receiver for "{output_topic}" created at {time.time()}.')

    time.sleep(0.5)  # Add delay to ensure the receiver is fully ready

    sender = conn.create_sender(input_topic)
    logger.debug(f'Sending message "{message_body}" to "{input_topic}" at {time.time()}.')
    sender.send(Message(body=message_body))

    for attempt in range(10):  # Retry receiving for up to 10 seconds
        try:
            logger.debug(f'Attempt {attempt + 1} to receive message from "{output_topic}".')
            received_message = receiver.receive(timeout=1).body
            logger.debug(f'Message received: "{received_message}" at {time.time()}.')
            receiver.accept()
            break
        except Timeout as e:
            logger.warning(f'Receive attempt {attempt + 1} timed out: {e}')
            received_message = None
    else:
        pytest.fail(f'Message not received within the retry limit from "{output_topic}".')

    conn.close()
    assert received_message == message_body
