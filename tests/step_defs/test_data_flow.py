import json
import logging
import time

import pytest
import testinfra
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.amqp import AmqpMessageBodyType
from pytest_bdd import given, parsers, scenario, then, when

import router

logger = router.get_logger(__name__, logging.DEBUG)


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


@given('the landing Service Bus Emulator', target_fixture='connection_string')
def _():
    """the landing Service Bus Emulator."""
    conn_str = 'Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;'
    conn_str += 'SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
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


@when('the input message is sent')
def _(connection_string: str, input_topic: str, output_topic: str, message_body: str):
    """the input message is sent."""
    none_destination_topics = ['N/A', 'DLQ']

    if output_topic in none_destination_topics:
        client = ServiceBusClient.from_connection_string(connection_string)
        sender = client.get_topic_sender(input_topic)
        sender.send_messages(ServiceBusMessage(body=message_body, session_id='0'))
        pytest.skip(f'Output topic is "{output_topic}".')


@then('the DLQ count is 1')
def _():
    """the DLQ count is 1."""
    host = testinfra.get_host('docker://router')
    cmd = host.run('curl localhost:8000')
    assert 'dlq_message_count_total 1.0' in cmd.stdout


@then('the deleted DLQ messages is 1')
def _(connection_string: str):
    from nukedlq import nuke_dead_letter_messages

    deleted_messages = nuke_dead_letter_messages(
        connection_str=connection_string,
        topic_name='topic.2',
        subscription_name='test',
        period='PT1S'
    )
    assert deleted_messages == 1


def is_message_valid(message: ServiceBusMessage, expected_body: str, topic_name: str) -> bool:
    """
    Check the validity of the received message.

    Parameters
    ----------
    ServiceBusMessage : azure.servicebus.ServiceBusMessage
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

    if message.body_type == AmqpMessageBodyType.DATA:
        actual_body = b''.join(message.body).decode()
    else:
        actual_body = message.body

    if actual_body != expected_body:
        logger.error(f'Expected message "{expected_body}".')
        logger.error(f'Actual message "{actual_body}".')
        response = False

    return response


@then('the expected output message is received')
def _(connection_string: str, input_topic: str, output_topic: str, message_body: str):
    """The expected output message is received."""
    client = ServiceBusClient.from_connection_string(connection_string)
    sender = client.get_topic_sender(input_topic)
    logger.debug(f'Sending message "{message_body}" to "{input_topic}" at {time.time()}.')
    sender.send_messages(ServiceBusMessage(body=message_body, session_id='0'))
    sender.close()
    message_received = False

    if output_topic == 'ie.topic':
        receiver = client.get_subscription_receiver(
            output_topic,
            'test',
            max_wait_time=5,
            session_id='0'
        )
    else:
        receiver = client.get_subscription_receiver(output_topic, 'test', max_wait_time=5)

    for message in receiver:
        logger.debug(f'Message received: "{str(message.body)}" of type "{message.body_type}".')
        receiver.complete_message(message)
        message_received = True

    receiver.close()
    client.close()
    assert message_received, f'Message not received within the retry limit from "{output_topic}".'
    assert is_message_valid(message, message_body, output_topic)
