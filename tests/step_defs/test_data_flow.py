"""Data Flow feature tests."""
import json
import time

import pytest
import testinfra
from proton.handlers import MessagingHandler
from proton.reactor import Container
from pytest_bdd import given, parsers, scenario, then, when

from router import ConnectionStringHelper, SimpleSender, get_logger

logger = get_logger(__file__)


class TestMessageId:
    def __init__(self) -> None:
        self.id = None
        self.id_found_in_message = False
        self.topic_name = None
        self.url = None

    def __str__(self):
        """Print the contents of the object."""
        response = f'Expected to find a message with id "{self.id}" '
        response += f'in the "{self.topic_name}" topic.'
        return response


class Consumer(MessagingHandler):
    def __init__(self, url, topic, timeout):
        super(Consumer, self).__init__()
        self.url = url
        self.topic = topic
        self.timeout = timeout
        self.start_time = None

    def on_start(self, event):
        # Store the start time to calculate the elapsed time
        self.start_time = time.time()

        # Create a connection to the broker
        conn = event.container.connect(self.url)

        # Create a receiver link (consumer) on the topic
        event.container.create_receiver(conn, self.topic)

    def on_message(self, event):
        global id_found_in_message

        # Check if the timeout has been reached
        if time.time() - self.start_time > self.timeout:
            print(f'No message received on topic {self.topic} within timeout.')

        # Handle the incoming message
        message = event.message
        print(f'Received message: {message.body}')
        data = json.loads(message.body)
        test_widget.id_found_in_message = data['id'] == test_widget.id

    def on_disconnected(self, event):
        print('Disconnected from broker.')


test_widget = TestMessageId()


@scenario('data-flow.feature', 'Inject a Message and Confirm the Destination')
def test_inject_a_message_and_confirm_the_destination():
    """Inject a Message and Confirm the Destination."""


@given('the landing Service Bus Emulator')
def _():
    """
    Wait for the Service Bus Emulator to be ready.

    Then return a connection string.
    """
    host = testinfra.get_host('local://')

    while True:
        cmd = host.run('docker logs servicebus-emulator')
        logs = cmd.stdout

        if 'Emulator Service is Successfully Up!' in logs:
            break

        time.sleep(1)

    conn_str = 'Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;'
    conn_str += 'SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
    conn_str = ConnectionStringHelper(conn_str)
    url = conn_str.amqp_url()
    logger.debug(f'AMQP URL is "{url}".')
    test_widget.url = url


@when(parsers.parse('the landed topic data is {input_data_file} into {topic_name}'))
def _(input_data_file: str, topic_name: str) -> None:
    """the landed topic data is <input_data_file> into <topic>."""
    id = input_data_file.split('-')[1].split('.')[0]
    test_widget.id = int(id)
    input_data_file_name = f'tests/resources/input-data/{input_data_file}'

    with open(input_data_file_name, 'rt') as stream:
        data = json.load(stream)

    message = json.dumps(data)
    Container(SimpleSender(test_widget.url, topic_name, message)).run()


@then(parsers.parse('read message with the expected ID will be on the {output_topic}'))
def _(output_topic: str):
    """read message with the expected ID will be on the <output_topic>."""
    if output_topic == 'N/A':
        pytest.skip('No output traffic expected.')

    test_widget.id_found_in_message = False
    test_widget.topic_name = output_topic
    container = Container(Consumer(test_widget.url, output_topic, 1))
    container.run()
    assert test_widget.id_found_in_message, str(test_widget)
