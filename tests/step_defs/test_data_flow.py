import json
import logging
import time

import pytest
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
from pytest_bdd import given, parsers, scenario, then, when

from router import ConnectionStringHelper

logger = logging.getLogger(__name__)


class SimpleSender(MessagingHandler):
    def __init__(self, url: str, target: str, message_body: Message) -> None:
        super(SimpleSender, self).__init__()
        self.url = url
        self.target = target
        self.message_body = message_body

    def on_start(self, event):
        logger.debug(f'Creating a connection to "{self.url}".')
        conn = event.container.connect(self.url)
        self.sender = event.container.create_sender(conn, self.target)

    def on_sendable(self, event):
        message = Message(body=self.message_body)
        event.sender.send(message)
        logger.debug(f'Message sent "{self.message_body}".')
        event.sender.close()
        event.connection.close()


class Recv(MessagingHandler):
    def __init__(self, url: str, topic: str):
        super(Recv, self).__init__()
        self.url = url
        self.topic = topic
        self.received_message = None

    def on_start(self, event):
        """Set up the connection and receive."""
        logger.debug(f'Creating a connection to "{self.url}".')
        conn = event.container.connect(self.url)
        event.container.create_receiver(conn, source=f'{self.topic}/Subscriptions/test')

    def on_message(self, event):
        """Handle incoming message."""
        logger.debug(f'Received message: {event.message.body}')
        self.received_message = event.message.body
        event.receiver.close()
        event.connection.close()

    def get_received_message(self):
        """Return the received message."""
        return self.received_message

    def run(self):
        container = Container(self)
        container.run(timeout=10)


@scenario('data-flow.feature', 'Inject a Message and Confirm the Destination')
def test_inject_a_message_and_confirm_the_destination():
    """Inject a Message and Confirm the Destination."""


@given('the landing Service Bus Emulator', target_fixture='test_details')
def _():
    """Wait for the Service Bus Emulator to be ready."""
    conn_str = 'Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;'
    conn_str += 'SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
    conn_str = ConnectionStringHelper(conn_str)
    url = conn_str.amqp_url()
    return {'url': url}


@when(parsers.parse('the landed topic data is {input_data_file} into {topic_name}'))
def _(input_data_file: str, topic_name: str, test_details: dict) -> None:
    """Inject the landed topic data."""
    id = input_data_file.split('-')[1].split('.')[0]
    test_details['expected_id'] = int(id)
    input_data_file_name = f'tests/resources/input-data/{input_data_file}'

    with open(input_data_file_name, 'rt') as stream:
        data = json.load(stream)

    message = json.dumps(data)
    Container(SimpleSender(test_details['url'], topic_name, message)).run()


def get_message(url: str, topic: str) -> str:
    time.sleep(1)
    handler = Recv(url, topic)
    container = Container(handler)
    container.run(timeout=5)
    message = handler.get_received_message()
    return message


@then(parsers.parse('read message with the expected ID will be on the {output_topic}'))
def _(output_topic: str, test_details: dict):
    """Ensure the expected message is read from the output topic."""
    if output_topic == 'N/A':
        pytest.skip('No output traffic expected.')

    message = get_message(test_details['url'], output_topic)
    assert message is not None
    message = json.loads(message)
    assert message['id'] == test_details['expected_id']
