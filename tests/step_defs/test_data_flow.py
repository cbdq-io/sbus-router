"""Data Flow feature tests."""
import json

from proton.reactor import Container
from pytest_bdd import given, parsers, scenario, when

from router import ConnectionStringHelper, SimpleSender, get_logger

logger = get_logger(__file__)


@scenario('data-flow.feature', 'Inject a Message and Confirm the Destination')
def test_inject_a_message_and_confirm_the_destination():
    """Inject a Message and Confirm the Destination."""


@given('the landing Service Bus Emulator', target_fixture='sender_url')
def _():
    """the landing Service Bus Emulator."""
    conn_str = 'Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;'
    conn_str += 'SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
    conn_str = ConnectionStringHelper(conn_str)
    url = conn_str.amqp_url()
    logger.debug(f'AMQP URL is "{url}".')
    return url


@when(parsers.parse('the landed topic data is {input_data_file} into {topic_name}'))
def _(input_data_file: str, topic_name: str, sender_url: str) -> None:
    """the landed topic data is <input_data_file> into <topic>."""
    input_data_file_name = f'tests/resources/input-data/{input_data_file}'

    with open(input_data_file_name, 'rt') as stream:
        data = json.load(stream)

    message = json.dumps(data)
    Container(SimpleSender(sender_url, 'topic.1', message)).run()
