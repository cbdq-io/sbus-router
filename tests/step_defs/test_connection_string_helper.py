"""Connection String Helper feature tests."""

from pytest_bdd import given, parsers, scenario, then, when

from router import ConnectionStringHelper


@scenario('connection-string-helper.feature', 'Valid Connection Strings')
def test_valid_connection_strings():
    """Valid Connection Strings."""


@given(parsers.parse('Azure Service Bus Connection String {sbus_connection_string}'),
       target_fixture='sbus_connection_string')
def _(sbus_connection_string: str):
    """Azure Service Bus Connection String <sbus_connection_string>."""
    return sbus_connection_string


@when('the Azure Service Bus Connection String is parsed')
def _():
    """the Azure Service Bus Connection String is parsed."""
    pass


@then(parsers.parse('the AMQP URL is {expected_url}'))
def _(expected_url: str, sbus_connection_string):
    """the AMQP URL is <amqp_url>."""
    widget = ConnectionStringHelper(sbus_connection_string)
    actual_url = widget.amqp_url()
    message = f'Expected AMQP URL of "{expected_url}", but got "{actual_url}".'
    assert actual_url == expected_url, message
