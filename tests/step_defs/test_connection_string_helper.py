"""Connection String Helper feature tests."""

from pytest_bdd import given, parsers, scenario, then, when

from router import ConnectionStringHelper


@scenario('connection-string-helper.feature', 'Invalid Connection Strings')
def test_invalid_connection_strings():
    """Invalid Connection Strings."""


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


@then(parsers.parse('key_name is {expected_key_name}'))
def _(expected_key_name: str, sbus_connection_string: str):
    """key_name is <key_name>."""
    widget = ConnectionStringHelper(sbus_connection_string)
    assert widget.key_name() == expected_key_name


@then(parsers.parse('key_value is {expected_key_value}'))
def _(expected_key_value: str, sbus_connection_string: str):
    """key_value is <key_value>."""
    widget = ConnectionStringHelper(sbus_connection_string)
    assert expected_key_value == widget.key_value()


@then(parsers.parse('netloc is {expected_netloc}'))
def _(expected_netloc: str, sbus_connection_string: str):
    """netloc is <netloc>."""
    widget = ConnectionStringHelper(sbus_connection_string)
    assert widget.netloc() == expected_netloc


@then(parsers.parse('the AMQP URL is {expected_url}'))
def _(expected_url: str, sbus_connection_string):
    """the AMQP URL is <amqp_url>."""
    widget = ConnectionStringHelper(sbus_connection_string)
    actual_url = widget.amqp_url()
    message = f'Expected AMQP URL of "{expected_url}", but got "{actual_url}".'
    assert actual_url == expected_url, message


@then('the invalid connection string raised a ValueError')
def _(sbus_connection_string: str):
    """the invalid connection string raised a ValueError."""
    value_error_exception_thrown = False

    try:
        ConnectionStringHelper(sbus_connection_string)
    except ValueError:
        value_error_exception_thrown = True
    except Exception as e:
        print(e)

    assert value_error_exception_thrown
