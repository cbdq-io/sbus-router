"""Environment Configuration Parser feature tests."""
import json

from pytest_bdd import given, parsers, scenario, then, when

from router import EnvironmentConfigParser, ServiceBusHandler


@scenario('environment-config-parser.feature', 'Construct Message ID')
def test_construct_message_id():
    """Construct Message ID."""


@scenario('environment-config-parser.feature', 'EnvironmentConfigParser Methods')
def test_environmentconfigparser_methods():
    """EnvironmentConfigParser Methods."""


@scenario('environment-config-parser.feature', 'Service Bus Namespace Connection Strings')
def test_service_bus_namespace_connection_strings():
    """Service Bus Namespace Connection Strings."""


@given('an Environment Dictionary', target_fixture='environ')
def _():
    """an Environment Dictionary."""
    return {}


@when(parsers.parse('the environment variable {key} has a value of {value}'))
def _(key: str, value: str, environ: dict):
    """the environment variable <key> has a value of <value>."""
    environ[key] = value


@when(parsers.parse('the message body is {message_body}'), target_fixture='message_body')
def _(message_body: str):
    """the message body is <message_body>."""
    return message_body


@when(parsers.parse('the message_id is {message_id}'), target_fixture='message_id')
def _(message_id: str):
    """the message_id is <message_id>."""
    return message_id


@then(parsers.parse('service bus count is {expected_count:d} with the namespace {namespace}'))
def _(expected_count: int, namespace: str, environ: dict):
    """service bus count is <count>."""
    try:
        print(f'Environ is "{environ}".')
        widget = EnvironmentConfigParser(environ)
        sbus_namespaces = widget.service_bus_namespaces()
        actual_count = sbus_namespaces.count()
        sbus_namespaces.get(namespace)
        sbus_namespaces.get_all_namespaces()
        widget.get_prefetch_count()
        len(sbus_namespaces.get_all_namespaces()) == 1
    except ValueError:
        actual_count = 0

    assert actual_count == expected_count


@then(parsers.parse('the EnvironmentConfigParser method {method_name} returns {expected_value}'))
def _(method_name: str, expected_value: str, environ: dict):
    """the EnvironmentConfigParser method <method_name> returns <value>."""
    print(f'Environ is "{environ}".')
    widget = EnvironmentConfigParser(environ)

    if method_name == 'get_source_url':
        actual_value = widget.get_source_connection_string()
    elif method_name == 'get_prometheus_port':
        expected_value = int(expected_value)
        actual_value = widget.get_prometheus_port()
    elif method_name == 'get_rules':
        ServiceBusHandler(widget)
        widget.topics_and_subscriptions()
        actual_value = widget.get_rules()[0].name()
        expected_value = widget.get_rules()[0].name()
    elif method_name == 'get_ts_app_prop_name':
        actual_value = widget.get_ts_app_prop_name()
    else:
        raise NotImplementedError(f'No method name "{method_name}".')

    message = f'Expected return value of {method_name} to be "{expected_value}" '
    message += f'but got "{actual_value}" instead.'
    assert actual_value == expected_value, message
    assert not widget.is_deduplication_enabled()


@then(parsers.parse('the constructed message_id is {expected_message_id}'))
def _(expected_message_id: str, message_body: str, message_id: str, environ: dict):
    """the constructed message_id is <expected_message_id>."""
    environ['ROUTER_SOURCE_CONNECTION_STRING'] = 'foobar'
    environ['ROUTER_NAMESPACE_IE_CONNECTION_STRING'] = 'foobar'
    application_properties = {}
    widget = EnvironmentConfigParser(environ)
    is_deduplication_enabled = widget.is_deduplication_enabled()
    handler = ServiceBusHandler(widget)

    if expected_message_id == 'None':
        expected_message_id = None

    if message_id != 'None':
        application_properties['message_id'] = message_id

    actual_message_id = handler.get_message_id(
        message_body=message_body,
        message_application_properties=application_properties,
        is_deduplication_enabled=is_deduplication_enabled
    )

    context = {
        'expected_message_id': expected_message_id,
        'message_body': message_body,
        'message_id': message_id,
        'environ': environ,
        'is_deduplication_enabled': is_deduplication_enabled,
        'actual_message_id': actual_message_id
    }

    assert actual_message_id == expected_message_id, json.dumps(context)
