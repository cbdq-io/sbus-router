"""Environment Configuration Parser feature tests."""

from pytest_bdd import given, parsers, scenario, then, when

from router import EnvironmentConfigParser


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


@then(parsers.parse('service bus count is {expected_count:d} with the namespace {namespace}'))
def _(expected_count: int, namespace: str, environ: dict):
    """service bus count is <count>."""
    try:
        print(f'Environ is "{environ}".')
        widget = EnvironmentConfigParser(environ)
        sbus_namespaces = widget.service_bus_namespaces()
        actual_count = sbus_namespaces.count()
        sbus_namespaces.get(namespace)
    except ValueError:
        actual_count = 0

    assert actual_count == expected_count


@then(parsers.parse('the EnvironmentConfigParser method {method_name} returns {expected_value}'))
def _(method_name: str, expected_value: str, environ: dict):
    """the EnvironmentConfigParser method <method_name> returns <value>."""
    print(f'Environ is "{environ}".')
    widget = EnvironmentConfigParser(environ)

    if method_name == 'get_dead_letter_queue':
        actual_value = widget.get_dead_letter_queue()
    elif method_name == 'get_source_url':
        actual_value = widget.get_source_url()
        expected_value = 'amqps://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost:5671'
    else:
        raise NotImplementedError(f'No method name "{method_name}".')

    message = f'Expected return value of {method_name} to be "{expected_value}" '
    message += f'but got "{actual_value}" instead.'
    assert actual_value == expected_value, message
