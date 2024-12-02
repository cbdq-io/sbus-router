"""Environment Configuration Parser feature tests."""

from pytest_bdd import given, parsers, scenario, then, when

from router import EnvironmentConfigParser


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
