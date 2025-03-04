"""Router Rule feature tests."""
import json

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from router import RouterRule


@scenario('router-rule.feature', 'Rule Exceptions')
def test_rule_exceptions():
    """Rule Exceptions."""


@scenario('router-rule.feature', 'Test Rules Against Input Files')
def test_test_rules_against_input_files():
    """Test Rules Against Input Files."""


@given(parsers.parse('a RouterRule definition of {rule_definition}'), target_fixture='router_rule')
def _(rule_definition: str):
    """a RouterRule definition of <rule_definition>."""
    return RouterRule('test', rule_definition)


@given(parsers.parse('an Invalid Router Rule of {rule}'), target_fixture='rule_string')
def _(rule: str):
    """an Kafka Router Rule of <rule>."""
    return rule


@given(parsers.parse('the RouterRule message file is {message_file}'), target_fixture='message_contents')
def _(message_file: str):
    """the RouterRule message file is <message_file>."""
    path = f'tests/resources/input-data/{message_file}'

    if path.endswith('.json'):
        with open(path, 'r') as stream:
            data = json.load(stream)

        return json.dumps(data)
    else:
        with open(path, 'r') as stream:
            data = stream.read()

    return data.strip()


@given(parsers.parse('the RouterRule source topic is {source_topic}'), target_fixture='source_topic')
def _(source_topic: str):
    """the RouterRule source topic is <source_topic>."""
    return source_topic


@when('the message is matched against the RouterRule')
def _():
    """the message is matched against the RouterRule."""
    pass


@when('the rule is initialised')
def _():
    """the rule is initialised."""
    pass


@then(parsers.parse('the RouterRule match is {is_match}'))
def _(is_match: str, router_rule: RouterRule, message_contents: str, source_topic: str):
    """the RouterRule match is <is_match>."""
    expected_value = is_match.capitalize() == 'True'
    (actual_value, _, _) = router_rule.is_match(source_topic, message_contents)
    assert expected_value == actual_value


@then('the SystemExit is 2')
def _(rule_string: str):
    """the SystemExit is 2."""
    with pytest.raises(SystemExit) as exit_info:
        RouterRule('test', rule_string)

    assert exit_info.value.code == 2
