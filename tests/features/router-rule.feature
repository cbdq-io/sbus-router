@unit
Feature: Router Rule
    In order to route a message
    As a Router
    I want a Router Rule engine

    Scenario Outline: Test Rules Against Input Files
        Given a RouterRule definition of <rule_definition>
        And the RouterRule message file is <message_file>
        And the RouterRule source topic is <source_topic>
        When the message is matched against the RouterRule
        Then the RouterRule match is <is_match>
        And max_tasks is <max_tasks>

        Examples:
            | rule_definition                                                                                                                                                                                     | message_file | source_topic | is_match | max_tasks |
            | { "destination_namespaces": "GB,IE", "destination_topics": "bar,bar", "source_subscription": "foo", "source_topic": "foo", "max_auto_renew_duration": 600}                                          | input-1.json | foo          | True     | 1         |
            | { "destination_namespaces": "GB,IE", "destination_topics": "bar,bar", "source_subscription": "foo", "source_topic": "foo"}                                                                          | input-1.json | snafu        | False    | 1         |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "regexp": "GB", "source_subscription": "foo", "source_topic": "foo"}                                                                 | input-1.json | foo          | True     | 1         |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "regexp": "GB", "source_subscription": "foo", "source_topic": "foo"}                                                                 | input-1.json | foo          | True     | 1         |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "country", "regexp": "^GB$", "source_subscription": "foo", "source_topic": "foo"}                                        | input-2.json | foo          | False    | 1         |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "country", "regexp": "^IE$", "source_subscription": "foo", "source_topic": "foo"}                                        | input-2.json | foo          | True     | 1         |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "details[?telephone_number].telephone_number", "regexp": "^\\+44", "source_subscription": "foo", "source_topic": "foo"}  | input-3.json | foo          | True     | 1         |
            | { "destination_namespaces": "IE", "destination_topics": "bar", "jmespath": "details[?telephone_number].telephone_number", "regexp": "^\\+353", "source_subscription": "foo", "source_topic": "foo"} | input-3.json | foo          | False    | 1         |
            | { "destination_namespaces": "", "destination_topics": "", "jmespath": "country", "regexp": "^FR$", "source_subscription": "foo", "source_topic": "foo"}                                             | input-5.json | foo          | True     | 1         |
            | { "destination_namespaces": "", "destination_topics": "", "jmespath": "country", "regexp": "^FR$", "source_subscription": "foo", "source_topic": "foo", "max_tasks": 42}                            | input-7.txt  | foo          | False    | 42        |

    Scenario Outline: Rule Exceptions
        Given an invalid Router Rule of <rule>
        When the rule is initialised
        Then the SystemExit is 2

        Examples:
            | rule                            |
            | Invalid JSON.                   |
            | { "message": "Invalid schema" } |
