@unit
Feature: Router Rule
    In order to route a message
    As a Router
    I want a Router Rule engine

    Scenario Outline: Test Rules Against Input Files
        Given a RouterRule definition of <rule_definition>
        And the RouterRule message file is <message_file>
        And the RouterRule message is <is_binary>
        And the RouterRule source topic is <source_topic>
        When the message is matched against the RouterRule
        Then the RouterRule match is <is_match>

        Examples:
            | rule_definition                                                                                                                                                                                     | message_file | is_binary | source_topic | is_match |
            | { "destination_namespaces": "GB,IE", "destination_topics": "bar,bar", "source_subscription": "foo", "source_topic": "foo"}                                                                          | input-1.json | False     | foo          | True     |
            | { "destination_namespaces": "GB,IE", "destination_topics": "bar,bar", "source_subscription": "foo", "source_topic": "foo"}                                                                          | input-1.json | False     | snafu        | False    |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "regexp": "GB", "source_subscription": "foo", "source_topic": "foo"}                                                                 | input-1.json | False     | foo          | True     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "regexp": "GB", "source_subscription": "foo", "source_topic": "foo"}                                                                 | input-1.json | True      | foo          | True     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "regexp": "GB", "source_subscription": "foo", "source_topic": "foo"}                                                                 | input-1.json | True      | foo          | True     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "country", "regexp": "^GB$", "source_subscription": "foo", "source_topic": "foo"}                                        | input-2.json | False     | foo          | False    |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "country", "regexp": "^IE$", "source_subscription": "foo", "source_topic": "foo"}                                        | input-2.json | False     | foo          | True     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "details[?telephone_number].telephone_number", "regexp": "^\\+44", "source_subscription": "foo", "source_topic": "foo"}  | input-3.json | False     | foo          | True     |
            | { "destination_namespaces": "IE", "destination_topics": "bar", "jmespath": "details[?telephone_number].telephone_number", "regexp": "^\\+353", "source_subscription": "foo", "source_topic": "foo"} | input-3.json | False     | foo          | False    |
            | { "destination_namespaces": "", "destination_topics": "", "jmespath": "country", "regexp": "^FR$", "source_subscription": "foo", "source_topic": "foo"}                                             | input-5.json | False     | foo          | True     |
            | { "destination_namespaces": "", "destination_topics": "", "jmespath": "country", "regexp": "^FR$", "source_subscription": "foo", "source_topic": "foo"}                                             | input-7.txt  | False     | foo          | False    |

    Scenario Outline: Rule Exceptions
        Given an invalid Router Rule of <rule>
        When the rule is initialised
        Then the SystemExit is 2

        Examples:
            | rule                            |
            | Invalid JSON.                   |
            | { "message": "Invalid schema" } |
