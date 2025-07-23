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
        And max_auto_renew_duration is <max_auto_renew_duration>

        Examples:
            | rule_definition                                                                                                                                                                                     | message_file | source_topic | is_match | max_auto_renew_duration |
            | { "destination_namespaces": "GB,IE", "destination_topics": "bar,bar", "source_subscription": "foo", "source_topic": "foo", "max_auto_renew_duration": 600}                                                                          | input-1.json | foo          | True     | 600                     |
            | { "destination_namespaces": "GB,IE", "destination_topics": "bar,bar", "source_subscription": "foo", "source_topic": "foo"}                                                                          | input-1.json | snafu        | False    | 300                     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "regexp": "GB", "source_subscription": "foo", "source_topic": "foo"}                                                                 | input-1.json | foo          | True     | 300                     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "regexp": "GB", "source_subscription": "foo", "source_topic": "foo"}                                                                 | input-1.json | foo          | True     | 300                     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "country", "regexp": "^GB$", "source_subscription": "foo", "source_topic": "foo"}                                        | input-2.json | foo          | False    | 300                     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "country", "regexp": "^IE$", "source_subscription": "foo", "source_topic": "foo"}                                        | input-2.json | foo          | True     | 300                     |
            | { "destination_namespaces": "GB", "destination_topics": "bar", "jmespath": "details[?telephone_number].telephone_number", "regexp": "^\\+44", "source_subscription": "foo", "source_topic": "foo"}  | input-3.json | foo          | True     | 300                     |
            | { "destination_namespaces": "IE", "destination_topics": "bar", "jmespath": "details[?telephone_number].telephone_number", "regexp": "^\\+353", "source_subscription": "foo", "source_topic": "foo"} | input-3.json | foo          | False    | 300                     |
            | { "destination_namespaces": "", "destination_topics": "", "jmespath": "country", "regexp": "^FR$", "source_subscription": "foo", "source_topic": "foo"}                                             | input-5.json | foo          | True     | 300                     |
            | { "destination_namespaces": "", "destination_topics": "", "jmespath": "country", "regexp": "^FR$", "source_subscription": "foo", "source_topic": "foo"}                                             | input-7.txt  | foo          | False    | 300                     |

    Scenario Outline: Rule Exceptions
        Given an invalid Router Rule of <rule>
        When the rule is initialised
        Then the SystemExit is 2

        Examples:
            | rule                            |
            | Invalid JSON.                   |
            | { "message": "Invalid schema" } |
