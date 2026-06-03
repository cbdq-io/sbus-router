@unit
Feature: Environment Configuration Parser

    Test that we can correctly extract configuration from
    the Environment.

    Scenario Outline: Service Bus Namespace Connection Strings
        Given an Environment Dictionary
        When the environment variable <key> has a value of <value>
        Then service bus count is <count> with the namespace <namespace>

        Examples:
            | key                                    | value                                                                                                | count    | namespace |
            | ROUTER_NAMESPACE_IE_CONNECTION_STRING  | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE; | 1        | IE        |
            | ROUTER_NAMESPACE_                      | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE; | 0        | IE        |

    Scenario Outline: EnvironmentConfigParser Methods
        Given an Environment Dictionary
        When the environment variable <key> has a value of <value>
        And the environment variable ROUTER_NAMESPACE_IE_CONNECTION_STRING has a value of Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;
        And the environment variable ROUTER_SOURCE_CONNECTION_STRING has a value of Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;
        Then the EnvironmentConfigParser method <method_name> returns <value>

        Examples:
            | key                                | value                                                                                                                                                                  | method_name           |
            | ROUTER_PROMETHEUS_PORT             | 8042                                                                                                                                                                   | get_prometheus_port   |
            | ROUTER_SOURCE_CONNECTION_STRING    | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;                                                                   | get_source_url        |
            | ROUTER_RULE_COUNTRY_GB             | { "destination_namespaces": "GB", "destination_topics": "gb.topic", "jmespath": "country", "regexp": "^GB$", "source_subscription": "test", "source_topic": "topic.1"} | get_rules             |
            | ROUTER_TIMESTAMP_APP_PROPERTY_NAME | __routed_at                                                                                                                                                            | get_ts_app_prop_name  |

    Scenario Outline: Construct Message ID and Correlation ID
        Given an Environment Dictionary
        When the environment variable <env_key> has a value of <env_value>
        And the message body is <message_body>
        And the message_id is <message_id>
        And the correlation_id is <correlation_id>
        Then the constructed message_id is <expected_message_id> with a correlation_id of <expected_correlation_id>

        Examples:
            | env_key                     | env_value | message_body  | message_id | correlation_id | expected_message_id                  | expected_correlation_id              |
            | ROUTER_ENABLE_DEDUPLICATION | 0         | Hello, world! | None       | None           | None                                 | None                                 |
            | ROUTER_ENABLE_DEDUPLICATION | 0         | Hello, world! | 42         | None           | None                                 | None                                 |
            | ROUTER_ENABLE_DEDUPLICATION | 1         | Hello, world! | 42         | None           | 42                                   | None                                 |
            | ROUTER_ENABLE_DEDUPLICATION | 1         | Hello, world! | None       | None           | 315f5bdb-76d0-78c4-3b8a-c0064e4a0164 | None                                 |
            | SHA256_GUID_CORRELATION_ID  | 1         | Hello, world! | 42         | None           | None                                 | 315f5bdb-76d0-78c4-3b8a-c0064e4a0164 |
