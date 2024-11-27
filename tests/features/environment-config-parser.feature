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
