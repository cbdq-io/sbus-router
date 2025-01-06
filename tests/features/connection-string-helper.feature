@unit
Feature: Connection String Helper
    In order to connect to Azure Service Bus
    As a developer
    I want my connection string to be validated.

  Scenario Outline: Valid Connection Strings
    Given Azure Service Bus Connection String <sbus_connection_string>
    When the Azure Service Bus Connection String is parsed
    Then the AMQP URL is <amqp_url>
    And key_name is <key_name>
    And key_value is <key_value>
    And netloc is <netloc>

    Examples:
      | sbus_connection_string                                                                                                           | amqp_url                                                        | key_name                  | key_value      | netloc                 |
      | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;                             | amqps://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost:5671  | RootManageSharedAccessKey | SAS_KEY_VALUE  | amqps://localhost:5671 |
      | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE=;                            | amqps://RootManageSharedAccessKey:SAS_KEY_VALUE=@localhost:5671 | RootManageSharedAccessKey | SAS_KEY_VALUE= | amqps://localhost:5671 |
      | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=A+A+/aa=                                   | amqps://RootManageSharedAccessKey:A+A+/aa=@localhost:5671       | RootManageSharedAccessKey | A+A+/aa=       | amqps://localhost:5671 |
      | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=A+A+/aa=;                                  | amqps://RootManageSharedAccessKey:A+A+/aa=@localhost:5671       | RootManageSharedAccessKey | A+A+/aa=       | amqps://localhost:5671 |
      | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=A+A+/aa=;UseDevelopmentEmulator=true;      | amqp://RootManageSharedAccessKey:A+A+/aa=@localhost:5672        | RootManageSharedAccessKey | A+A+/aa=       | amqp://localhost:5672  |
      | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true; | amqp://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost:5672   | RootManageSharedAccessKey | SAS_KEY_VALUE  | amqp://localhost:5672  |

  Scenario Outline: Invalid Connection Strings
    Given Azure Service Bus Connection String <sbus_connection_string>
    When the Azure Service Bus Connection String is parsed
    Then the invalid connection string raised a ValueError

    Examples:
      | sbus_connection_string                                                       |
      | SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE; |
      | Endpoint=sb://localhost;SharedAccessKey=SAS_KEY_VALUE;                       |
      | Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;       |
