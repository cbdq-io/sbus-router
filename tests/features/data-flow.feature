@system
Feature: Data Flow
    In order to confirm rules
    As a Service Bus Router
    I want validate topic destination

Scenario Outline: Inject a Message and Confirm the Destination
    Given the landing Service Bus Emulator
    And the message contents is <input_data_file>
    And the input topic is <input_topic>
    And the output topic is <output_topic>
    When the input message is sent
    Then the expected output message is received with correlation ID <correlation_id>

    Examples:
        | input_data_file | input_topic   | output_topic      | correlation_id |
        | input-6.json    | topic.2       | DLQ               | N/A            |
        | input-1.json    | topic.1       | gb.topic          | 42             |
        | input-6.json    | topic.1       | DLQ               | N/A            |
        | input-2.json    | topic.2       | ie.topic          | 42             |
        | input-3.json    | topic.1       | gb.topic          | 42             |
        | input-4.json    | topic.2       | ie.topic          | 42             |
        | input-5.json    | topic.1       | N/A               | N/A            |
        | input-8.json    | topic.2       | gb.topic,ie.topic | 42             |

Scenario: Replay DLQ Message
    Given the landing Service Bus Emulator
    Then the DLQ count is 2
    Then the deleted DLQ messages is 1
