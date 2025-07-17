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
    Then the expected output message is received

    Examples:
        | input_data_file | input_topic   | output_topic |
        | input-6.json    | topic.2       | DLQ          |
        | input-1.json    | topic.1       | gb.topic     |
        | input-6.json    | topic.1       | DLQ          |
        | input-2.json    | topic.2       | ie.topic     |
        | input-3.json    | topic.1       | gb.topic     |
        | input-4.json    | topic.2       | ie.topic     |
        | input-5.json    | topic.1       | N/A          |

Scenario: Replay DLQ Message
    Given the landing Service Bus Emulator
    Then the DLQ count is 2
    Then the deleted DLQ messages is 1
