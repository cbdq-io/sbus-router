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
        | input_data_file | input_topic   | output_topic      | correlation_id                       |
        | input-6.json    | topic.2       | DLQ               | N/A                                  |
        | input-1.json    | topic.1       | gb.topic          | 70b4a46f-29b6-0dd4-36c2-d32424cef898 |
        | input-6.json    | topic.1       | DLQ               | N/A                                  |
        | input-2.json    | topic.2       | ie.topic          | 1dbdb677-ddd8-f35e-9d85-3c7bde1c083f |
        | input-3.json    | topic.1       | gb.topic          | 6ac905d2-7c3f-cd70-cbf8-5587b156080e |
        | input-4.json    | topic.2       | ie.topic          | ff5568fe-989c-aa0b-d0a3-7eabac8c3d52 |
        | input-5.json    | topic.1       | N/A               | N/A                                  |
        | input-8.json    | topic.2       | gb.topic,ie.topic | 2f723eea-2558-c53f-b2f8-5de510b76220 |

Scenario: Replay DLQ Message
    Given the landing Service Bus Emulator
    Then the DLQ count is 2
    Then the deleted DLQ messages is 1
