Feature: Data Flow
    In order to confirm rules
    As a Service Bus Router
    I want validate topic destination

Scenario Outline: Inject a Message and Confirm the Destination
    Given the landing Service Bus Emulator
    When the landed topic data is <input_data_file> into <input_topic>
    Then read message with the expected ID will be on the <output_topic>

    Examples:
        | input_data_file | input_topic   | output_topic |
        | input-1.json    | topic.1       | gb.topic     |
        # | input-2.json    | topic.2       | ie.topic     |
        # | input-3.json    | topic.1       | gb.topic     |
        # | input-4.json    | topic.2       | ie.topic     |
        # | input-5.json    | topic.1       | N/A          |
        # | input-6.json    | topic.2       | dlq          |
