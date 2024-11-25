Feature: Data Flow
    In order to confirm rules
    As a Service Bus Router
    I want validate topic destination

Scenario Outline: Inject a Message and Confirm the Destination
    Given the landing Service Bus Emulator
    When the landed topic data is <input_data_file> into <topic>

    Examples:
        | input_data_file | topic   |
        | input-1.json    | topic.1 |
        | input-2.json    | topic.1 |
        | input-3.json    | topic.1 |
        | input-4.json    | topic.1 |
        | input-5.json    | topic.1 |
        | input-6.json    | topic.1 |
