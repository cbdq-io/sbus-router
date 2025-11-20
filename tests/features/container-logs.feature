Feature: Test the Router Container Logs
    Scenario: Container Logs
        Given the TestInfra host with URL "local://" is ready
        When the TestInfra command is "docker compose logs router"
        Then the TestInfra command stdout contains "No rules match message from topic.1, sending to the DLQ."
        And the TestInfra command stdout contains "Checking DLQ for topic.1/test"
