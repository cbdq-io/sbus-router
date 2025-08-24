Feature: Test the Container
    Scenario Outline: System Under Test
        Given the TestInfra host with URL "docker://router" is ready
        When the TestInfra file is <path>
        Then the TestInfra file is present
        And the TestInfra file type is file
        And the TestInfra file owner is appuser
        And the TestInfra file group is appuser

        Examples:
            | path                           |
            | /home/appuser/nukedlq.py       |
            | /home/appuser/rule-schema.json |
            | /home/appuser/router.py        |

    Scenario Outline: Python Packages
        Given the TestInfra host with URL "docker://router" is ready
        When the TestInfra pip package is <pip_package>
        Then the TestInfra pip package is present

        Examples:
            | pip_package      |
            | azure-servicebus |

    Scenario Outline: Commands on the Path
        Given the TestInfra host with URL "docker://router" is ready
        Then the TestInfra command "<command>" exists in path

        Examples:
            | command  |
            | nc       |
            | nslookup |
            | python   |
