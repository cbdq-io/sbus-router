# sbus-router

A configurable router for Azure Service Bus

## Configuration

All configuration is set in environment variables.  There must also be at
least one rule configured  (see below).

| Environment Variable | Required | Default | Description |
| -------------------- | -------- | ------- | ----------- |
| LOG_LEVEL | | WARN | The log level for the router.|
| ROUTER_SOURCE_CONNECTION_STRING | Yes | | The connection string for the source Service Bus namespace. |

## Rules

Rules are configured as JSON, the format of which must match the schema
provided in `rule-schema.json`.  Please note that in the JSON, one can
input fields in the format `$ENV_VAR` indicating that the field
name `ENV_VAR` is to be replaced with an environment variable of that
name.  For example configuration, see the **router** service in
[tests/resources/docker-compose.yaml](tests/resources/docker-compose.yaml).

The configurable fields for rules are:

- `destination_namespaces`: The namespaces (comma separated) to which data
  will be sent.  If comma separated, they must match the destination_topics
  definition.
- `destination_topics`: The topics (comma separated) to which data will be
  sent.  If this is blank ("") then messages that match the rule will be
  considered valid, not be produced onto the DLQ, but will be dropped.
  Topics can be comma separated which means that the messages that match
  the rule will be sent to each of the topics.
- `jmespath`: A [JMESPath](https://jmespath.org/) expression to query an
  element within the JSON contained in the message.
- `regexp`: A
  [regular expression](https://en.wikipedia.org/wiki/Regular_expression)
  that will me used to match against the data returned from `jmespath`.
- `requires_session`: Should sessions be enabled for the source/destination
  topics.  Defaults to false.
- `source_subscription`: The subscription from which data will be retrieved.
- `source_topic`: The topic from which data will be retrieved.

Please note that environment variables containing rules must be prefixed with
`ROUTER_RULE_` (e.g. `ROUTER_RULE_COUNTRY_GB`) and matching against the rules
will be done one by one in the alphabetical order of the environment variable
name.

## Destination Namespaces

Destination namespaces are defined with a prefix of `ROUTER_NAMESPACE_` and
a suffix of `_CONNECTION_STRING`.  For example, if an environment variable
called `ROUTER_NAMESPACE_IE_CONNECTION_STRING` is set, then a destination
namespace of `IE` can be referred to in the rules.
