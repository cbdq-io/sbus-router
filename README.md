# sbus-router

A configurable router for Azure Service Bus

## Configuration

All configuration is set in environment variables.  There must also be at
least one rule configured  (see below).

| Environment Variable | Required | Default | Description |
| -------------------- | -------- | ------- | ----------- |
| LOG_FORMAT | No | "%(levelname)s [%(filename)s:%(lineno)d] %(message)s" | The log format (passed to `logging.basicConfig) |
| LOG_LEVEL | | WARN | The log level for the router.|
| ROUTER_CUSTOM_SENDER | No | N/A | See below. |
| ROUTER_MAX_TASKS | No | 1 | The number of tasks to allocate to each topic/subscription. |
| ROUTER_PREFETCH_COUNT | No | 100 | The maximum number of messages to cache with each request to the service. |
| ROUTER_PROMETHEUS_PORT | No | 8000 | The port for Prometheus to start on. |
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
- `max_auto_renew_duration`: The time in seconds to allow messags for this
  topic to be locked for (default: 300).
- `is_session_required`: Does the source subscription require sessions?
- `jmespath`: A [JMESPath](https://jmespath.org/) expression to query an
  element within the JSON contained in the message.
- `regexp`: A
  [regular expression](https://en.wikipedia.org/wiki/Regular_expression)
  that will me used to match against the data returned from `jmespath`.
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

## Using a Custom Transformer

The environment variable `ROUTER_CUSTOM_TRANSFORMER` can be set to configure a
custom transformer.  `ROUTER_CUSTOM_TRANSFORMER` must be a colon (`:`)
separated value of two items where the first item is the path to a Python
script and the second is the name of a function in the Python script to be
called (e.g. `custom:transorm` will call a function called
`transorm` in a script called `custom.py`).  The function MUST be defined
to accept the following arguments:

| Type                                   | Description                                             |
| -------------------------------------- | ------------------------------------------------------- |
| azure.servicebus.aio.ServiceBusMessage | The message to be transformed.                          |
| str                                    | The name of the destination topic.                      |
| logging.Logger                         | A logger in case one wants to track the transformation. |

An example custom transformer is implemented in the file
`tests/resources/custom.py`.  Please note that the trasnformer function MUST
not incur any I/O.  It also MUST return an azure.servicebus.ServiceBusMessage

## Useful External Links

The followng links can assist in crafting rules and regular expressions:

- https://gchq.github.io/CyberChef
- https://pythex.org
- https://play.jmespath.org

## Breaking Changes

The ability to configure a custom sender hook (introduced in version 0.4.0) has
been removed in version 0.10.0.  It has been replaced with the custom transform
hook.
