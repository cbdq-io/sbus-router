# Changelog


## Unreleased

### Fix

* Drop async processing and dynamically handle session IDs. [Ben Dalling]

* Correct documented environment variables in README.md. [Ben Dalling]

### Features

* Allow rules to specify that a session is required to receive messages. [Ben Dalling]


## 0.3.1 (2025-03-07)

### Fix

* Separate the sender and receiver namespaces. [Ben Dalling]

* Add a metadata description to the image. [Ben Dalling]

* Ensure netork diagnostic tools are on the image. [Ben Dalling]


## 0.3.0 (2025-03-04)

### Fix

* Migrate from Qpid Proton to Azure Service Bus SDK. [Ben Dalling]

  BREAKING CHANGE: Removed the separate DLQ topic in favour of the
  native DLQ process.


## 0.2.5 (2025-02-05)

### Fix

* More logging refactoring. [Ben Dalling]


## 0.2.4 (2025-01-27)

### Features

* Add the "ROUTER_ALLOWED_SASL_MECHS" environment variable. [Ben Dalling]


## 0.2.3 (2025-01-27)

### Fix

* Refactor logging. [Ben Dalling]


## 0.2.2 (2025-01-06)

### Fix

* Handle non-URL compatible characters in namespace connection strings. [Ben Dalling]

* Stop false negative security alert. [Ben Dalling]


## 0.2.1 (2024-12-31)

### Fix

* Changes to ensure connection to Azure Service Bus. [Ben Dalling]


## 0.2.0 (2024-12-24)

### New

* Add Prometheus metrics. [Ben Dalling]

* Provide a basic script to replay messages from the DLQ. [Ben Dalling]

* Allow environment templating in the rules definitions. [Ben Dalling]


## 0.1.0 (2024-12-09)

### New

* A basic prototype that routes messages according to configured rules. [Ben Dalling]


