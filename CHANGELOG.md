# Changelog


## Unreleased

### Features

* Feat: Add the "ROUTER_ALLOWED_SASL_MECHS" environment variable. [Ben Dalling]


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


