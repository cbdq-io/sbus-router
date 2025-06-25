# Changelog


## 0.7.0 (2025-06-25)

### Features

* Add a Prometheus Counter for each rule as it is matched. [Ben Dalling]

### Build

* Release/0.7.0. [Ben Dalling]

### Continuous Integration

* Updated periodic-trivy-scan.yml with github.repository variable. [James Loughlin]


## 0.6.1 (2025-06-11)

### Fix

* Ensure more than 100 messages can be removed from dead letter messages. [Ben Dalling]

### Build

* Hotfix/0.6.1. [Ben Dalling]


## 0.6.0 (2025-06-11)

### Features

* Add a nukedlq.py script to the image. [Ben Dalling]

### Build

* Release/0.6.0. [Ben Dalling]

### Continuous Integration

* Add a Trivy scan to the CI pipeline. [Ben Dalling]

* Update the periodic Trivy scan. [Ben Dalling]

### Documentation

* Fix formatting errors in README.md and containerise change log. [Ben Dalling]


## 0.5.3 (2025-05-19)

### Fix

* Bump the prefetch count from 20 to 100 and allow it to be configured. [Ben Dalling]

* Pre-compile JMESPath expressions. [Ben Dalling]


## 0.5.2 (2025-05-16)

### Fix

* If a message is sent do the DLQ, do not include the message body in the custom properties. [Ben Dalling]


## 0.5.1 (2025-05-13)

### Fix

* Correct the container image description. [Ben Dalling]

* Make the get_sender method of the handler thread safe. [Ben Dalling]

* Remove python-qpid-proton from the artefact. [Ben Dalling]

### Continuous Integration

* On push to develop, publish a "latest" artefact. [Ben Dalling]

* Periodically run Trivy against the latest released container image. [Ben Dalling]


## 0.5.0 (2025-03-17)

### Fix

* Add health check to the container image. [Ben Dalling]

* Implement suggestions of Sam M of PremFina. [Ben Dalling]


## 0.4.0 (2025-03-16)

### Features

* Add the ability to set a hook for a custom sender. [Ben Dalling]

### Fix

* Ensure message properties are preserved during routing. [Ben Dalling]

* Ensure receivers are created for unique topic/subscription combinations. [Ben Dalling]


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

* Docker Compose environment variable. [Ben Dalling]


## 0.2.5 (2025-02-05)

### Fix

* More logging refactoring. [Ben Dalling]


## 0.2.4 (2025-01-27)

### Features

* Add the "ROUTER_ALLOWED_SASL_MECHS" environment variable. [Ben Dalling]

### Fix

* Bump release number. [Ben Dalling]

* Migrate Git Change Log to the Conventianal Commit standard (II). [Ben Dalling]

* Ensure full pipeline skips on push of tag. [Ben Dalling]

* Migrate Git Change Log to the Conventianal Commit standard. [Ben Dalling]

* Better readiness tests that remove the need for random sleeps. [Ben Dalling]

* Add a healthcheck to the sqledge container. [Ben Dalling]


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


