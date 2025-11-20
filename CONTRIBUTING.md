# Contributing Guide

Thank you for taking your time in contributing to this project. Any
contribution made to the code will be acknowledged in our
[change log](CHANGELOG.md). Please read this guide fully before
submitting any changes or issues to the project.

## Table of Contents

1. [Getting Started](#getting-started)
1. [Branching Model](#branching-model)
1. [Submitting Changes](#submitting-changes)
1. [Code Review](#code-review)
1. [Guidelines](#guidelines)
   - [Commit Messages](#commit-messages)
1. [Issues and Bugs](#issues-and-bugs)
1. [Documentation](#documentation)
1. [Maintainers Only](#maintainers-only)
   - [Cutting a New Release](#cutting-a-new-release)
1. [Using SonarQube Locally](#using-sonarqube-locally)

## Getting Started

We carry out development in the Visual Code Studio
[Dev Container](https://code.visualstudio.com/docs/devcontainers/containers)
environment.

1. Fork the repository from <https://github.com/cbdq-io/kc-connectors/fork>.
1. Clone your fork (using either HTTPS or SSH):

   ```shell
   # If using HTTPS:
   git clone https://github.com/YOUR_USERNAME/kc-connectors.git

   # If using SSH:
   git clone git@github.com:YOUR_USERNAME/kc-connectors.git

   ```

1. Setup the Upstream Repository

   ```shell
   cd kc-connectors

   # If using HTTP
   git remote add upstream https://github.com/cbdq-io/kc-connectors.git


   # If using SSH
   git remote add upstream git@github.com:cbdq-io/kc-connectors.git

   ```

1. Open in Visual Code Studio
   Assuming you have the Dev Container extension installed and configured,

   ```shell
   make
   ```

## Branching Model

We use the
[Gitflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow)
branching model. Here's the summary:

1. **Main branches**:
   - `main`: The stable production branch.
   - `develop`: The integration branch for new features and bugfixes.
2. **Supported branches**:

   - `feature/*`: For developing new features. Branch off from `develop`.
   - `bugfix/*`: For developing new bug fixes. Branch off from `develop`.
   - `release/*`: For preparing a new release. Branch off from `develop`.
     Used by maintainers only.
   - `hotfix/*`: For urgent fixes on the production code. Branch off
     from `main`. Used by maintainers only.

   If we need to support legacy versions, then there are also

   `support/*` branches as well.

### Creating a Branch

1. Feature Branch:

   ```shell

   git checkout develop
   git pull upstream develop

   git checkout -b feature/your-feature-name
   ```

1. Bugfix Branch:

   ```shell

   git checkout develop

   git pull upstream develop
   git checkout -b bugfix/your-bugfix-name
   ```

## Submitting Changes

1. Please ensure that your commit messages follow our
   [commit message](#commit-messages) guidelines.

   ```bash
   git add .
   ```

2. Push your branch:

   ```shell

   # For a feature branch.
   git push origin feature/your-feature-name

   # For a bugfix branch.
   git push origin bugfix/your-bugfix-name
   ```

3. Create a Pull Request:
   - Navigate to your forked repository on GitHub.
   - Click the “Compare & pull request” button.
   - Select develop as the base branch and your branch as the compare branch.
   - Provide a clear description of your changes.

## Code Review

- Your pull request will be reviewed by the maintainers.
- Address any feedback and make necessary changes.
- Once approved, your changes will be merged.

## Guidelines
- Follow the coding standards established in the project.
- Write meaningful commit messages. More on that below.
- Write tests for new features or changes.

### Commit Messages

[Change logs](CHANGELOG.md) are useful, but it's a schlep maintaining them.
To reduce the pain, we use the
[Git Change Log](https://github.com/cbdq-io/gitchangelog).  Please follow the
guidelines on that repository to create well-crafted a meaningful commit
messages that will then be included in the change log.

## Issues and Bugs

- Check the existing
  [issues](https://github.com/cbdq-io/kc-routers/issues) before
  submitting a new one.
- If you find a bug, please open an issue with a clear description and steps to reproduce.

## Documentation

- Update the documentation for any new features or changes you make.
- Ensure that the documentation is clear and concise.

## Maintainers Only

### Cutting a New Release

1. Creating a Release Branch:

   ```shell
   git checkout develop
   git pull
   git checkout -b release/your-release-name
   ```

   Now edit `router.py` and ensure that `__version__` is set to the same
   as the proposed release name.

2. Run the local end-to-end tests:

   ```shell
   make
   ```

   Only progress to the next step when all tests pass.
3. Push the release branch:


   ```shell
   git push origin release/your-release-name
   ```

4. Create a pull request to merge the release branch onto `main`.
