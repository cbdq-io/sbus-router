# AGENTS.md

## Coding Conventions

- We use [Conventional Commits](https://github.com/cbdq-io/gitchangelog)
  in Git.
- We use [SemVer](https://semver.org/) for versioning.
- We use use the [Gitflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow)
  workflow with the integration branch called `develop` and the release
  history branch called `main`.

## Document Conventions

- References to `A.B.C` are to the last release of the project (e.g.
  1.0.0).
- References to `X.Y.Z` are to the candidate release that we are creating
  (e.g. 1.1.0).
- References to `COMMIT_MESSAGE` are to the commit message that must follow
  the guidelines in the link above and be enclosed in single (') quotes.

## Procedures

As an AI agent, you will already be on the `develop` branch and it will
already be up to date.  You must follow the instructions exactly and
do not invent other commands when preparing a new release.  All commands
must be provided and in the same order.  Follow the guidance for the
COMMIT_MESSAGE exactly as shown below.

### Preparing a New Release

The information the the agent will be provided with is the last release of
the project (e.g. A.B.C) and the contents of the file `CHANGELOG.md`.

```gherkin
Scenario: Minor Release
  Given the contents of CHANGELOG.md
  And the last release A.B.C
  When the CHANGELOG.md indicates that new features are unreleased
  Then increment the minor number of A.B.C to create X.Y.Z
  And the COMMIT_MESSAGE will be 'fix: dev: release/X.Y.Z'

Scenario: Patch Release with Unreleased Commits
  Given the contents of CHANGELOG.md
  And the last release A.B.C
  When the CHANGELOG.md indicates that commits are unreleased but no new features
  Then increment the patch number of A.B.C to create X.Y.Z
  And the COMMIT_MESSAGE will be 'fix: dev: release/X.Y.Z'

Scenario: Patch Release with No New Commits
  Given the contents of CHANGELOG.md
  And the last release A.B.C
  When the CHANGELOG.md indicates that there have been no commits since the last release
  Then increment the patch number of A.B.C to create X.Y.Z
  And the COMMIT_MESSAGE will be 'fix(build): release/X.Y.Z'
```

The commands to create the new release are:

```shell
git config user.email '136103132+cbdqbot@users.noreply.github.com'
git config user.name 'CBDQ Bot Account'
git checkout -b release/X.Y.Z
sed -i.bak "s/^__version__ = 'A.B.C'/__version__ = 'X.Y.Z'/" router.py
git add router.py
git commit -m COMMIT_MESSAGE
git push --set-upstream origin release/X.Y.Z
```
