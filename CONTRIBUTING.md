# Contributing to SwarmQ

Thanks for your interest in SwarmQ. Bug reports, documentation fixes and
pull requests are welcome. This page explains how the project works so your
contribution lands smoothly.

By taking part you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
Security problems go through the private channel in [SECURITY.md](SECURITY.md),
never through a public issue.

## Before you start

- **Bugs:** open an issue with the bug report form. A minimal reproduction
  helps more than anything else.
- **Features and larger changes:** open an issue first and describe the
  problem you want to solve. SwarmQ keeps a deliberately small core, and many
  design choices are settled on purpose. They are explained in
  [Design decisions](website/explanation/design-decisions.md) and recorded in
  the [decision register](compound-packs/swarmq-entscheidungen/) (in German).
  A change that reverses one of them needs a discussion before code.
- **Small fixes** (typos, docs, an obvious one-line bug) can go straight to a
  pull request.

## Development setup

You need Python 3.14, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
git clone https://github.com/Schorschofwill/swarmq.git
cd swarmq
uv sync --all-extras
uv run pre-commit install    # format, lint and type checks before each commit
uv run just                  # list all recipes
```

### Test services

Integration and end-to-end tests need RabbitMQ and Valkey. They run against
dedicated test ports, **not** the defaults 5672 and 6379: the test fixtures
flush Valkey, and they refuse to start against a default port so they never
wipe a local instance you use for something else.

```bash
docker run -d --name swarmq-test-rabbit -p 5699:5672 -p 15699:15672 \
  rabbitmq:4.3.6-management
docker run -d --name swarmq-test-valkey -p 6399:6379 valkey/valkey:8
```

A few characterisation tests also need RabbitMQ 4.2 on port 5742; without it
they are skipped. See [`docker-compose.yml`](docker-compose.yml) for its
settings.

### Running checks

```bash
uv run just unit         # unit tests, no services needed
uv run just integration  # against RabbitMQ and Valkey
uv run just e2e          # full stack: client, worker, RabbitMQ, Valkey
uv run just all          # every suite the CI runs
uv run just lint         # ruff format and ruff check
uv run just types        # mypy, basedpyright, stubtest
```

The CI runs the same recipes. The test strategy, including the opt-in suites
(property, fuzz, chaos, stress), is described in
[`docs/testing/test-strategy.md`](docs/testing/test-strategy.md) (in German).

## How changes are made

- **Test first.** Every behaviour change starts with a failing test, then the
  smallest code that makes it pass, then a refactor. A bug fix starts with a
  test that reproduces the bug. The full rules are in [`AGENTS.md`](AGENTS.md)
  (in German).
- **Coverage.** The CI enforces a total coverage floor (`fail_under` in
  `pyproject.toml`) and checks the lines your change touches. Untested code
  will not pass.
- **Changelog.** Add an entry under `## [Unreleased]` in
  [`website/reference/changelog.md`](website/reference/changelog.md) for every
  user-visible change, following [Keep a Changelog](https://keepachangelog.com/).
- **Docs.** The documentation in `website/` exists in English (`*.md`) and
  German (`*.de.md`). Update the English page; a German translation is welcome
  but not required, the maintainer can add it.
- **Commit messages** follow the
  [Conventional Commits](https://www.conventionalcommits.org/) style, for example
  `fix(worker): …`, `feat: …`, `docs: …`. English or German are both fine.

## Pull requests

- Keep a pull request to one topic.
- Fill in the pull request template; link the issue it resolves.
- All CI checks must be green. The CI runs on the maintainer's own runners;
  for a first-time or outside contributor it starts once a maintainer has
  approved the run.
- Pull requests are squash-merged.

## License

SwarmQ is licensed under the [MIT License](LICENSE). By submitting a
contribution you agree that it is licensed under the same terms.
