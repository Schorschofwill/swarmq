# Security Policy

## Supported versions

SwarmQ is in the 0.x series. Security fixes go into the latest release line
only; there are no backports to older minor versions. Upgrade to the newest
release on [PyPI](https://pypi.org/project/swarmq/) before reporting, if you can.

## Reporting a vulnerability

**Please do not open a public issue, pull request or discussion for a
security problem.**

Report it privately through GitHub's
[private vulnerability reporting](https://github.com/Schorschofwill/swarmq/security/advisories/new)
(repository tab *Security* → *Report a vulnerability*). Only the maintainer
can see the report.

Helpful details:

- the affected swarmq version, Python version, and RabbitMQ and Valkey versions
- what an attacker needs (network access to the broker, ability to publish
  messages, control over task arguments, …) and what they gain
- a minimal reproduction or proof of concept
- any idea for a fix you already have

Problems in RabbitMQ, Valkey or a third-party library belong to that project.
If swarmq's use of it makes the problem worse or exposes it, report it here
as well.

## What happens next

SwarmQ is maintained by one person, so these are goals, not guarantees:

- acknowledgement within 7 days
- a first assessment (accepted, needs more information, or not a
  vulnerability) within 14 days
- a fix released as soon as it is ready and tested; the timeline depends on
  the severity and is agreed with you

Accepted reports are published as a
[GitHub security advisory](https://github.com/Schorschofwill/swarmq/security/advisories)
together with the fixed release, with a CVE where it applies, and noted in the
[changelog](website/reference/changelog.md). You are credited in the advisory
unless you prefer not to be.

## Verifying a release

Every release after 0.15.1 is built on GitHub-hosted runners and signed
through Sigstore:

- PyPI shows the publish attestations (PEP 740) on each file's page.
- Each wheel and sdist has a GitHub build provenance and an SBOM
  attestation. Download a file from PyPI or from the
  [GitHub release](https://github.com/Schorschofwill/swarmq/releases) and run

  ```bash
  gh attestation verify swarmq-*.whl -R Schorschofwill/swarmq
  ```

- The GitHub release carries the SBOM (`swarmq-<version>.cdx.json`,
  CycloneDX) with the runtime dependency versions the release was tested
  against.
