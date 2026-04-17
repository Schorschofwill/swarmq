#!/usr/bin/env bash
# Mutation testing runner — uses mutmut against src/swarmq/, scoring
# mutants on the unit-test suite. Wired into the Weekly CI job in U4.3.
set -euo pipefail
cd "$(dirname "$0")/.."
uv run --extra test mutmut run --paths-to-mutate src/swarmq/
uv run --extra test mutmut results
