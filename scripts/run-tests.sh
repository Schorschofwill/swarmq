#!/usr/bin/env bash
# Startet pytest fuer eine Suite (Plan 2026-10-13-0902, U7).
#
# Aufruf: run-tests.sh [--no-services] <pytest-Argumente>. Ohne
# --no-services laeuft pytest unter der Testsperre (with-test-lock.sh).
#
# Der Seed fuer pytest-randomly wird immer ausdruecklich gesetzt und
# ausgegeben: pytest-randomly selbst zeigt ihn mit -q nicht, und ein roter
# Lauf liesse sich sonst nicht nachstellen. Vorgabe per SWARMQ_TEST_SEED.
# Mit SWARMQ_TEST_COVERAGE=1 laeuft pytest unter ``coverage run``.
set -euo pipefail

lock=("$(dirname "$0")/with-test-lock.sh")
if [[ "${1:-}" == "--no-services" ]]; then
    lock=()
    shift
fi

seed="${SWARMQ_TEST_SEED:-$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')}"
echo "pytest-randomly: Seed ${seed}; nachstellen mit SWARMQ_TEST_SEED=${seed}" >&2
runner=(pytest)
if [[ "${SWARMQ_TEST_COVERAGE:-}" == "1" ]]; then
    runner=(coverage run -m pytest)
fi
exec "${lock[@]}" uv run --extra test --extra reload "${runner[@]}" "$@" "--randomly-seed=${seed}"
