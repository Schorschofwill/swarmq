#!/usr/bin/env bash
# Startet pytest fuer eine Suite (Plan 2026-10-13-0902, U7).
#
# Aufruf: run-tests.sh [--no-services | --parallel N] [--suite PFAD] <pytest-Argumente>.
# Ohne --no-services laeuft alles unter der Testsperre (with-test-lock.sh).
#
# --suite PFAD (Plan 2026-10-15-1138, U16): Standardpfad der Suite. Er
# entfaellt, sobald ein pytest-Argument ein existierender Pfad ist (auch als
# Node-ID ``datei.py::test``); Optionen und ihre Werte (``-k tests``) zaehlen
# nicht.
#
# --parallel N (Beschleunigung Stufe 3): erst ein Durchgang auf N
# xdist-Prozessen (--dist loadgroup) ohne die exclusive-Tests, dann ein
# serieller nur mit ihnen (tests/support/xdist_isolation.py); beide unter
# derselben Sperre.
#
# Der Seed fuer pytest-randomly wird immer ausdruecklich gesetzt und
# ausgegeben: pytest-randomly selbst zeigt ihn mit -q nicht, und ein roter
# Lauf liesse sich sonst nicht nachstellen. Vorgabe per SWARMQ_TEST_SEED.
# Mit SWARMQ_TEST_COVERAGE=1 laeuft pytest unter ``coverage run``.
set -euo pipefail

original=("$@")
services=1
workers=""
if [[ "${1:-}" == "--no-services" ]]; then
    services=""
    shift
elif [[ "${1:-}" == "--parallel" ]]; then
    workers="$2"
    shift 2
fi
suite=()
if [[ "${1:-}" == "--suite" ]]; then
    suite=("$2")
    shift 2
fi

has_path_arg() {
    local previous="" arg
    for arg in "$@"; do
        # Wert einer Option mit getrenntem Argument, etwa ``-k tests``
        case "$previous" in
            -k | -m | -p | -o | -c | -W | -n | --dist | --deselect | --ignore | --ignore-glob | --rootdir \
                | --confcutdir | --basetemp | --junitxml | --junit-xml | --maxfail | --tb | --durations \
                | --timeout | --randomly-seed | --log-level | --log-file)
                previous=""
                continue
                ;;
        esac
        previous="$arg"
        [[ "$arg" == -* ]] && continue
        [[ -e "${arg%%::*}" ]] && return 0
    done
    return 1
}

if [[ -n "$services" && -z "${CI:-}" && -z "${SWARMQ_TEST_LOCK_HELD:-}" ]]; then
    # with-test-lock.sh setzt SWARMQ_TEST_LOCK_HELD; der zweite Aufruf sperrt nicht erneut.
    exec "$(dirname "$0")/with-test-lock.sh" "$0" "${original[@]}"
fi

seed="${SWARMQ_TEST_SEED:-$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')}"
echo "pytest-randomly: Seed ${seed}; nachstellen mit SWARMQ_TEST_SEED=${seed}" >&2
runner=(pytest)
if [[ "${SWARMQ_TEST_COVERAGE:-}" == "1" ]]; then
    runner=(coverage run -m pytest)
fi

run() {
    uv run --extra test --extra reload "${runner[@]}" "$@" "--randomly-seed=${seed}"
}

if has_path_arg "$@"; then suite=(); fi
set -- "${suite[@]}" "$@"
if [[ -z "$workers" ]]; then
    run "$@"
    exit
fi

# Der serielle Durchgang schreibt seine junit-Datei daneben (x.xml -> x-serial.xml).
serial=()
previous=""
for arg in "$@"; do
    case "$previous|$arg" in
        *"|--junitxml="*.xml | *"|--junit-xml="*.xml | --junitxml\|*.xml | --junit-xml\|*.xml)
            serial+=("${arg%.xml}-serial.xml") ;;
        *) serial+=("$arg") ;;
    esac
    previous="$arg"
done

parallel_rc=0
SWARMQ_TEST_EXCLUSIVE=skip run -n "$workers" --dist loadgroup "$@" || parallel_rc=$?
serial_rc=0
SWARMQ_TEST_EXCLUSIVE=only run "${serial[@]}" || serial_rc=$?
# Exit 5 (keine Tests ausgewaehlt) ist normal fuer einen der Durchgaenge, etwa
# ohne exclusive-Tests oder mit -k; nur zwei leere Durchgaenge sind ein Fehler.
for rc in "$parallel_rc" "$serial_rc"; do
    if [[ $rc -ne 0 && $rc -ne 5 ]]; then exit "$rc"; fi
done
if [[ $parallel_rc -eq 5 && $serial_rc -eq 5 ]]; then exit 5; fi
exit 0
