"""Golden-file test for the ``swarmq`` CLI help output.

Captures ``--help`` text for every subcommand and argparse path that
users see. Guards against byte-level drift introduced by the
``cli.py`` → ``cli/`` package split (plan 2026-04-22-001), where the
argparse parser is moved into ``cli/parser.py``.

Fixtures live in ``tests/unit/fixtures/cli_help/*.txt``. Regenerate
(after a deliberate UX change) via the same code path the test
exercises (``_build_parser().parse_args([..., '--help'])``) so the
fixture and the assertion cannot drift::

    uv run python - <<'PY'
    from pathlib import Path
    from tests.unit.test_cli_help_output import _HELP_CASES, _capture_help
    out = Path("tests/unit/fixtures/cli_help")
    out.mkdir(parents=True, exist_ok=True)
    for argv_prefix, fname in _HELP_CASES:
        (out / fname).write_text(_capture_help(argv_prefix))
    PY
"""

from __future__ import annotations

import io
import re
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

from swarmq.cli import _build_parser

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "cli_help"

# Maps argparse invocation (tuple of CLI tokens) to fixture filename.
_HELP_CASES: tuple[tuple[tuple[str, ...], str], ...] = (
    ((), "root.txt"),
    (("worker",), "worker.txt"),
    (("dlq",), "dlq.txt"),
    (("dlq", "list"), "dlq_list.txt"),
    (("dlq", "inspect"), "dlq_inspect.txt"),
    (("dlq", "retry"), "dlq_retry.txt"),
    (("dlq", "purge"), "dlq_purge.txt"),
    (("schedule",), "schedule.txt"),
    (("schedule", "list"), "schedule_list.txt"),
    (("schedule", "add"), "schedule_add.txt"),
    (("schedule", "remove"), "schedule_remove.txt"),
    (("schedule", "status"), "schedule_status.txt"),
    (("schedule", "prune"), "schedule_prune.txt"),
    (("inspect",), "inspect.txt"),
)


def _capture_help(argv_prefix: tuple[str, ...]) -> str:
    """Return the exact text argparse prints for ``<prefix> --help``.

    argparse calls ``parser.print_help()`` then ``SystemExit(0)``. We
    catch the exit and return the captured stdout.
    """
    parser = _build_parser()
    buf_out = io.StringIO()
    buf_err = io.StringIO()
    with redirect_stdout(buf_out), redirect_stderr(buf_err), pytest.raises(SystemExit) as excinfo:
        parser.parse_args([*argv_prefix, "--help"])
    assert excinfo.value.code == 0, (
        f"--help for {argv_prefix!r} exited with {excinfo.value.code}, stderr={buf_err.getvalue()!r}"
    )
    return buf_out.getvalue()


@pytest.mark.parametrize(("argv_prefix", "fixture_name"), _HELP_CASES)
def test_help_output_matches_fixture(
    argv_prefix: tuple[str, ...],
    fixture_name: str,
) -> None:
    fixture_path = _FIXTURE_DIR / fixture_name
    expected = fixture_path.read_text()
    actual = _capture_help(argv_prefix)
    assert actual == expected, (
        f"Help output for `swarmq {' '.join(argv_prefix)} --help` drifted "
        f"from fixture {fixture_name}. If the change is intentional, "
        f"regenerate fixtures per the docstring of this test file."
    )


def test_worker_help_describes_current_supervisor_reload_and_check_semantics() -> None:
    """Die Hilfetexte folgen dem Verhalten (Release 0.8.0): der Supervisor
    laeuft immer, ein fehlendes ``watchfiles`` endet mit Exit 78 (1 nur
    unter ``--check``), und ``--check`` erlaubt feste Basis-Ports im
    Mehrprozessbetrieb, prueft aber Port-Bereiche, Logging und Pool."""
    # argparse bricht auch an Bindestrichen um ("in-\nprocess"): Whitespace
    # zusammenfassen und den Umbruch nach einem Wort-Bindestrich entfernen.
    text = re.sub(r"(?<=\w-) ", "", " ".join(_capture_help(("worker",)).split()))

    # --processes: kein "legacy in-process"-Modus mehr.
    assert "legacy in-process" not in text
    assert "Default 1 still runs under the supervisor." in text
    # --reload: Supervisor ist immer aktiv, Exit-Code 78.
    assert "activates the supervisor when" not in text
    assert "Missing dep exits with code 1;" not in text
    assert "Missing 'watchfiles' exits with code 78 (1 under --check)" in text
    # --check: Basis-Ports sind erlaubt; beschrieben wird, was geprueft wird.
    assert "multi-process mode is not combined" not in text
    assert "port ranges" in text and "65535" in text
    assert "--log-config" in text and "--max-processes" in text
    # Neustart per execvpe mit dem sys.orig_argv-Schnappschuss, nicht execvp.
    assert "os.execvp()" not in text and "execvp)" not in text
    assert "os.execvpe()" in text
