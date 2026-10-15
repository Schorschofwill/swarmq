"""Nachweis F3-19 (Code-Teil): Der JSON-Payload eines Argparse-Fehlers unter
``swarmq worker --check`` hat nicht die zugesagte Schlüsselmenge.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-19, Verdacht R3D-19a).

``check_cmd.py`` sagt zu, dass jeder Payload von ``--check`` die Schlüssel
aus ``_WORKER_CHECK_PAYLOAD_KEYS`` trägt („stable field-set contract“), egal
welche Prüfung scheitert. ``_emit_argparse_check_error`` schreibt aber nur
die alten acht Schlüssel; ``pool``, ``slots`` und ``configure`` fehlen.
``message-formats.md`` sagt für diesen Fall: die übrigen Felder ``null``.

Nur der Code-Teil: Exit 1 ist maßgeblich (``cli.md:33``, Code); die Angabe
„Exit 2“ in ``message-formats.md`` ist ein Doku-Fix und wird hier nicht
geprüft. Die Schlüsselliste steht ausdrücklich im Test (öffentlicher
JSON-Vertrag), statt die private Konstante zu importieren.

Testliste:
# [x] ``swarmq worker --check`` ohne ``--module`` → rc 1, ``ok: false``,
#     alle Vertragsschlüssel vorhanden, ``slots`` und ``configure`` ``null``
"""

from __future__ import annotations

import json

import pytest

from swarmq.cli import main

#: Vertragsschlüssel jedes ``--check``-Payloads (``check_cmd._WORKER_CHECK_PAYLOAD_KEYS``).
_CONTRACT_KEYS = frozenset(
    {
        "ok",
        "module",
        "module_origin",
        "reload",
        "watch_dirs",
        "warnings",
        "error",
        "error_type",
        "pool",
        "slots",
        "configure",
    }
)


@pytest.mark.befund("F3-19")
@pytest.mark.xfail(strict=True, reason="F3-19: offen - Argparse-Payload von --check ohne pool/slots/configure")
def test_argparse_error_payload_carries_every_contract_key(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["worker", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["ok"] is False
    assert payload["error_type"] == "ArgparseError"
    missing = _CONTRACT_KEYS - payload.keys()
    assert not missing, f"Argparse-Payload ohne Vertragsschlüssel: {sorted(missing)}"
    assert payload["slots"] is None
    assert payload["configure"] is None
