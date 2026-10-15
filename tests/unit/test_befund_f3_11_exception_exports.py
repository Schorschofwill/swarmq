"""Nachweis F3-11: ``BrokerPublishBlocked`` und ``PublishRejected`` fehlen in
``swarmq.__all__``.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-11; Quelle R3D-17).

``website/reference/errors.md`` sagt: „Every class except
``WorkflowStateConflict`` and ``SettleFailedAfterPublish`` can be imported from
``swarmq`` directly“. Der Test liest die Klassen aus der Tabelle „Hierarchy at
a glance“ und prueft jede ausser den zwei genannten: in ``swarmq.__all__`` und
als dieselbe Klasse wie in ``swarmq.exceptions`` importierbar.

Testliste:
# [x] jede Klasse der Tabelle (ausser den zwei Ausnahmen) ist aus ``swarmq``
#     exportiert
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import swarmq
import swarmq.exceptions

_ERRORS_MD = Path(__file__).resolve().parents[2] / "website" / "reference" / "errors.md"
_NOT_EXPORTED = {"WorkflowStateConflict", "SettleFailedAfterPublish"}


def _documented_classes() -> list[str]:
    return re.findall(r"^\| `(\w+)` \| `", _ERRORS_MD.read_text(encoding="utf-8"), re.MULTILINE)


@pytest.mark.befund("F3-11")
@pytest.mark.xfail(strict=True, reason="F3-11: offen - BrokerPublishBlocked/PublishRejected fehlen in swarmq.__all__")
def test_documented_exceptions_are_exported_from_package() -> None:
    documented = _documented_classes()
    assert len(documented) > 20, "Voraussetzung: Tabelle in errors.md gefunden"

    missing = [
        name
        for name in documented
        if name not in _NOT_EXPORTED
        and (name not in swarmq.__all__ or getattr(swarmq, name, None) is not getattr(swarmq.exceptions, name))
    ]

    assert missing == [], f"laut errors.md aus swarmq importierbar, aber nicht exportiert: {missing}"
