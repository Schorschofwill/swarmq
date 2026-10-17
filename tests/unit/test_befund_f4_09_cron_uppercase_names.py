"""Nachweis F4-09: ``TaskInfo`` lehnt Standard-Namen mit ``L``/``W`` in Groß-
oder gemischter Schreibweise als erweiterte Syntax ab.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-09; Verdacht
R4F-06; Gegenprüfung ``6-gegenpruefung-F.md``; Fuzz-Treffer in
``tests/fuzz/test_cron_fuzz.py``, geschrumpft ``"* * * * WED"``).

``_FORBIDDEN_CRON_CHARS`` (``task.py``) sucht ``[LW#]`` als Zeichen im ganzen
Ausdruck. ``WED``, ``Wed``, ``JUL``, ``@HOURLY``, ``@DAILY``, ``@Weekly``
scheitern deshalb mit „cron uses unsupported extended syntax (L/W/#)“;
kleingeschrieben werden dieselben Namen angenommen, ``next_fire_times`` und
``swarmq schedule add --cron`` nehmen sie an.

Erwartet: ``configuration.md:471`` („Standard 5-field cron expression“),
crontab(5) zu Namen („case does not matter“), Kommentar ``task.py:34-36``
(gemeint sind die Operatoren L, W, #). Die Tests mit Marker E-065 prüfen nur
Operatoren (``0 3 L * *``, ``0 3 15W * *``, ``0 3 * * 1#2``) und bleiben bei
einer operatorgenauen Prüfung grün.

Testliste:
# [x] 0 0 * * WED / Wed, 0 0 * JUL *, @HOURLY, @DAILY, @Weekly -> angenommen wie kleingeschrieben
"""

from __future__ import annotations

import pytest

from swarmq.task import TaskInfo

pytestmark = pytest.mark.timeout(5)


@pytest.mark.befund("F4-09")
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="F4-09: offen - grossgeschriebene Standard-Namen mit L/W gelten als erweiterte Syntax",
)
@pytest.mark.parametrize("cron", ["* * * * WED", "0 0 * * Wed", "0 0 * JUL *", "@HOURLY", "@DAILY", "@Weekly"])
def test_standard_names_are_accepted_in_any_case(cron: str) -> None:
    TaskInfo(name="f4_09_lower", queues=["default"], cron=cron.lower())  # Gegenprobe: klein angenommen

    error: ValueError | None = None
    try:
        TaskInfo(name="f4_09_cron", queues=["default"], cron=cron)
    except ValueError as exc:
        error = exc

    assert error is None, f"{cron!r} abgelehnt: {error}"
