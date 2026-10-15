"""Nachweis F3-08: ``TaskInfo`` nimmt Cron-Ausdruecke ohne naechsten Termin an.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-08; Quelle R3C-07,
Teil 1). Die Feldzahl (6/7 Felder) ist die offene Produktfrage F3-35 und
gehoert nicht hierher.

``TaskInfo._validate_scheduling`` (``task.py``) prueft nur die Syntax mit
``croniter(...)``. Ausdruecke wie ``0 0 30 2 *`` (30. Februar) oder
``0 0 31 4 *`` (31. April) sind syntaktisch gueltig, haben aber nie einen
Termin. Der Leader verwirft sie beim Sync mit ``scheduler_cron_report_invalid``
(``scheduler/sync.py``, ``next_fire_ms``), und der Cron feuert still nie.

Erwartet (``configuration.md``: „validates every field … a misconfigured task
should fail the worker's startup“): ``ValueError`` schon beim Bau von
``TaskInfo``.

Testliste:
# [x] cron="0 0 30 2 *" / "0 0 31 4 *" -> ValueError beim TaskInfo-Bau
"""

from __future__ import annotations

import pytest

from swarmq.task import TaskInfo


@pytest.mark.befund("F3-08")
@pytest.mark.entscheidung("E-065")
@pytest.mark.parametrize("cron", ["0 0 30 2 *", "0 0 31 4 *"])
def test_taskinfo_rejects_cron_that_never_fires(cron: str) -> None:
    with pytest.raises(ValueError, match="cron"):
        TaskInfo(name="f308_never", queues=["default"], cron=cron)
