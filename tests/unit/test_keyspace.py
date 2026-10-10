"""``swarmq.keyspace`` (Plan 0.12.0, MR 0, Q4): eine Quelle fuer alle Valkey-Keys.

Test-Liste:
- [x] ``KeySpace(None)`` erzeugt genau die Keys aus ``golden/keys.json``
      (tabellengetrieben, beide Richtungen: jede Zeile steht im Golden, jeder
      Golden-Key hat eine Zeile)
- [x] Pub/Sub-Kanaele entsprechen ``golden/channels.txt``
- [x] Muster und Praefixe (Handover-Scan, Worker-Versionen, Lock-Praefix)
      passen zu den Keys, die sie finden sollen
"""

from __future__ import annotations

import fnmatch
import json
from collections.abc import Callable
from pathlib import Path

import pytest

from swarmq.keyspace import KeySpace

_GOLDEN = Path(__file__).resolve().parents[1] / "integration" / "backend" / "golden"

# Feste IDs wie in ``tests/integration/backend/_golden.py``.
TASK_ID = "golden-task-0001"
WF_ID = "golden-wf-0001"
DEDUP_HASH = "0" * 64
SCHEDULE = "golden-sched"
FIRE_TS_MS = 1_700_000_000_000

_KS = KeySpace(None)

_TABLE: list[tuple[str, Callable[[], str]]] = [
    ("sq:cancel:golden-task-0001", lambda: _KS.cancel(TASK_ID)),
    ("sq:control:setpoint:golden-group", lambda: _KS.control_setpoint("golden-group")),
    ("sq:control:status:golden-group:golden-sup", lambda: _KS.control_status("golden-group", "golden-sup")),
    ("sq:guard:{golden-task-0001}:run", lambda: _KS.guard_run(TASK_ID)),
    ("sq:guard:{golden-task-0001}:state", lambda: _KS.guard_state(TASK_ID)),
    ("sq:lock:golden-lock", lambda: _KS.lock("golden-lock")),
    ("sq:lock:golden-sem", lambda: _KS.lock("golden-sem")),
    ("sq:lock:{sched}:leader", lambda: _KS.scheduler_lock()),
    ("sq:progress:golden-task-0001", lambda: _KS.progress(TASK_ID)),
    ("sq:result:golden-task-0001", lambda: _KS.result(TASK_ID)),
    ("sq:rl:{golden-task}:<window>", lambda: _KS.rate_limit("golden-task", "<window>")),
    ("sq:sched:fired:golden-sched:1700000000000", lambda: _KS.sched_fired(f"{SCHEDULE}:{FIRE_TS_MS}")),
    ("sq:sched:handover:golden-owner", lambda: _KS.sched_handover("golden-owner")),
    (
        "sq:sched:inflight:golden-sched:1700000060000",
        lambda: _KS.sched_inflight(f"{SCHEDULE}:{FIRE_TS_MS + 60_000}"),
    ),
    (f"sq:unique:{DEDUP_HASH}", lambda: _KS.unique(DEDUP_HASH)),
    ("sq:wf:cancel:golden-wf-0001", lambda: _KS.workflow_cancel(WF_ID)),
    ("sq:wf:chord:golden-wf-0001:c1", lambda: _KS.chord(WF_ID, "c1")),
    ("sq:wf:golden-wf-0001", lambda: _KS.workflow(WF_ID)),
    ("sq:{sched}:defs", lambda: _KS.sched_defs()),
    ("sq:{sched}:disabled", lambda: _KS.sched_disabled()),
    ("sq:{sched}:handover", lambda: _KS.sched_handover_latest()),
    ("sq:{sched}:leader_version", lambda: _KS.sched_leader_version()),
    ("sq:{sched}:report:def", lambda: _KS.sched_report_def()),
    ("sq:{sched}:report:first_seen", lambda: _KS.sched_report_first_seen()),
    ("sq:{sched}:report:known", lambda: _KS.sched_report_known()),  # Plan 0.12.0, MR 11 (BZ-11)
    ("sq:{sched}:report:last_seen", lambda: _KS.sched_report_last_seen()),
    ("sq:{sched}:worker_version:golden-worker", lambda: _KS.sched_worker_version("golden-worker")),
    ("sq:{sched}:zset", lambda: _KS.sched_zset()),
]


def _golden_keys() -> set[str]:
    return set(json.loads((_GOLDEN / "keys.json").read_text()))


@pytest.mark.parametrize(("expected", "build"), _TABLE, ids=[row[0] for row in _TABLE])
def test_key_matches_golden(expected: str, build: Callable[[], str]) -> None:
    assert build() == expected
    assert expected in _golden_keys()


def test_every_golden_key_has_a_table_row() -> None:
    assert _golden_keys() == {row[0] for row in _TABLE}


def test_channels_match_golden() -> None:
    golden = [line.split("\t", 1)[0] for line in (_GOLDEN / "channels.txt").read_text().splitlines()]
    assert golden == [
        _KS.notify_channel(TASK_ID),
        _KS.progress_channel(TASK_ID),
        _KS.workflow_notify_channel(WF_ID),
    ]


def test_handover_scan_pattern_finds_handover_markers_only() -> None:
    pattern = _KS.sched_handover_pattern()
    assert pattern == "sq:sched:handover:*"
    assert fnmatch.fnmatchcase(_KS.sched_handover("golden-owner"), pattern)
    assert not fnmatch.fnmatchcase(_KS.sched_fired("golden-owner"), pattern)


def test_prefixes_are_the_stem_of_their_keys() -> None:
    assert _KS.lock("x") == _KS.lock_prefix + "x"
    assert _KS.sched_worker_version("w") == _KS.sched_worker_version_prefix + "w"
