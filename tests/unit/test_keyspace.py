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
    ("sq:lock:{sched}:leader", _KS.scheduler_lock),
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
    ("sq:{sched}:defs", _KS.sched_defs),
    ("sq:{sched}:disabled", _KS.sched_disabled),
    ("sq:{sched}:epoch", _KS.sched_epoch),  # Plan 0.12.0, MR 14 (BZ-04)
    ("sq:{sched}:handover", _KS.sched_handover_latest),
    ("sq:{sched}:leader_version", _KS.sched_leader_version),
    ("sq:{sched}:report:def", _KS.sched_report_def),
    ("sq:{sched}:report:first_seen", _KS.sched_report_first_seen),
    ("sq:{sched}:report:known", _KS.sched_report_known),  # Plan 0.12.0, MR 11 (BZ-11)
    ("sq:{sched}:report:last_seen", _KS.sched_report_last_seen),
    ("sq:{sched}:report:queues", _KS.sched_report_queues),  # Plan 0.12.0, MR 18 (BZ-14)
    ("sq:{sched}:worker_version:golden-worker", lambda: _KS.sched_worker_version("golden-worker")),
    ("sq:{sched}:zset", _KS.sched_zset),
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


# ---------------------------------------------------------------------------
# Namensraum (Plan 0.12.0, MR 15, BZ-10)
# ---------------------------------------------------------------------------
#
# Test-Liste:
# - [x] ``KeySpace("app1")`` setzt ``app1:`` vor jeden Key und Kanal; die
#       Hash-Tags bleiben unveraendert
# - [x] Muster und Praefixe tragen den Namensraum (SCAN findet nur eigene Keys)
# - [x] ``keys.namespace.json`` ist ``keys.json`` mit Praefix (Golden additiv)
# - [x] ungueltige Namensraeume: Doppelpunkt, Glob-Zeichen, Leerzeichen, zu lang,
#       ``sq`` -> ``ValueError``; leer -> kein Namensraum


def test_namespace_prefixes_a_key() -> None:
    assert KeySpace("app1").result(TASK_ID) == "app1:sq:result:golden-task-0001"


_NS = KeySpace("app1")


def _all_keys(ks: KeySpace) -> list[str]:
    """Jede Key-Familie einmal, mit den IDs aus ``_TABLE``."""
    return [
        ks.cancel(TASK_ID),
        ks.control_setpoint("golden-group"),
        ks.control_status("golden-group", "golden-sup"),
        ks.guard_run(TASK_ID),
        ks.guard_state(TASK_ID),
        ks.lock("golden-lock"),
        ks.lock("golden-sem"),
        ks.scheduler_lock(),
        ks.progress(TASK_ID),
        ks.result(TASK_ID),
        ks.rate_limit("golden-task", "<window>"),
        ks.sched_fired(f"{SCHEDULE}:{FIRE_TS_MS}"),
        ks.sched_handover("golden-owner"),
        ks.sched_inflight(f"{SCHEDULE}:{FIRE_TS_MS + 60_000}"),
        ks.unique(DEDUP_HASH),
        ks.workflow_cancel(WF_ID),
        ks.chord(WF_ID, "c1"),
        ks.workflow(WF_ID),
        ks.sched_defs(),
        ks.sched_disabled(),
        ks.sched_epoch(),
        ks.sched_handover_latest(),
        ks.sched_leader_version(),
        ks.sched_report_def(),
        ks.sched_report_first_seen(),
        ks.sched_report_known(),
        ks.sched_report_last_seen(),
        ks.sched_report_queues(),
        ks.sched_worker_version("golden-worker"),
        ks.sched_zset(),
    ]


def test_namespace_prefixes_every_key_and_keeps_the_hash_tags() -> None:
    plain = _all_keys(_KS)
    assert set(plain) == _golden_keys()
    assert _all_keys(_NS) == ["app1:" + key for key in plain]


def test_namespace_prefixes_channels_patterns_and_stems() -> None:
    assert [_NS.notify_channel(TASK_ID), _NS.progress_channel(TASK_ID), _NS.workflow_notify_channel(WF_ID)] == [
        "app1:" + _KS.notify_channel(TASK_ID),
        "app1:" + _KS.progress_channel(TASK_ID),
        "app1:" + _KS.workflow_notify_channel(WF_ID),
    ]
    assert _NS.sched_handover_pattern() == "app1:sq:sched:handover:*"
    assert _NS.lock_prefix == "app1:sq:lock:"
    assert _NS.sched_worker_version_prefix == "app1:sq:{sched}:worker_version:"
    # SCAN ohne Namensraum findet keine Keys einer App mit Namensraum (und umgekehrt).
    assert not fnmatch.fnmatchcase(_NS.sched_handover("o"), _KS.sched_handover_pattern())
    assert not fnmatch.fnmatchcase(_KS.sched_handover("o"), _NS.sched_handover_pattern())


def test_namespaced_golden_is_the_golden_with_the_prefix() -> None:
    """Golden additiv (Q3): ``keys.json`` bleibt byte-identisch, die Keys mit
    Namensraum stehen in einer eigenen Datei (Namensraum ``golden-ns``)."""
    plain = json.loads((_GOLDEN / "keys.json").read_text())
    namespaced = json.loads((_GOLDEN / "keys.namespace.json").read_text())
    assert namespaced == {f"golden-ns:{key}": type_ for key, type_ in plain.items()}


@pytest.mark.parametrize(
    "value",
    ["a:b", "app*", "app?", "a[b]", "a{b}", "a b", "-app", ".app", "x" * 65, "sq", "SQ", "ä"],
)
def test_invalid_namespace_is_rejected(value: str) -> None:
    from swarmq.keyspace import validate_namespace

    with pytest.raises(ValueError, match="namespace"):
        validate_namespace(value)


@pytest.mark.parametrize("value", ["app1", "billing-prod", "a.b_c", "a" * 64])
def test_valid_namespace_is_returned(value: str) -> None:
    from swarmq.keyspace import validate_namespace

    assert validate_namespace(value) == value


@pytest.mark.parametrize("value", [None, ""])
def test_empty_namespace_means_none(value: str | None) -> None:
    from swarmq.keyspace import validate_namespace

    assert validate_namespace(value) is None


def test_keyspace_rejects_an_invalid_namespace_and_treats_empty_as_none() -> None:
    with pytest.raises(ValueError, match="namespace"):
        KeySpace("a:b")
    assert KeySpace("").result(TASK_ID) == _KS.result(TASK_ID)
