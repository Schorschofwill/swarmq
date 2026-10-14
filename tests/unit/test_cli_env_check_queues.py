"""``swarmq check``: Abschnitt ``queues`` (Plan 0.12.0, MR 18, BZ-14).

Mit ``SWARMQ_BROKER_URL`` liest ``swarmq check`` die von Workern gemeldeten
Queues (``sq:{sched}:report:queues``) und prueft sie per passivem Declare:
verwaiste Queues (Rueckstand, kein Consumer, kein lebender Worker meldet sie)
und fehlende ``.XQ``. Beides sind Warnungen (Exit 0). Getestet wird der reine
Bericht ``build_report`` aus gelesenen Werten; der Lauf gegen Valkey und
RabbitMQ steht in ``tests/integration/test_cli_env_check.py``.

Test-Liste:
- [x] ohne ``SWARMQ_BROKER_URL``: Abschnitt ``queues`` ungeprueft, kein Befund
- [x] nicht mehr gemeldete Queue mit Rueckstand ohne Consumer -> ``warning`` ``orphaned_queue``, Exit 0
- [x] lebend gemeldete Queue ohne Consumer oder Queue mit Consumer: kein Befund
- [x] gemeldete Queue ohne ``.XQ`` -> ``warning`` ``dead_letter_queue_missing``
- [x] fehlende Haupt-Queue: kein ``.XQ``-Befund
- [x] Meldungen nicht lesbar -> ``warning`` ``queues_unchecked`` (Broker nicht erreichbar: Exit 1, Integrationstest)
"""

from __future__ import annotations

from typing import Any

import pytest

from swarmq.cli.env_check_cmd import QueueState, build_report
from swarmq.contracts.broker import QueueStatus

pytestmark = pytest.mark.timeout(5)

_DESCRIBED: dict[str, Any] = {
    "maxmemory_policy": "noeviction",
    "maxmemory": 0,
    "used_memory": 1,
    "eviction": "safe",
    "aof_enabled": True,
    "version": "8.1.10",
    "cluster_enabled": False,
}


def _report(queues: QueueState | None) -> dict[str, Any]:
    return build_report("valkey://fake:6399/0", dict(_DESCRIBED), "strict", queues=queues)


@pytest.mark.entscheidung("E-006")
def test_without_broker_url_queues_stay_unchecked() -> None:
    report = _report(None)

    assert report["queues"] == {"checked": False, "reported": None, "orphaned": [], "missing_dead_letter_queues": []}
    assert report["findings"] == []


_NOW_MS = 1_700_000_000_000
_LIVE_MS = 180_000


def _state(last_seen_s_ago: dict[str, float], statuses: dict[str, QueueStatus | None]) -> QueueState:
    return QueueState(
        now_ms=_NOW_MS,
        last_seen_ms={q: _NOW_MS - int(ago * 1000) for q, ago in last_seen_s_ago.items()},
        live_window_ms=_LIVE_MS,
        statuses=statuses,
    )


def _codes(report: dict[str, Any]) -> list[str]:
    return [f["code"] for f in report["findings"]]


@pytest.mark.entscheidung("E-015")
def test_unreported_queue_with_backlog_and_no_consumer_is_orphaned() -> None:
    report = _report(
        _state(
            {"old": 3600.0},
            {"old": QueueStatus(message_count=4, consumer_count=0), "old.XQ": QueueStatus(0, 0)},
        )
    )

    assert report["ok"] is True
    assert report["queues"]["checked"] is True
    assert report["queues"]["reported"] == 1
    assert report["queues"]["orphaned"] == [{"queue": "old", "messages": 4, "last_reported_s_ago": 3600}]
    [finding] = report["findings"]
    assert (finding["section"], finding["level"], finding["code"]) == ("queues", "warning", "orphaned_queue")
    assert "old" in finding["message"]
    assert "4 messages" in finding["message"]


@pytest.mark.parametrize(
    ("ago", "status"),
    [(60.0, QueueStatus(message_count=4, consumer_count=0)), (3600.0, QueueStatus(message_count=4, consumer_count=1))],
    ids=["live-report", "with-consumer"],
)
def test_live_or_consumed_queue_is_not_orphaned(ago: float, status: QueueStatus) -> None:
    report = _report(_state({"tasks": ago}, {"tasks": status, "tasks.XQ": QueueStatus(0, 0)}))

    assert report["queues"]["orphaned"] == []
    assert report["findings"] == []


def test_reported_queue_without_dead_letter_queue_warns() -> None:
    report = _report(
        _state({"tasks": 10.0}, {"tasks": QueueStatus(message_count=0, consumer_count=2), "tasks.XQ": None})
    )

    assert report["ok"] is True
    assert report["queues"]["missing_dead_letter_queues"] == ["tasks"]
    [finding] = report["findings"]
    assert (finding["level"], finding["code"]) == ("warning", "dead_letter_queue_missing")
    assert "tasks.XQ" in finding["message"]


def test_missing_main_queue_needs_no_dead_letter_queue() -> None:
    report = _report(_state({"gone": 3600.0}, {"gone": None, "gone.XQ": None}))

    assert report["queues"]["missing_dead_letter_queues"] == []
    assert report["findings"] == []


@pytest.mark.entscheidung("E-006")
def test_unreadable_queues_warn_without_failing() -> None:
    report = _report(QueueState(error="broker unreachable: connection refused"))

    assert report["ok"] is True
    assert report["queues"]["checked"] is False
    [finding] = report["findings"]
    assert (finding["section"], finding["level"], finding["code"]) == ("queues", "warning", "queues_unchecked")
    assert "connection refused" in finding["message"]
