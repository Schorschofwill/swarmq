"""Nachweis F3-04: ``inf``/``nan`` gehen durch fast alle Frist-Felder der
``SwarmQConfig``.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-04, R3C-03).

``_validate_core`` prueft die Fristen nur mit ``value <= 0``; ``inf`` und
``nan`` bestehen das. Endlich geprueft sind bisher nur ``lock_wait_max_s``
(Commit 2962bf2, Review MR 13, P2-3) und ``log_drain_timeout``. Folgen etwa:
``SWARMQ_CONSUMER_TIMEOUT_S=inf`` macht den Default von
``execution_guard_done_ttl_s`` unendlich, ``ExecutionGuard.apply`` wirft vor
jedem Claim ``OverflowError``; ``dead_letter_ttl_s=inf`` laesst
``aio_pika.Message`` scheitern.

Festlegung (Entscheidung 9): Alle Frist-/Zeitfelder muessen endlich sein,
Muster ``test_must_be_finite`` aus 2962bf2 (kwarg und Umgebung, ``inf`` und
``nan``) -> ``ConfigurationError``, deren Meldung das Feld nennt.

Seit 0.12.3 behoben (E-063): ``_validate_finite`` prueft ``_FINITE_FIELDS``
vor allen Relationspruefungen. Darum sind auch die Faelle dabei, die vorher
nur ueber eine Relationspruefung (mit irrefuehrender Meldung) scheiterten:
``inf`` in ``heartbeat_interval_s``, ``scheduler_heartbeat_interval``,
``scheduler_seen_interval_s``, ``backend_defer_base_delay`` und
``scheduler_lock_ttl`` sowie ``nan`` in ``scheduler_lock_ttl``.

Testliste:
# [x] jedes Frist-Feld (Fliesskomma-Sekunden) lehnt inf/nan als kwarg ab
# [x] dieselben Felder lehnen "inf"/"nan" aus der Umgebung ab
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError

# Fliesskomma-Fristen: ``_POSITIVE_FIELDS`` (ohne Ganzzahl-Felder und ohne das
# schon endlich gepruefte ``lock_wait_max_s``) plus ``retry_max_delay``,
# ``unknown_task_grace_s`` und die optionalen TTLs mit berechnetem Default.
_DEADLINE_FIELDS: tuple[tuple[str, str], ...] = (
    ("cancel_poll_interval", "SWARMQ_CANCEL_POLL_INTERVAL"),
    ("backend_op_timeout", "SWARMQ_BACKEND_OP_TIMEOUT"),
    ("backend_connect_timeout_s", "SWARMQ_BACKEND_CONNECT_TIMEOUT_S"),
    ("backend_defer_base_delay", "SWARMQ_BACKEND_DEFER_BASE_DELAY"),
    ("backend_defer_max_delay", "SWARMQ_BACKEND_DEFER_MAX_DELAY"),
    ("scheduler_poll_interval", "SWARMQ_SCHEDULER_POLL_INTERVAL"),
    ("scheduler_heartbeat_interval", "SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL"),
    ("metrics_sample_interval", "SWARMQ_METRICS_SAMPLE_INTERVAL"),
    ("scheduler_lock_ttl", "SWARMQ_SCHEDULER_LOCK_TTL"),
    ("scheduler_inflight_ttl", "SWARMQ_SCHEDULER_INFLIGHT_TTL"),
    ("scheduler_seen_interval_s", "SWARMQ_SCHEDULER_SEEN_INTERVAL_S"),
    ("scheduler_prune_after_s", "SWARMQ_SCHEDULER_PRUNE_AFTER_S"),
    ("scheduler_unroutable_oneshot_max_s", "SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S"),
    ("scheduler_dedup_ttl", "SWARMQ_SCHEDULER_DEDUP_TTL"),
    ("burst_idle_timeout", "SWARMQ_BURST_IDLE_TIMEOUT"),
    ("publish_timeout_s", "SWARMQ_PUBLISH_TIMEOUT_S"),
    ("heartbeat_interval_s", "SWARMQ_HEARTBEAT_INTERVAL_S"),
    ("heartbeat_timeout_s", "SWARMQ_HEARTBEAT_TIMEOUT_S"),
    ("consumer_timeout_s", "SWARMQ_CONSUMER_TIMEOUT_S"),
    ("consumer_watch_interval_s", "SWARMQ_CONSUMER_WATCH_INTERVAL_S"),
    ("consumer_ready_grace_s", "SWARMQ_CONSUMER_READY_GRACE_S"),
    ("execution_guard_ttl_s", "SWARMQ_EXECUTION_GUARD_TTL_S"),
    ("execution_guard_done_ttl_s", "SWARMQ_EXECUTION_GUARD_DONE_TTL_S"),
    ("execution_guard_backlog_ttl_s", "SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S"),
    ("dead_letter_ttl_s", "SWARMQ_DEAD_LETTER_TTL_S"),
    ("unknown_task_grace_s", "SWARMQ_UNKNOWN_TASK_GRACE_S"),
    ("retry_max_delay", "SWARMQ_RETRY_MAX_DELAY"),
)

_CASES = [
    pytest.param(field_name, env_name, value, id=f"{field_name}-{value}")
    for field_name, env_name in _DEADLINE_FIELDS
    for value in ("inf", "nan")
]


def _cfg(**kwargs: Any) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)


@pytest.mark.befund("F3-04")
@pytest.mark.entscheidung("E-063")
@pytest.mark.parametrize(("field_name", "env_name", "value"), _CASES)
def test_deadline_field_must_be_finite_kwarg(field_name: str, env_name: str, value: str) -> None:
    del env_name
    with pytest.raises(ConfigurationError, match=re.escape(field_name)):
        _cfg(**{field_name: float(value)})


@pytest.mark.befund("F3-04")
@pytest.mark.entscheidung("E-063")
@pytest.mark.parametrize(("field_name", "env_name", "value"), _CASES)
def test_deadline_field_must_be_finite_env(
    monkeypatch: pytest.MonkeyPatch, field_name: str, env_name: str, value: str
) -> None:
    monkeypatch.setenv(env_name, value)
    with pytest.raises(ConfigurationError, match=re.escape(field_name)):
        _cfg()
