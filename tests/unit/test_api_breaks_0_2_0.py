"""0.2.0-Bruch-Tests (Plan U15, test-first gegen das U14-Inventar).

Parametrisiert über die Alias-Tabellen aus
``docs/design/2026-08-0-2-0-api-brueche.md``: jedes entfernte Symbol muss
mit ``AttributeError`` verschwinden. Rot vor der Entfernung, grün danach —
und dauerhaft der Wächter, dass kein Alias zurückkehrt.
"""

from __future__ import annotations

import pytest

# A2: ValkeyBackend-Methoden-Aliase (KTD10, U17/U18)
_BACKEND_ALIASES = [
    "setnx_unique",
    "get_results_batch",
    "publish_notification",
    "publish_workflow_notification",
    "subscribe_and_wait",
    "create_pubsub",
    "zadd_schedule",
    "zrem_schedule",
    "zrangebyscore_schedules",
    "hset_schedule_def",
    "hsetnx_schedule_def",
    "hget_schedule_def",
    "hdel_schedule_def",
    "hgetall_schedule_defs",
    "set_schedfire_dedup",
    "set_schedfire_inflight",
    "clear_schedfire_inflight",
    "is_schedfire_completed",
    "set_schedfire_handover_marker",
    "get_and_del_schedfire_handover_marker",
]

# A3: RabbitMQBroker-Admin-/Verb-Aliase (KTD10, U5/U8)
_BROKER_ALIASES = [
    "list_dead_letter_queue",
    "inspect_dead_letter_message",
    "retry_dead_letter_message",
    "retry_all_dead_letter_messages",
    "purge_dead_letter_queue",
    "get_queue_depth",
    "get_dlq_depth",
    "nack",
    "publish_to_delay_queue",
    "publish_to_dlq",
]

# B1: Klassen-Re-Exports der Alias-Module
_CLASS_REEXPORTS = [
    ("swarmq.locking", "LockManager"),
    ("swarmq.limits", "RateLimiter"),
    ("swarmq.workflow_orchestration", "ChordManager"),
    ("swarmq.workflow_orchestration", "chord_set_key"),
]


@pytest.mark.parametrize("name", _BACKEND_ALIASES)
def test_backend_alias_removed(name: str) -> None:
    from swarmq.backend.valkey import ValkeyBackend

    assert not hasattr(ValkeyBackend, name), (
        f"ValkeyBackend.{name} ist ein KTD10-Alias und muss seit U15 entfernt sein "
        "(Migration: docs/design/2026-08-0-2-0-api-brueche.md, A1/A2)"
    )


@pytest.mark.parametrize("name", _BROKER_ALIASES)
def test_broker_alias_removed(name: str) -> None:
    from swarmq.broker.rabbitmq import RabbitMQBroker

    assert not hasattr(RabbitMQBroker, name), (
        f"RabbitMQBroker.{name} ist ein KTD10-Alias und muss seit U15 entfernt sein "
        "(Migration: docs/design/2026-08-0-2-0-api-brueche.md, A3)"
    )


@pytest.mark.parametrize(("module", "name"), _CLASS_REEXPORTS)
def test_class_reexport_removed(module: str, name: str) -> None:
    import importlib

    mod = importlib.import_module(module)
    with pytest.raises(AttributeError):
        getattr(mod, name)
