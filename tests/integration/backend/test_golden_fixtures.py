"""Golden-Fixtures (KTD12): Daten- und Wire-Kompatibilität 0.1.2 → 0.2.0.

Gleichheit (nicht Teilmenge) in beiden Richtungen: Fixture dekodieren und mit
der festen Eingabe vergleichen, feste Eingabe kodieren und mit den
Fixture-Bytes vergleichen. Eine rote Zeile hier ist ein benannter Bruch, der
einen Migrationshinweis im Changelog braucht (R15) — nie ein Grund, die
Fixture stillschweigend zu regenerieren.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import msgspec
import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.scheduler import ScheduleDefinition
from swarmq.serialization import decode_json, decode_result
from tests.integration.backend import _golden as g


@pytest.fixture
async def backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    impl = ValkeyBackend(valkey_url)
    await impl.connect()
    yield impl
    await impl.close()


async def test_keys_and_types_match_golden(backend: ValkeyBackend) -> None:
    expected = json.loads((g.GOLDEN_DIR / "keys.json").read_text())
    assert await g.snapshot_keys(backend) == expected


async def test_namespaced_keys_and_types_match_golden(valkey_url: str) -> None:
    """BZ-10 (Plan 0.12.0, MR 15): mit Namensraum ``golden-ns`` liegt jeder
    Key unter ``golden-ns:sq:...``; Golden additiv in ``keys.namespace.json``."""
    expected = json.loads((g.GOLDEN_DIR / "keys.namespace.json").read_text())
    backend = ValkeyBackend(valkey_url, namespace=g.NAMESPACE)
    await backend.connect()
    try:
        assert await g.snapshot_keys(backend) == expected
    finally:
        await backend.close()


def test_result_envelope_is_byte_identical_and_decodes_to_fixed_input() -> None:
    fixture = (g.GOLDEN_DIR / "values" / "result.msgpack").read_bytes()
    assert g.value_fixtures()["result.msgpack"] == fixture
    assert decode_result(fixture) == g.golden_result_dict()


def test_error_result_envelope_is_byte_identical_and_decodes_to_fixed_input() -> None:
    """P9 (Plan 2026-10-12-1105, MR 3, additiv): Form des Fehlerergebnisses
    samt ``error_class``; ``result.msgpack`` bleibt unveraendert."""
    fixture = (g.GOLDEN_DIR / "values" / "error_result.msgpack").read_bytes()
    assert g.value_fixtures()["error_result.msgpack"] == fixture
    assert decode_result(fixture) == g.golden_error_result_dict()


def test_workflow_state_is_field_equal() -> None:
    fixture = (g.GOLDEN_DIR / "values" / "workflow_state.json").read_bytes()
    assert g.value_fixtures()["workflow_state.json"] == fixture
    assert decode_json(fixture) == g.golden_workflow_state()


def test_schedule_definition_is_field_equal() -> None:
    fixture = (g.GOLDEN_DIR / "values" / "schedule_definition.json").read_bytes()
    assert g.value_fixtures()["schedule_definition.json"] == fixture
    assert msgspec.json.decode(fixture, type=ScheduleDefinition) == g.golden_schedule_definition()
    assert list(json.loads(fixture)) == [
        "schedule_name",
        "task_name",
        "args",
        "kwargs",
        "queue",
        "cron",
        "fire_ts_ms",
        "source",
        "disabled",
        "last_fire_at",
        "dedup_hash",
        "unique_until",
        "timezone",
        "task_id",
        "unroutable_since",
        "unique_for",
    ]


async def test_progress_hash_fields_match_golden(backend: ValkeyBackend) -> None:
    expected = json.loads((g.GOLDEN_DIR / "values" / "progress_hash.json").read_text())
    await backend.set_progress(g.TASK_ID, g.PROGRESS_FIELDS, g.TTL_MS)
    raw = await backend.get_progress(g.TASK_ID)
    assert raw is not None
    assert {k.decode(): v.decode() for k, v in raw.items()} == expected


def test_channels_match_golden() -> None:
    assert g.channels() == (g.GOLDEN_DIR / "channels.txt").read_text()


def test_namespaced_channels_match_golden() -> None:
    # Review MR 15 (Hinweis): Kanaele mit Namensraum additiv (Q3); die Datei
    # ist ``channels.txt`` mit Praefix ``golden-ns:``.
    expected = (g.GOLDEN_DIR / "channels.namespace.txt").read_text()
    assert g.channels_namespace() == expected
    plain = (g.GOLDEN_DIR / "channels.txt").read_text().splitlines()
    assert expected.splitlines() == [f"{g.NAMESPACE}:{line}" for line in plain]


async def test_queue_arguments_match_golden() -> None:
    expected = json.loads((g.GOLDEN_DIR / "queue_args.json").read_text())
    assert await g.queue_args() == expected


def test_header_names_match_golden() -> None:
    assert g.header_names() == (g.GOLDEN_DIR / "headers.txt").read_text()


def test_lua_arity_and_slot_equality_match_golden() -> None:
    contract = g.lua_contract()
    assert contract == (g.GOLDEN_DIR / "lua_contract.txt").read_text()
    assert "DIFFERENT" not in contract
