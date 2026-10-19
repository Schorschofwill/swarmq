"""Entscheidungen zu ``swarmq schedule add`` (Fehlersuche Runde 4, Georg 2026-10-17).

Gegen ``InMemoryBackend``; der Handler schließt das Backend, die Tests lesen
danach weiter.

Testliste:
# [x] E-076: neu angelegt -> ``inserted: true``, ``already_exists: false``
# [x] E-076: Name belegt -> ``inserted: false``, ``already_exists: true``, kein ``duplicate_of``
# [x] E-059 (Nachtrag, CLI-Pfad): Fehlerantwort beim Speichern -> eigener Claim freigegeben
# [x] E-065 (Nachtrag, Produktfrage B): ``--cron`` nimmt die Formen der Positivliste an
#
# Fix-Plan Runde 4, U18 (PF-6, PF-7, KTD13):
# [x] E-076: Dedup-Duplikat -> ``already_exists: false``, ``duplicate_of`` gesetzt
# [x] E-059: Konflikt, Eintrag ohne eigenen ``dedup_hash`` -> eigener Claim frei
# [x] E-059: Konflikt, Eintrag mit eigenem ``dedup_hash`` -> Claim bleibt
# [x] E-059: Backend-Ausfall beim Speichern -> Claim bleibt
# [x] E-059: Konflikt, Lesen des Eintrags fällt aus -> Claim bleibt
# [x] E-059: Konflikt, Lesen des Eintrags mit Fehlerantwort -> Claim frei
# [x] E-059: Konflikt, Eintrag verschwunden -> Claim frei
# [x] E-059: Duplikat (Claim gehört einem anderen Schedule) -> fremder Claim bleibt
# [x] E-076: gleicher Name, gleiche Argumente, Claim gehalten -> ``already_exists: true``
# Nacharbeit U18 (Orchestrator): eigener Claim gehalten -> Eintrag lesen,
# ``already_exists`` = Eintrag vorhanden:
# [x] Oneshot gefeuert, Claim läuft noch -> ``already_exists: false``
# [x] Claim ohne Eintrag nach Ausfall -> ``already_exists: false``
# [x] nach ``schedule remove`` -> ``already_exists: false``
# [x] Lesen fällt aus -> Claim bleibt, Fehler an Aufrufer
# [x] Lesen mit Fehlerantwort -> Claim bleibt (nicht selbst gesetzt), Fehler an Aufrufer
#
# 0.13.1 (Entscheidungsvorlage 0.13.0, Punkt 6C; Nachtrag E-059): ein Abbruch
# gilt wie ein Ausfall als unbestimmt:
# [x] Abbruch beim Speichern -> Claim bleibt, ``CancelledError`` an den Aufrufer
# [x] Abbruch beim Lesen des Eintrags nach Konflikt -> Claim bleibt
# [x] anderer Fehler (``RuntimeError``) beim Speichern -> Claim frei
"""

from __future__ import annotations

import asyncio
import io
import json
from contextlib import redirect_stdout
from typing import Any
from unittest.mock import AsyncMock

import pytest

from swarmq.cli import _build_parser, schedule_cmd
from swarmq.exceptions import BackendConnectionError, BackendResponseError
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import ScheduleStore
from swarmq.serialization import compute_dedup_hash
from tests.support.inmemory import FakeClock, InMemoryBackend

pytestmark = [pytest.mark.timeout(5)]

_NOW_MS = 1_800_000_000_000


@pytest.fixture
async def memory(monkeypatch: pytest.MonkeyPatch) -> InMemoryBackend:
    be = InMemoryBackend(FakeClock(_NOW_MS))
    await be.connect()
    be.close = AsyncMock()  # type: ignore[method-assign]  # der Handler schließt; die Tests lesen weiter

    async def _factory() -> InMemoryBackend:
        return be

    monkeypatch.setattr(schedule_cmd, "_get_backend_factory", lambda: _factory)
    return be


async def _add(name: str, *extra: str, cron: str = "0 1 * * *", at: str | None = None) -> dict[str, Any]:
    when = ["--at", at] if at is not None else ["--cron", cron]
    ns = _build_parser().parse_args(["schedule", "add", name, *when, "--task", "t", "--format", "json", *extra])
    buf = io.StringIO()
    with redirect_stdout(buf):
        await schedule_cmd._run_schedule_add(ns)
    payload: dict[str, Any] = json.loads(buf.getvalue())
    return payload


@pytest.mark.entscheidung("E-076")
async def test_inserted_schedule_reports_already_exists_false(memory: InMemoryBackend) -> None:
    payload = await _add("nightly")

    assert payload["inserted"] is True
    assert payload["already_exists"] is False


@pytest.mark.entscheidung("E-076")
async def test_taken_name_reports_already_exists_true(memory: InMemoryBackend) -> None:
    await _add("nightly")

    payload = await _add("nightly", cron="0 2 * * *")

    assert payload["inserted"] is False
    assert payload["already_exists"] is True
    assert "duplicate_of" not in payload


@pytest.mark.entscheidung("E-076")
async def test_dedup_duplicate_reports_already_exists_false_and_duplicate_of(memory: InMemoryBackend) -> None:
    """PF-7: unter dem neuen Namen existiert nichts; den Grund nennt ``duplicate_of``."""
    await _add("nightly", "--unique-for", "60")

    payload = await _add("nightly-copy", "--unique-for", "60")

    assert payload["inserted"] is False
    assert payload["already_exists"] is False
    assert payload["duplicate_of"] == "nightly"
    assert await _claim_owner(memory) == "nightly"  # der fremde Claim bleibt


@pytest.mark.entscheidung("E-076")
async def test_same_name_and_args_with_held_claim_reports_already_exists_true(memory: InMemoryBackend) -> None:
    """Unter dem Namen existiert der Eintrag, der den Claim hält; ``duplicate_of``
    nennt den eigenen Namen."""
    await _add("nightly", "--unique-for", "60")

    payload = await _add("nightly", "--unique-for", "60")

    assert payload["inserted"] is False
    assert payload["already_exists"] is True
    assert payload["duplicate_of"] == "nightly"
    assert await _claim_owner(memory) == "nightly"


@pytest.mark.entscheidung("E-059")
async def test_error_reply_on_store_releases_the_own_claim(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fehlerantwort (nichts gespeichert): der eigene Claim wird freigegeben."""
    monkeypatch.setattr(ScheduleStore, "add_schedule", AsyncMock(side_effect=BackendResponseError("ERR script")))

    with pytest.raises(BackendResponseError):
        await _add("nightly", "--unique-for", "60")

    dedup_hash = compute_dedup_hash("t", (), {})
    assert await memory.claim_unique(dedup_hash, "someone-else", 60_000) is None


@pytest.mark.entscheidung("E-065")
@pytest.mark.parametrize("cron", ["0 9 * * mon-fri", "*/15 * * * *", "0 0 1,15 jan *", "@daily"])
async def test_cli_cron_accepts_the_crontab5_positive_list(memory: InMemoryBackend, cron: str) -> None:
    payload = await _add("job", cron=cron)

    assert payload["inserted"] is True


_HASH = compute_dedup_hash("t", (), {})


async def _claim_owner(memory: InMemoryBackend) -> str | None:
    """Owner des Dedup-Claims von ``t()``; ``None``, wenn er frei ist (der
    Probe-Claim belegt ihn dann)."""
    return await memory.claim_unique(_HASH, "probe", 60_000)


@pytest.mark.entscheidung("E-059")
async def test_conflict_with_foreign_entry_releases_the_own_claim(memory: InMemoryBackend) -> None:
    """PF-6: der Name trägt einen Eintrag ohne den eigenen ``dedup_hash``;
    kein Schedule trägt den eigenen Claim, er wird owner-geprüft frei."""
    await _add("nightly")

    payload = await _add("nightly", "--unique-for", "60", cron="0 2 * * *")

    assert payload["inserted"] is False
    assert payload["already_exists"] is True
    assert await _claim_owner(memory) is None


@pytest.mark.entscheidung("E-059")
async def test_conflict_with_entry_carrying_the_own_hash_keeps_the_claim(memory: InMemoryBackend) -> None:
    """PF-6: der Eintrag trägt den eigenen ``dedup_hash`` (etwa ein eigener
    Insert nach verlorener Antwort, E-058); er trägt den Claim, der bleibt."""
    await _add("nightly", "--unique-for", "60")
    await memory.release_unique(_HASH, "nightly")  # Claim abgelaufen, Eintrag steht noch

    payload = await _add("nightly", "--unique-for", "60")

    assert payload["inserted"] is False
    assert payload["already_exists"] is True
    assert await _claim_owner(memory) == "nightly"


@pytest.mark.entscheidung("E-059")
async def test_outage_on_store_keeps_the_own_claim(memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch) -> None:
    """PF-6: ``sched_add.lua`` speichert atomar, nach einem Ausfall kann der
    Eintrag stehen und feuern; der Claim bleibt (wie E-041)."""
    monkeypatch.setattr(ScheduleStore, "add_schedule", AsyncMock(side_effect=BackendConnectionError("timeout")))

    with pytest.raises(BackendConnectionError):
        await _add("nightly", "--unique-for", "60")

    assert await _claim_owner(memory) == "nightly"


@pytest.mark.entscheidung("E-059")
async def test_conflict_with_vanished_entry_releases_the_own_claim(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PF-6: zwischen Konflikt und Lesen verschwunden (gefeuert, entfernt);
    kein Eintrag trägt den eigenen Hash, der Claim wird frei."""
    monkeypatch.setattr(ScheduleStore, "add_schedule", AsyncMock(return_value=False))

    payload = await _add("nightly", "--unique-for", "60")

    assert payload["inserted"] is False
    assert await _claim_owner(memory) is None


@pytest.mark.entscheidung("E-059")
async def test_error_reply_on_reading_the_conflicting_entry_releases_the_own_claim(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PF-6, Lesen scheitert mit Fehlerantwort: Fehlerantwort-Regel, der
    Claim wird frei; der Fehler erreicht den Aufrufer."""
    await _add("nightly")
    monkeypatch.setattr(ScheduleStore, "get_definition", AsyncMock(side_effect=BackendResponseError("ERR wrongtype")))

    with pytest.raises(BackendResponseError):
        await _add("nightly", "--unique-for", "60", cron="0 2 * * *")

    assert await _claim_owner(memory) is None


@pytest.mark.entscheidung("E-059")
async def test_outage_on_reading_the_conflicting_entry_keeps_the_own_claim(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PF-6, Lesen fällt aus: Ausfall-Regel, der Claim bleibt (unbestimmt,
    ob der Eintrag ihn trägt); der Fehler erreicht den Aufrufer."""
    await _add("nightly")
    monkeypatch.setattr(ScheduleStore, "get_definition", AsyncMock(side_effect=BackendConnectionError("timeout")))

    with pytest.raises(BackendConnectionError):
        await _add("nightly", "--unique-for", "60", cron="0 2 * * *")

    assert await _claim_owner(memory) == "nightly"


@pytest.mark.entscheidung("E-076")
async def test_held_own_claim_after_fired_oneshot_reports_already_exists_false(memory: InMemoryBackend) -> None:
    """Der Leader hat den Oneshot gefeuert und gelöscht, der Claim läuft noch:
    unter dem Namen existiert kein Eintrag."""
    await _add("once", "--unique-for", "60", at="2020-01-01T00:00:00Z")
    await memory.locks.acquire(SCHEDULER_LOCK_NAME, owner="leader", ttl_ms=60_000, max_concurrency=1)
    store = ScheduleStore(memory)
    [(name, score, _)] = await store.poll_due(_NOW_MS)
    assert await store.commit_fire(name, -1, None, "leader", fired_ts_ms=score)

    payload = await _add("once", "--unique-for", "60", at="2020-01-01T00:00:00Z")

    assert payload["inserted"] is False
    assert payload["already_exists"] is False
    assert payload["duplicate_of"] == "once"


@pytest.mark.entscheidung("E-076")
async def test_held_own_claim_without_entry_after_outage_reports_already_exists_false(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nach einem Ausfall beim Speichern blieb der Claim stehen (E-059), ein
    Eintrag entstand nicht."""
    real_add = ScheduleStore.add_schedule
    monkeypatch.setattr(ScheduleStore, "add_schedule", AsyncMock(side_effect=BackendConnectionError("timeout")))
    with pytest.raises(BackendConnectionError):
        await _add("nightly", "--unique-for", "60")
    monkeypatch.setattr(ScheduleStore, "add_schedule", real_add)

    payload = await _add("nightly", "--unique-for", "60")

    assert (payload["inserted"], payload["already_exists"], payload["duplicate_of"]) == (False, False, "nightly")


@pytest.mark.entscheidung("E-076")
async def test_held_own_claim_after_schedule_remove_reports_already_exists_false(memory: InMemoryBackend) -> None:
    """``schedule remove --permanent`` löscht den Eintrag, der Claim läuft weiter."""
    await _add("nightly", "--unique-for", "60")
    assert await ScheduleStore(memory).remove_schedule("nightly")

    payload = await _add("nightly", "--unique-for", "60")

    assert (payload["inserted"], payload["already_exists"], payload["duplicate_of"]) == (False, False, "nightly")


@pytest.mark.entscheidung("E-059")
async def test_outage_on_reading_the_entry_behind_the_held_own_claim_keeps_it(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lesen fällt aus: Ausfall-Regel, der Claim bleibt; der Fehler erreicht
    den Aufrufer."""
    await _add("nightly", "--unique-for", "60")
    monkeypatch.setattr(ScheduleStore, "get_definition", AsyncMock(side_effect=BackendConnectionError("timeout")))

    with pytest.raises(BackendConnectionError):
        await _add("nightly", "--unique-for", "60")

    assert await _claim_owner(memory) == "nightly"


@pytest.mark.entscheidung("E-059")
async def test_error_reply_on_reading_the_entry_behind_the_held_own_claim_keeps_it(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lesen mit Fehlerantwort: Der Claim gehört einem früheren ``add`` unter
    dem Namen, nicht diesem Aufruf; er bleibt stehen (E-059, Nacharbeit U18),
    der Fehler erreicht den Aufrufer."""
    await _add("nightly", "--unique-for", "60")
    monkeypatch.setattr(ScheduleStore, "get_definition", AsyncMock(side_effect=BackendResponseError("ERR wrongtype")))

    with pytest.raises(BackendResponseError):
        await _add("nightly", "--unique-for", "60")

    assert await _claim_owner(memory) == "nightly"


@pytest.mark.entscheidung("E-059")
async def test_cancel_while_storing_keeps_the_own_claim(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """0.13.1: ``sched_add.lua`` kann gelaufen sein (Strg+C, Timeout); der
    Claim bleibt wie nach einem Ausfall, der Abbruch geht durch."""
    monkeypatch.setattr(ScheduleStore, "add_schedule", AsyncMock(side_effect=asyncio.CancelledError()))

    with pytest.raises(asyncio.CancelledError):
        await _add("nightly", "--unique-for", "60")

    assert await _claim_owner(memory) == "nightly"


@pytest.mark.entscheidung("E-059")
async def test_cancel_while_reading_the_conflicting_entry_keeps_the_own_claim(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _add("nightly")
    monkeypatch.setattr(ScheduleStore, "get_definition", AsyncMock(side_effect=asyncio.CancelledError()))

    with pytest.raises(asyncio.CancelledError):
        await _add("nightly", "--unique-for", "60", cron="0 2 * * *")

    assert await _claim_owner(memory) == "nightly"


@pytest.mark.entscheidung("E-059")
async def test_other_error_while_storing_releases_the_own_claim(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ScheduleStore, "add_schedule", AsyncMock(side_effect=RuntimeError("bug")))

    with pytest.raises(RuntimeError):
        await _add("nightly", "--unique-for", "60")

    assert await _claim_owner(memory) is None
