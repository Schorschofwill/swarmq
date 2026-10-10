"""Contract-Klasse für ``ResultBackend`` (Entwurf §2.3).

Fixtures eines Bindings: ``impl`` (verbunden), ``fresh_impl`` (unverbunden),
``unreachable_impl``, ``clock``. IDs werden pro Test eindeutig gebildet, damit
Bindings gegen ein geteiltes Valkey laufen können.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.contracts import ResultBackend
from swarmq.exceptions import BackendConnectionError
from tests.contracts._support import Clock


def _id(prefix: str = "ctr") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class ResultBackendContract:
    # --- Lebenszyklus ------------------------------------------------------

    async def test_connect_is_idempotent(self, fresh_impl: ResultBackend) -> None:
        await fresh_impl.connect()
        await fresh_impl.connect()
        await fresh_impl.ping()
        await fresh_impl.close()

    async def test_ping_before_connect_raises(self, fresh_impl: ResultBackend) -> None:
        with pytest.raises(BackendConnectionError):
            await fresh_impl.ping()

    async def test_driver_errors_are_wrapped_on_connect(self, unreachable_impl: ResultBackend) -> None:
        with pytest.raises(BackendConnectionError):
            await unreachable_impl.connect()

    # --- Ergebnisse -----------------------------------------------------------

    async def test_store_and_get_result(self, impl: ResultBackend) -> None:
        tid = _id()
        await impl.store_result(tid, b"\x01payload", ttl_ms=60_000)
        assert await impl.get_result(tid) == b"\x01payload"

    async def test_missing_result_is_none(self, impl: ResultBackend) -> None:
        assert await impl.get_result(_id()) is None

    async def test_result_expires_after_ttl(self, impl: ResultBackend, clock: Clock) -> None:
        tid = _id()
        await impl.store_result(tid, b"ttl", ttl_ms=500)
        assert await impl.get_result(tid) == b"ttl"
        await clock.advance(1.0)
        assert await impl.get_result(tid) is None

    async def test_delete_result(self, impl: ResultBackend) -> None:
        tid = _id()
        await impl.store_result(tid, b"x", ttl_ms=60_000)
        await impl.delete_result(tid)
        assert await impl.get_result(tid) is None

    async def test_get_results_preserves_order_and_gaps(self, impl: ResultBackend) -> None:
        a, b, c = _id(), _id(), _id()
        await impl.store_result(a, b"A", ttl_ms=60_000)
        await impl.store_result(c, b"C", ttl_ms=60_000)
        assert await impl.get_results([a, b, c]) == [b"A", None, b"C"]
        assert await impl.get_results([]) == []

    # --- Warten -------------------------------------------------------------

    async def test_wait_for_result_wakes_on_notification(self, impl: ResultBackend) -> None:
        tid = _id()

        async def producer() -> None:
            await asyncio.sleep(0.2)
            await impl.store_result(tid, b"done", ttl_ms=60_000)
            await impl.notify_result(tid)

        task = asyncio.create_task(producer())
        got = await impl.wait_for_result(tid, timeout=5.0)
        await task
        assert got == b"done"

    async def test_wait_for_result_falls_back_to_polling_without_notification(self, impl: ResultBackend) -> None:
        tid = _id()

        async def silent_producer() -> None:
            await asyncio.sleep(0.2)
            await impl.store_result(tid, b"quiet", ttl_ms=60_000)

        task = asyncio.create_task(silent_producer())
        got = await impl.wait_for_result(tid, timeout=5.0)
        await task
        assert got == b"quiet"

    async def test_wait_for_result_returns_immediately_when_present(self, impl: ResultBackend) -> None:
        tid = _id()
        await impl.store_result(tid, b"now", ttl_ms=60_000)
        assert await impl.wait_for_result(tid, timeout=0.05) == b"now"

    async def test_wait_for_result_returns_none_on_timeout(self, impl: ResultBackend) -> None:
        assert await impl.wait_for_result(_id(), timeout=0.2) is None

    async def test_notify_result_without_waiter_is_harmless(self, impl: ResultBackend) -> None:
        await impl.notify_result(_id())

    # --- Fortschritt ---------------------------------------------------------

    async def test_set_and_get_progress(self, impl: ResultBackend) -> None:
        tid = _id()
        await impl.set_progress(tid, {"current": "1", "total": "4"}, ttl_ms=60_000)
        assert await impl.get_progress(tid) == {b"current": b"1", b"total": b"4"}

    async def test_missing_progress_is_none(self, impl: ResultBackend) -> None:
        assert await impl.get_progress(_id()) is None

    async def test_watch_progress_yields_published_payloads(self, impl: ResultBackend) -> None:
        tid = _id()
        received: list[bytes] = []
        ready = asyncio.Event()

        async def watcher() -> None:
            stream = impl.watch_progress(tid)
            ready.set()
            async for payload in stream:
                received.append(payload)
                if len(received) == 2:
                    break

        task = asyncio.create_task(watcher())
        await ready.wait()
        await asyncio.sleep(0.2)  # Abonnement steht
        await impl.publish_progress(tid, b'{"current": "1"}')
        await impl.publish_progress(tid, b'{"current": "2"}')
        await asyncio.wait_for(task, timeout=5.0)
        assert received == [b'{"current": "1"}', b'{"current": "2"}']

    # --- Cancel ----------------------------------------------------------------

    async def test_cancel_flag_is_set_once(self, impl: ResultBackend) -> None:
        tid = _id()
        assert await impl.is_cancelled(tid) is False
        assert await impl.set_cancel_flag(tid, ttl_ms=60_000) is True
        assert await impl.set_cancel_flag(tid, ttl_ms=60_000) is False
        assert await impl.is_cancelled(tid) is True
        # BZ-23 (Plan 0.12.0, MR 5): clear verkürzt das Flag auf höchstens
        # 1 h statt es zu löschen; eine Neuzustellung bleibt storniert.
        await impl.clear_cancel_flag(tid)
        assert await impl.is_cancelled(tid) is True

    async def test_cancel_flag_expires(self, impl: ResultBackend, clock: Clock) -> None:
        tid = _id()
        await impl.set_cancel_flag(tid, ttl_ms=300)
        await clock.advance(0.8)
        assert await impl.is_cancelled(tid) is False

    async def test_clear_never_extends_a_shorter_cancel_flag(self, impl: ResultBackend, clock: Clock) -> None:
        # BZ-23: PEXPIRE … LT — die Abhilfe "kurze ttl_ms" bleibt intakt.
        tid = _id()
        await impl.set_cancel_flag(tid, ttl_ms=300)
        await impl.clear_cancel_flag(tid)
        await clock.advance(0.8)
        assert await impl.is_cancelled(tid) is False

    async def test_clear_missing_cancel_flag_is_harmless(self, impl: ResultBackend) -> None:
        await impl.clear_cancel_flag(_id())

    # --- Workflow --------------------------------------------------------------

    async def test_workflow_state_roundtrip(self, impl: ResultBackend) -> None:
        wf = _id("wf")
        assert await impl.get_workflow_state(wf) is None
        await impl.store_workflow_state(wf, b'{"v": 1}', ttl_ms=60_000)
        assert await impl.get_workflow_state(wf) == b'{"v": 1}'
        await impl.store_workflow_state(wf, b'{"v": 2}', ttl_ms=60_000)
        assert await impl.get_workflow_state(wf) == b'{"v": 2}'

    # --- Workflow-Zustand: Compare-and-Set (KTD11, R22) ------------------------
    #
    # Testliste: Treffer -> True + neuer Wert; Fehlversion -> False + alter Wert;
    # fehlende ``version`` im Bestand zaehlt als 0; fehlender Schluessel -> False;
    # TTL gilt fuer den neuen Wert.

    async def test_compare_and_store_workflow_state_matching_version(self, impl: ResultBackend) -> None:
        wf = _id("wf")
        await impl.store_workflow_state(wf, b'{"version": 0, "x": 1}', ttl_ms=60_000)
        stored = await impl.compare_and_store_workflow_state(
            wf, b'{"version": 1, "x": 2}', ttl_ms=60_000, expected_version=0
        )
        assert stored is True
        assert await impl.get_workflow_state(wf) == b'{"version": 1, "x": 2}'

    async def test_compare_and_store_workflow_state_rejects_stale_version(self, impl: ResultBackend) -> None:
        wf = _id("wf")
        await impl.store_workflow_state(wf, b'{"version": 2, "x": 1}', ttl_ms=60_000)
        stored = await impl.compare_and_store_workflow_state(
            wf, b'{"version": 2, "x": 9}', ttl_ms=60_000, expected_version=1
        )
        assert stored is False
        assert await impl.get_workflow_state(wf) == b'{"version": 2, "x": 1}'

    async def test_compare_and_store_workflow_state_missing_version_counts_as_zero(
        self, impl: ResultBackend
    ) -> None:
        """Dokumente aus 0.5.x tragen keine ``version`` -- sie gelten als 0."""
        wf = _id("wf")
        await impl.store_workflow_state(wf, b'{"x": 1}', ttl_ms=60_000)
        assert (
            await impl.compare_and_store_workflow_state(wf, b'{"version": 1}', ttl_ms=60_000, expected_version=1)
            is False
        )
        assert (
            await impl.compare_and_store_workflow_state(wf, b'{"version": 1}', ttl_ms=60_000, expected_version=0)
            is True
        )
        assert await impl.get_workflow_state(wf) == b'{"version": 1}'

    async def test_compare_and_store_workflow_state_missing_key_is_rejected(self, impl: ResultBackend) -> None:
        """Kein Bestand -> keine Schreibung; die Erstanlage ist ``store_workflow_state``."""
        wf = _id("wf")
        assert (
            await impl.compare_and_store_workflow_state(wf, b'{"version": 1}', ttl_ms=60_000, expected_version=0)
            is False
        )
        assert await impl.get_workflow_state(wf) is None

    async def test_compare_and_store_workflow_state_applies_ttl(self, impl: ResultBackend, clock: Clock) -> None:
        wf = _id("wf")
        await impl.store_workflow_state(wf, b'{"version": 0}', ttl_ms=60_000)
        assert (
            await impl.compare_and_store_workflow_state(wf, b'{"version": 1}', ttl_ms=500, expected_version=0)
            is True
        )
        assert await impl.get_workflow_state(wf) == b'{"version": 1}'
        await clock.advance(1.0)
        assert await impl.get_workflow_state(wf) is None

    async def test_workflow_cancel_flag(self, impl: ResultBackend) -> None:
        wf = _id("wf")
        assert await impl.is_workflow_cancelled(wf) is False
        assert await impl.set_workflow_cancel_flag(wf, ttl_ms=60_000) is True
        assert await impl.set_workflow_cancel_flag(wf, ttl_ms=60_000) is False
        assert await impl.is_workflow_cancelled(wf) is True

    async def test_wait_for_workflow_wakes_on_notification(self, impl: ResultBackend) -> None:
        wf = _id("wf")

        async def notifier() -> None:
            await asyncio.sleep(0.2)
            await impl.notify_workflow(wf)

        task = asyncio.create_task(notifier())
        assert await impl.wait_for_workflow(wf, timeout=5.0) is True
        await task

    async def test_wait_for_workflow_times_out(self, impl: ResultBackend) -> None:
        assert await impl.wait_for_workflow(_id("wf"), timeout=0.2) is False

    # --- Dedup -----------------------------------------------------------------

    async def test_claim_unique_reports_existing_owner(self, impl: ResultBackend) -> None:
        h = _id("hash")
        assert await impl.claim_unique(h, "owner-1", ttl_ms=60_000) is None
        assert await impl.claim_unique(h, "owner-2", ttl_ms=60_000) == "owner-1"

    async def test_release_unique_enforces_owner(self, impl: ResultBackend) -> None:
        h = _id("hash")
        await impl.claim_unique(h, "owner-1", ttl_ms=60_000)
        assert await impl.release_unique(h, "owner-2") is False
        assert await impl.claim_unique(h, "owner-3", ttl_ms=60_000) == "owner-1"
        assert await impl.release_unique(h, "owner-1") is True
        assert await impl.claim_unique(h, "owner-3", ttl_ms=60_000) is None

    async def test_release_unique_of_missing_flag_is_false(self, impl: ResultBackend) -> None:
        assert await impl.release_unique(_id("hash"), "nobody") is False

    async def test_unique_claim_expires(self, impl: ResultBackend, clock: Clock) -> None:
        h = _id("hash")
        await impl.claim_unique(h, "owner-1", ttl_ms=300)
        await clock.advance(0.8)
        assert await impl.claim_unique(h, "owner-2", ttl_ms=60_000) is None
