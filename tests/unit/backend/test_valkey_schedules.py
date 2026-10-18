"""``ValkeyScheduleStore`` (Plan U18): Vertrag ``ScheduleStore`` auf Bytes
gegen einen gemockten valkey-Client — Key-Wahl, Skript-Argumente und
Fehler-Wrapping. Der Store-Teil der früheren ``test_scheduler_store.py``.

Test-Liste 0.9.0 (Ausfall vs. Fehlerantwort, rollendes Upgrade):
1. Fehlerantwort von Valkey (``ResponseError``) -> ``BackendResponseError``;
   Transportfehler bleiben einfacher ``BackendConnectionError``.
2. ``commit_fire``: Skriptergebnis 2 (verworfen) -> ``True``.
3. ``commit_fire`` ohne ``fired_ts_ms`` sendet vier ARGV (altes Skript-
   Protokoll; rollendes Upgrade).

Test-Liste Plan 2026-10-08-1541 (U3, KTD12):
4. ``commit_fire`` mit ``fired_payload`` sendet sechs ARGV; ohne
   ``fired_ts_ms`` steht an fünfter Stelle ``""`` (kein Verwerf-Schutz).

Test-Liste Plan 0.12.0, MR 4 (BZ-07, Handover ohne SCAN):
5. ``set_handover_marker`` schreibt den festen Key ``sq:{sched}:handover``
   (Wert: Owner) und den Legacy-Key je Owner, den 0.11-Nachfolger per SCAN
   finden; beide mit derselben TTL.
6. ``take_handover_marker`` liest den festen Key per ``GETDEL``, ohne SCAN,
   und löscht den Legacy-Key des gefundenen Owners.
7. Mit ``previous_owner`` erkennt es auch den Legacy-Key eines 0.11-Leaders.

Test-Liste Plan 2026-10-14-0748 (U6, KTD7, FS-06a):
8. ``claim_fire`` mit ``owner`` haengt den eigenen Owner an die
   Uebernahme-Tokens; ohne ``owner`` nichts (Argumente ``["1", ttl, ""]``).

Test-Liste Plan 2026-10-15-1138 (U3, KTD4, F3-01):
9. ``add`` ruft ``sched_add.lua`` mit ``keys=[defs, zset]`` und
   ``args=[name, payload, fire_ts]``; ohne Fire-Zeitpunkt ``""``.
10. Skriptergebnis 1 -> ``True``, 0 -> ``False``.
11. Ein Skriptfehler wird ``BackendConnectionError``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.backend.valkey.schedules import DEFS_KEY, ZSET_KEY, ValkeyScheduleStore
from swarmq.exceptions import BackendConnectionError, BackendResponseError
from swarmq.locking import SCHEDULER_LOCK_KEY

pytestmark = pytest.mark.timeout(5)


def _await_kwargs(mock: AsyncMock) -> Mapping[str, Any]:
    """Keyword-Argumente des letzten ``await`` auf ``mock``."""
    call = mock.await_args
    assert call is not None
    return call.kwargs


def _store(*, hexists: int = 1, hsetnx: int = 1) -> tuple[ValkeyScheduleStore, MagicMock, dict[str, AsyncMock]]:
    client = MagicMock()
    scripts = {
        "poll": AsyncMock(return_value=[]),
        "commit": AsyncMock(return_value=1),
        "disable": AsyncMock(return_value=1),
        "replan": AsyncMock(return_value=1),
        "fire_claim": AsyncMock(return_value=1),
        "owner_release": AsyncMock(return_value=1),
        "report": AsyncMock(return_value=1),
        "report_prune": AsyncMock(return_value=0),
        "prune": AsyncMock(return_value=1),
        "add": AsyncMock(return_value=hsetnx),
    }
    client.register_script = MagicMock(side_effect=list(scripts.values()))
    client.hexists = AsyncMock(return_value=hexists)
    client.hsetnx = AsyncMock(return_value=hsetnx)
    client.hset = AsyncMock()
    client.hget = AsyncMock(return_value=b"def")
    client.hdel = AsyncMock()
    client.hgetall = AsyncMock(return_value={b"a": b"A"})
    client.zadd = AsyncMock()
    client.zrem = AsyncMock()
    client.set = AsyncMock(return_value=True)
    client.exists = AsyncMock(return_value=1)
    client.delete = AsyncMock()
    return ValkeyScheduleStore(client), client, scripts


class TestCrud:
    async def test_add_runs_sched_add_script_with_defs_and_zset(self) -> None:
        """Liste 9/10 (F3-01): Definition und Fire-Index in einem EVAL."""
        store, client, scripts = _store(hsetnx=1)
        assert await store.add("s", b"payload", 1_700) is True
        scripts["add"].assert_awaited_once_with(keys=[DEFS_KEY, ZSET_KEY], args=["s", b"payload", 1_700])
        client.hsetnx.assert_not_awaited()
        client.zadd.assert_not_awaited()

    async def test_add_existing_reports_false(self) -> None:
        """Liste 10: Skriptergebnis 0 -> ``False`` (Name existiert)."""
        store, _, _ = _store(hsetnx=0)
        assert await store.add("s", b"payload", 1_700) is False

    async def test_add_without_fire_time_passes_empty_argument(self) -> None:
        """Liste 9: ohne Fire-Zeitpunkt ``""`` (kein Index-Eintrag)."""
        store, _, scripts = _store()
        assert await store.add("s", b"payload", None) is True
        assert _await_kwargs(scripts["add"])["args"] == ["s", b"payload", ""]

    async def test_put_overwrites_and_indexes(self) -> None:
        store, client, _ = _store()
        await store.put("s", b"p2", 1_800)
        client.hset.assert_awaited_once_with(DEFS_KEY, "s", b"p2")
        client.zadd.assert_awaited_once_with(ZSET_KEY, {"s": 1_800})

    async def test_remove_reports_prior_existence(self) -> None:
        store, client, _ = _store(hexists=1)
        assert await store.remove("s") is True
        client.zrem.assert_awaited_once_with(ZSET_KEY, "s")
        client.hdel.assert_awaited_once_with(DEFS_KEY, "s")
        store2, _, _ = _store(hexists=0)
        assert await store2.remove("ghost") is False

    async def test_list_all_decodes_names(self) -> None:
        store, _, _ = _store()
        assert await store.list_all() == {"a": b"A"}


class TestScripts:
    async def test_disable_calls_script_with_defs_and_zset(self) -> None:
        store, _, scripts = _store()
        assert await store.disable("s", b"disabled") is True
        call_kwargs = _await_kwargs(scripts["disable"])
        assert call_kwargs["keys"] == [DEFS_KEY, ZSET_KEY]
        assert call_kwargs["args"] == ["s", b"disabled"]

    async def test_replan_if_unchanged_calls_script_with_defs_and_zset(self) -> None:
        store, _, scripts = _store()
        scripts["replan"].return_value = 0
        assert await store.replan_if_unchanged("s", b"read", b"healed", 4_000) is False
        scripts["replan"].assert_awaited_once_with(
            keys=[DEFS_KEY, ZSET_KEY],
            args=["s", b"read", b"healed", 4_000],
        )

    async def test_poll_due_decodes_rows(self) -> None:
        store, _, scripts = _store()
        scripts["poll"].return_value = [(b"s", b"1700", b"payload")]
        assert await store.poll_due(2_000, limit=5) == [("s", 1700, b"payload")]
        assert _await_kwargs(scripts["poll"]) == {"keys": [ZSET_KEY, DEFS_KEY], "args": [2_000, 5]}

    async def test_commit_fire_passes_leader_key_and_owner(self) -> None:
        store, _, scripts = _store()
        assert await store.commit_fire("s", next_fire_ts_ms=1_900, payload=b"next", owner="chief") is True
        call_kwargs = _await_kwargs(scripts["commit"])
        assert call_kwargs["keys"] == [ZSET_KEY, DEFS_KEY, SCHEDULER_LOCK_KEY]
        assert call_kwargs["args"] == ["s", 1_900, b"next", "chief"]

    async def test_commit_fire_one_shot_encodes_removal(self) -> None:
        store, _, scripts = _store()
        await store.commit_fire("once", next_fire_ts_ms=None, payload=None, owner="chief")
        assert _await_kwargs(scripts["commit"])["args"] == ["once", -1, b"", "chief"]

    async def test_commit_fire_superseded_result_counts_as_committed(self) -> None:
        """Liste 2: verworfen (Ergebnis 2) ist kein Owner-Konflikt."""
        store, _, scripts = _store()
        scripts["commit"].return_value = 2
        assert await store.commit_fire("s", next_fire_ts_ms=1, payload=b"x", owner="chief", fired_ts_ms=1_000) is True

    async def test_commit_fire_sends_fired_ts_only_when_given(self) -> None:
        """Liste 3: ohne ``fired_ts_ms`` vier ARGV -- das Protokoll eines
        Leaders vor 0.9.0, den das Skript im rollenden Upgrade weiter
        bedient (ohne Verwerf-Pruefung); mit ``fired_ts_ms`` fuenf."""
        store, _, scripts = _store()
        await store.commit_fire("s", next_fire_ts_ms=1_900, payload=b"next", owner="chief")
        assert _await_kwargs(scripts["commit"])["args"] == ["s", 1_900, b"next", "chief"]
        await store.commit_fire("s", next_fire_ts_ms=1_900, payload=b"next", owner="chief", fired_ts_ms=1_000)
        assert _await_kwargs(scripts["commit"])["args"] == ["s", 1_900, b"next", "chief", 1_000]

    async def test_commit_fire_sends_fired_payload_as_sixth_argv(self) -> None:
        """Liste 4."""
        store, _, scripts = _store()
        await store.commit_fire(
            "s",
            next_fire_ts_ms=1_900,
            payload=b"next",
            owner="chief",
            fired_ts_ms=1_000,
            fired_payload=b"polled",
        )
        assert _await_kwargs(scripts["commit"])["args"] == ["s", 1_900, b"next", "chief", 1_000, b"polled"]
        await store.commit_fire("s", next_fire_ts_ms=1_900, payload=b"next", owner="chief", fired_payload=b"polled")
        assert _await_kwargs(scripts["commit"])["args"] == ["s", 1_900, b"next", "chief", "", b"polled"]

    @pytest.mark.entscheidung("E-047")
    async def test_commit_fire_rejected_by_owner_check(self) -> None:
        store, _, scripts = _store()
        scripts["commit"].return_value = 0
        assert await store.commit_fire("s", next_fire_ts_ms=1, payload=b"x", owner="impostor") is False


class TestErrorWrapping:
    async def test_driver_errors_become_backend_connection_error(self) -> None:
        """Liste 11: ein Transportfehler im ``sched_add``-Skript."""
        store, _, scripts = _store()
        scripts["add"].side_effect = ValkeyConnectionError("down")
        with pytest.raises(BackendConnectionError):
            await store.add("s", b"p", None)

    async def test_script_errors_are_wrapped_with_context(self) -> None:
        store, _, scripts = _store()
        scripts["commit"].side_effect = RuntimeError("boom")
        with pytest.raises(BackendConnectionError, match="sched_commit for 's' failed"):
            await store.commit_fire("s", next_fire_ts_ms=1, payload=b"x", owner="o")


class TestSchedfireClaims:
    """Schedfire-Primitive (U15: früher Fassaden-Aliase, jetzt Store-Vertrag)."""

    async def test_mark_fire_completed_true_when_new(self) -> None:
        store, client, _ = _store()
        client.set = AsyncMock(return_value=True)
        assert await store.mark_fire_completed("cron_a:1700000000000", 60_000) is True
        client.set.assert_awaited_once_with("sq:sched:fired:cron_a:1700000000000", "1", nx=True, px=60_000)

    async def test_mark_fire_completed_false_on_duplicate(self) -> None:
        store, client, _ = _store()
        client.set = AsyncMock(return_value=None)  # NX already exists
        assert await store.mark_fire_completed("cron_a:1700000000000", 60_000) is False

    async def test_claim_fire_and_release(self) -> None:
        store, client, scripts = _store()
        assert await store.claim_fire("k", 5_000) is True
        scripts["fire_claim"].assert_awaited_once_with(keys=["sq:sched:inflight:k"], args=["1", 5_000, ""])
        await store.release_fire_claim("k")
        client.delete.assert_awaited_once_with("sq:sched:inflight:k")

    async def test_claim_fire_with_owner_and_takeover(self) -> None:
        store, _, scripts = _store()
        scripts["fire_claim"].return_value = 0
        assert await store.claim_fire("k", 5_000, owner="b", takeover="a") is False
        scripts["fire_claim"].assert_awaited_once_with(keys=["sq:sched:inflight:k"], args=["b", 5_000, "a", "b"])

    async def test_claim_fire_passes_every_takeover_token(self) -> None:
        store, _, scripts = _store()
        await store.claim_fire("k", 5_000, owner="c", takeover=("b", "a"))
        scripts["fire_claim"].assert_awaited_once_with(keys=["sq:sched:inflight:k"], args=["c", 5_000, "b", "a", "c"])

    async def test_claim_fire_offers_own_owner_as_takeover_token(self) -> None:
        """FS-06a: der eigene Owner steht unter den Uebernahme-Tokens, damit
        die Wiederholung nach verlorener Antwort den eigenen Claim trifft."""
        store, _, scripts = _store()
        await store.claim_fire("k", 5_000, owner="b")
        scripts["fire_claim"].assert_awaited_once_with(keys=["sq:sched:inflight:k"], args=["b", 5_000, "b"])

    async def test_release_fire_claim_with_owner_uses_compare_and_delete(self) -> None:
        store, client, scripts = _store()
        await store.release_fire_claim("k", owner="a")
        scripts["owner_release"].assert_awaited_once_with(keys=["sq:sched:inflight:k"], args=["a"])
        client.delete.assert_not_awaited()

    async def test_claim_fire_transport_error_is_backend_connection_error(self) -> None:
        store, _, scripts = _store()
        scripts["fire_claim"].side_effect = ValkeyConnectionError("down")
        with pytest.raises(BackendConnectionError):
            await store.claim_fire("k", 5_000, owner="b")

    async def test_claim_fire_error_reply_is_a_response_error(self) -> None:
        """Liste 1."""
        from valkey.exceptions import ResponseError

        store, _, scripts = _store()
        scripts["fire_claim"].side_effect = ResponseError("WRONGTYPE")
        with pytest.raises(BackendResponseError):
            await store.claim_fire("k", 5_000, owner="b")

    async def test_claim_fire_transport_error_is_a_plain_outage(self) -> None:
        """Liste 1."""
        store, _, scripts = _store()
        scripts["fire_claim"].side_effect = ValkeyConnectionError("down")
        with pytest.raises(BackendConnectionError) as exc_info:
            await store.claim_fire("k", 5_000, owner="b")
        assert not isinstance(exc_info.value, BackendResponseError)

    async def test_is_fire_completed(self) -> None:
        store, client, _ = _store()
        client.exists = AsyncMock(return_value=1)
        assert await store.is_fire_completed("k") is True
        client.exists.assert_awaited_once_with("sq:sched:fired:k")

    async def test_handover_marker_roundtrip_names(self) -> None:
        """Liste 5 (BZ-07)."""
        store, client, _ = _store()
        client.set = AsyncMock(return_value=True)
        await store.set_handover_marker("owner-1", 5_000)
        assert [(c.args, c.kwargs) for c in client.set.await_args_list] == [
            (("sq:{sched}:handover", "owner-1"), {"px": 5_000}),
            (("sq:sched:handover:owner-1", "1"), {"px": 5_000}),
        ]

    async def test_take_handover_marker_reads_fixed_key_without_scan(self) -> None:
        """Liste 6 (BZ-07)."""
        store, client, _ = _store()
        client.getdel = AsyncMock(return_value=b"owner-1")
        client.scan_iter = MagicMock(side_effect=AssertionError("no SCAN"))
        assert await store.take_handover_marker() is True
        client.getdel.assert_awaited_once_with("sq:{sched}:handover")
        client.delete.assert_awaited_once_with("sq:sched:handover:owner-1")

    async def test_take_handover_marker_finds_legacy_marker_of_previous_owner(self) -> None:
        """Liste 7 (BZ-07): ein 0.11-Leader schreibt nur den Legacy-Key."""
        store, client, _ = _store()
        client.getdel = AsyncMock(side_effect=lambda key: b"1" if key == "sq:sched:handover:old" else None)
        assert await store.take_handover_marker(previous_owner="old") is True
        assert [c.args for c in client.getdel.await_args_list] == [("sq:{sched}:handover",), ("sq:sched:handover:old",)]

    async def test_wrapping_on_driver_error(self) -> None:
        store, client, _ = _store()
        client.set = AsyncMock(side_effect=ValkeyConnectionError("down"))
        with pytest.raises(BackendConnectionError):
            await store.mark_fire_completed("k", 1_000)


def _scan(*keys: bytes, error: BaseException | None = None) -> MagicMock:
    async def _iter(**_kwargs):
        for key in keys:
            yield key
        if error is not None:
            raise error

    return MagicMock(side_effect=_iter)


class TestWorkerVersions:
    """``worker_versions`` (KTD11, nur ``schedule status``): SCAN + MGET."""

    async def test_no_worker_keys_returns_empty_without_mget(self) -> None:
        store, client, _ = _store()
        client.scan_iter = _scan()
        client.mget = AsyncMock()
        assert await store.worker_versions() == {}
        client.mget.assert_not_awaited()

    @pytest.mark.parametrize("error", [ValkeyConnectionError("down"), OSError("reset")])
    async def test_scan_transport_error_is_a_plain_outage(self, error: BaseException) -> None:
        store, client, _ = _store()
        client.scan_iter = _scan(b"sq:{sched}:worker_version:w-1", error=error)
        with pytest.raises(BackendConnectionError) as exc_info:
            await store.worker_versions()
        assert not isinstance(exc_info.value, BackendResponseError)

    async def test_versions_are_keyed_by_worker_and_expired_keys_skipped(self) -> None:
        store, client, _ = _store()
        client.scan_iter = _scan(b"sq:{sched}:worker_version:w-1", b"sq:{sched}:worker_version:w-2")
        client.mget = AsyncMock(return_value=[b"0.10.0", None])
        assert await store.worker_versions() == {"w-1": "0.10.0"}

    async def test_versions_are_keyed_by_the_raw_suffix_old_and_per_process(self) -> None:
        """F4-06 (KTD12): Schlüssel je Prozess (``<worker_identity>|<owner_token>``)
        und alte Schlüssel ohne ``|`` (Mischbetrieb) kommen roh zurück; die
        Aufteilung macht ``schedule status``."""
        store, client, _ = _store()
        client.scan_iter = _scan(b"sq:{sched}:worker_version:h:0|h:11:aa", b"sq:{sched}:worker_version:h:0")
        client.mget = AsyncMock(return_value=[b"0.13.0", b"0.12.3"])
        assert await store.worker_versions() == {"h:0|h:11:aa": "0.13.0", "h:0": "0.12.3"}
