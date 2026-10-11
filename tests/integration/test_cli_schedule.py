"""Integration tests for 'swarmq schedule *' subcommands.

Drives the CLI handlers directly against real Valkey (no subprocess).
Covers the add/list/remove/status flow including persistence layout.
"""

from __future__ import annotations

import argparse
import io
import uuid
from contextlib import redirect_stdout

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq import cli
from swarmq.locking import SCHEDULER_LOCK_KEY

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


@pytest.fixture
def _env(monkeypatch, valkey_url):
    monkeypatch.setenv("SWARMQ_BACKEND_URL", valkey_url)


def _ns_add(**overrides) -> argparse.Namespace:
    base = {
        "name": "test",
        "cron": "*/5 * * * *",
        "at": None,
        "task": "ping",
        "args": "[]",
        "kwargs": "{}",
        "queue": "default",
        "format": "table",
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _ns_list(format="table"):  # noqa: A002 - spiegelt die CLI-Option --format, Aufrufer uebergeben format= per Schluesselwort
    return argparse.Namespace(format=format)


def _ns_remove(name, permanent=False, format="table"):  # noqa: A002 - spiegelt die CLI-Option --format, Aufrufer uebergeben format= per Schluesselwort
    return argparse.Namespace(name=name, permanent=permanent, format=format)


def _ns_status(limit=10, format="table"):  # noqa: A002 - spiegelt die CLI-Option --format, Aufrufer uebergeben format= per Schluesselwort
    return argparse.Namespace(limit=limit, format=format)


class TestAddThenList:
    @pytest.mark.usefixtures("_env")
    async def test_add_then_list_shows_entry(self, valkey_url):
        name = f"cron-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_list(_ns_list())
        assert name in buf.getvalue()

    @pytest.mark.usefixtures("_env")
    async def test_add_duplicate_is_no_op(self, valkey_url):
        name = f"cron-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_add(_ns_add(name=name))
        assert "already exists" in buf.getvalue()

    @pytest.mark.usefixtures("_env")
    async def test_list_json_format(self, valkey_url):
        name = f"cron-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_list(_ns_list(format="json"))
        import json

        data = json.loads(buf.getvalue())
        assert any(entry["schedule_name"] == name for entry in data)


class TestAddWithAt:
    @pytest.mark.usefixtures("_env")
    async def test_add_at_is_one_shot(self, valkey_url):
        name = f"one-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name, cron=None, at="2027-01-01T00:00:00Z"))

        client = valkey_asyncio.Valkey.from_url(valkey_url)
        try:
            raw = await client.hget("sq:{sched}:defs", name)
            assert raw is not None
            import msgspec

            from swarmq.scheduler import ScheduleDefinition

            d = msgspec.json.decode(raw, type=ScheduleDefinition)
            assert d.cron is None
            assert d.source == "cli"
        finally:
            await client.aclose()


class TestRemove:
    @pytest.mark.usefixtures("_env")
    async def test_remove_without_permanent_sets_disabled(self, valkey_url):
        name = f"d-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))
        await cli._run_schedule_remove(_ns_remove(name))

        client = valkey_asyncio.Valkey.from_url(valkey_url)
        try:
            raw = await client.hget("sq:{sched}:defs", name)
            assert raw is not None
            import msgspec

            from swarmq.scheduler import ScheduleDefinition

            d = msgspec.json.decode(raw, type=ScheduleDefinition)
            assert d.disabled is True
        finally:
            await client.aclose()

    @pytest.mark.usefixtures("_env")
    async def test_remove_permanent_deletes_entries(self, valkey_url):
        name = f"p-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))
        await cli._run_schedule_remove(_ns_remove(name, permanent=True))

        client = valkey_asyncio.Valkey.from_url(valkey_url)
        try:
            assert await client.hget("sq:{sched}:defs", name) is None
            assert await client.zscore("sq:{sched}:zset", name) is None
        finally:
            await client.aclose()


class TestStatus:
    @pytest.mark.usefixtures("_env")
    async def test_status_lists_none_when_empty(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_status(_ns_status())
        output = buf.getvalue()
        assert "LEADER: (none)" in output
        assert "(none in next 24h)" in output

    @pytest.mark.usefixtures("_env")
    async def test_status_json_leader_none_upcoming_empty(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_status(_ns_status(format="json"))
        import json

        data = json.loads(buf.getvalue())
        assert data["leader"] is None
        assert data["upcoming"] == []

    @pytest.mark.usefixtures("_env")
    async def test_status_shows_leader_when_lock_held(self, valkey_url):
        client = valkey_asyncio.Valkey.from_url(valkey_url)
        try:
            await client.set(SCHEDULER_LOCK_KEY, "w1:42:abcdef12")

            buf = io.StringIO()
            with redirect_stdout(buf):
                await cli._run_schedule_status(_ns_status(format="json"))
            import json

            data = json.loads(buf.getvalue())
            assert data["leader"] == "w1:42:abcdef12"
        finally:
            await client.delete(SCHEDULER_LOCK_KEY)
            await client.aclose()

    @pytest.mark.usefixtures("_env")
    async def test_status_shows_leader_and_worker_versions(self, valkey_url):
        """U13 (KTD11): Leader-Version neben dem Lock, Worker-Versionen."""
        import json

        from swarmq.backend.valkey.schedules import ValkeyScheduleStore

        client = valkey_asyncio.Valkey.from_url(valkey_url)
        schedules = ValkeyScheduleStore(client)
        worker = f"host-{uuid.uuid4().hex[:8]}:0"
        try:
            await client.set(SCHEDULER_LOCK_KEY, "w1:42:abcdef12")
            await schedules.set_leader_version("w1:42:abcdef12", "0.10.0", ttl_ms=60_000)
            await schedules.report_worker_version(worker, "0.10.0", ttl_ms=60_000)

            buf = io.StringIO()
            with redirect_stdout(buf):
                await cli._run_schedule_status(_ns_status(format="json"))
            data = json.loads(buf.getvalue())
            assert data["leader_version"] == "0.10.0"
            assert {"worker_id": worker, "version": "0.10.0"} in data["workers"]

            buf = io.StringIO()
            with redirect_stdout(buf):
                await cli._run_schedule_status(_ns_status(format="table"))
            assert "LEADER_VERSION: 0.10.0\n" in buf.getvalue()
            assert f"  {worker}  0.10.0\n" in buf.getvalue()
        finally:
            await client.delete(SCHEDULER_LOCK_KEY)
            await client.aclose()


class TestAddRemoveJsonOutput:
    """Unit 4 / M-2: agent-parseable JSON output for mutating
    subcommands. Boolean fields let agents distinguish insert-vs-
    no-op and removed-vs-missing without parsing prose."""

    @pytest.mark.usefixtures("_env")
    async def test_add_json_reports_inserted_true(self, valkey_url):
        name = f"add-{uuid.uuid4().hex[:8]}"
        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_add(_ns_add(name=name, format="json"))
        import json

        data = json.loads(buf.getvalue())
        assert data == {"inserted": True, "name": name, "already_exists": False}

    @pytest.mark.usefixtures("_env")
    async def test_add_json_reports_already_exists(self, valkey_url):
        name = f"dup-{uuid.uuid4().hex[:8]}"
        # First add succeeds
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_add(_ns_add(name=name, format="json"))
        import json

        data = json.loads(buf.getvalue())
        assert data == {"inserted": False, "name": name, "already_exists": True}

    @pytest.mark.usefixtures("_env")
    async def test_remove_json_reports_removed_true(self, valkey_url):
        name = f"rm-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_remove(_ns_remove(name, permanent=True, format="json"))
        import json

        data = json.loads(buf.getvalue())
        assert data == {"removed": True, "permanent": True, "name": name}

    @pytest.mark.usefixtures("_env")
    async def test_remove_json_reports_missing(self, valkey_url):
        """Missing name → removed=false, exit still 0. Agents can tell
        no-op from success via the boolean."""
        name = f"ghost-{uuid.uuid4().hex[:8]}"

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_remove(_ns_remove(name, permanent=True, format="json"))
        import json

        data = json.loads(buf.getvalue())
        assert data == {"removed": False, "permanent": True, "name": name}

    @pytest.mark.usefixtures("_env")
    async def test_remove_disable_json_reports_removed_bool(self, valkey_url):
        """Non-permanent remove also reports a removed-bool in JSON
        mode. True when an entry was disabled, False on no-op."""
        name = f"dis-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_remove(_ns_remove(name, permanent=False, format="json"))
        import json

        data = json.loads(buf.getvalue())
        assert data == {"removed": True, "permanent": False, "name": name}


class TestPruneOrphans:
    """U10 (KTD8, R13): ``schedule prune --orphans`` und LAST_SEEN in
    ``schedule list`` gegen echtes Valkey."""

    @staticmethod
    def _static(name: str) -> bytes:
        import msgspec

        from swarmq.scheduler import ScheduleDefinition

        return msgspec.json.encode(
            ScheduleDefinition(
                schedule_name=name,
                task_name=name,
                args=(),
                kwargs={},
                queue="default",
                cron="0 1 * * *",
                fire_ts_ms=4_000_000_000_000,
                source="static",
            )
        )

    @pytest.mark.usefixtures("_env")
    async def test_prune_orphans_removes_only_unreported_static_crons(self, valkey_url):
        import json

        from swarmq.backend.valkey.schedules import ValkeyScheduleStore

        legacy, reported = f"legacy-{uuid.uuid4().hex[:8]}", f"rep-{uuid.uuid4().hex[:8]}"
        by_cli = f"cli-{uuid.uuid4().hex[:8]}"
        client = valkey_asyncio.Valkey.from_url(valkey_url)
        schedules = ValkeyScheduleStore(client)
        try:
            await schedules.add(legacy, self._static(legacy), 4_000_000_000_000)
            await schedules.add(reported, self._static(reported), 4_000_000_000_000)
            await schedules.report_cron(reported, "fp", self._static(reported), live_window_ms=60_000)
            await cli._run_schedule_add(_ns_add(name=by_cli))

            buf = io.StringIO()
            with redirect_stdout(buf):
                await cli._run_schedule_list(_ns_list(format="json"))
            seen = {e["schedule_name"]: e["last_seen_ms"] for e in json.loads(buf.getvalue())}
            assert seen[legacy] is None
            assert seen[by_cli] is None
            assert isinstance(seen[reported], int)

            buf = io.StringIO()
            with redirect_stdout(buf):
                await cli._run_schedule_async(
                    argparse.Namespace(
                        schedule_command="prune",
                        orphans=True,
                        format="json",
                    )
                )
            assert json.loads(buf.getvalue()) == {"removed": [legacy]}
            assert await client.hget("sq:{sched}:defs", legacy) is None
            assert await client.zscore("sq:{sched}:zset", legacy) is None
            assert await client.hget("sq:{sched}:defs", reported) is not None
            assert await client.hget("sq:{sched}:defs", by_cli) is not None
        finally:
            await client.aclose()
