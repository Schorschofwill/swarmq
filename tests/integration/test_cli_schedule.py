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
    yield


def _ns_add(**overrides) -> argparse.Namespace:
    base = dict(
        name="test",
        cron="*/5 * * * *",
        at=None,
        task="ping",
        args="[]",
        kwargs="{}",
        queue="default",
        format="table",
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _ns_list(format="table"):
    return argparse.Namespace(format=format)


def _ns_remove(name, permanent=False, format="table"):
    return argparse.Namespace(
        name=name, permanent=permanent, format=format
    )


def _ns_status(limit=10, format="table"):
    return argparse.Namespace(limit=limit, format=format)


class TestAddThenList:
    async def test_add_then_list_shows_entry(self, _env, valkey_url):
        name = f"cron-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_list(_ns_list())
        assert name in buf.getvalue()

    async def test_add_duplicate_is_no_op(self, _env, valkey_url):
        name = f"cron-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_add(_ns_add(name=name))
        assert "already exists" in buf.getvalue()

    async def test_list_json_format(self, _env, valkey_url):
        name = f"cron-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_list(_ns_list(format="json"))
        import json
        data = json.loads(buf.getvalue())
        assert any(entry["schedule_name"] == name for entry in data)


class TestAddWithAt:
    async def test_add_at_is_one_shot(self, _env, valkey_url):
        name = f"one-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(
            name=name, cron=None, at="2027-01-01T00:00:00Z"
        ))

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
    async def test_remove_without_permanent_sets_disabled(
        self, _env, valkey_url
    ):
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

    async def test_remove_permanent_deletes_entries(
        self, _env, valkey_url
    ):
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
    async def test_status_lists_none_when_empty(self, _env):
        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_status(_ns_status())
        output = buf.getvalue()
        assert "LEADER: (none)" in output
        assert "(none in next 24h)" in output

    async def test_status_json_leader_none_upcoming_empty(self, _env):
        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_status(_ns_status(format="json"))
        import json
        data = json.loads(buf.getvalue())
        assert data["leader"] is None
        assert data["upcoming"] == []

    async def test_status_shows_leader_when_lock_held(self, _env, valkey_url):
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

    async def test_status_shows_leader_and_worker_versions(self, _env, valkey_url):
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

    async def test_add_json_reports_inserted_true(self, _env, valkey_url):
        name = f"add-{uuid.uuid4().hex[:8]}"
        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_add(_ns_add(name=name, format="json"))
        import json
        data = json.loads(buf.getvalue())
        assert data == {"inserted": True, "name": name, "already_exists": False}

    async def test_add_json_reports_already_exists(self, _env, valkey_url):
        name = f"dup-{uuid.uuid4().hex[:8]}"
        # First add succeeds
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_add(_ns_add(name=name, format="json"))
        import json
        data = json.loads(buf.getvalue())
        assert data == {"inserted": False, "name": name, "already_exists": True}

    async def test_remove_json_reports_removed_true(self, _env, valkey_url):
        name = f"rm-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_remove(
                _ns_remove(name, permanent=True, format="json")
            )
        import json
        data = json.loads(buf.getvalue())
        assert data == {"removed": True, "permanent": True, "name": name}

    async def test_remove_json_reports_missing(self, _env, valkey_url):
        """Missing name → removed=false, exit still 0. Agents can tell
        no-op from success via the boolean."""
        name = f"ghost-{uuid.uuid4().hex[:8]}"

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_remove(
                _ns_remove(name, permanent=True, format="json")
            )
        import json
        data = json.loads(buf.getvalue())
        assert data == {"removed": False, "permanent": True, "name": name}

    async def test_remove_disable_json_reports_removed_bool(
        self, _env, valkey_url
    ):
        """Non-permanent remove also reports a removed-bool in JSON
        mode. True when an entry was disabled, False on no-op."""
        name = f"dis-{uuid.uuid4().hex[:8]}"
        await cli._run_schedule_add(_ns_add(name=name))

        buf = io.StringIO()
        with redirect_stdout(buf):
            await cli._run_schedule_remove(
                _ns_remove(name, permanent=False, format="json")
            )
        import json
        data = json.loads(buf.getvalue())
        assert data == {"removed": True, "permanent": False, "name": name}
