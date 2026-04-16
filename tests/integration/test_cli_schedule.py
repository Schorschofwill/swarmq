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

pytestmark = pytest.mark.integration


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
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _ns_list(format="table"):
    return argparse.Namespace(format=format)


def _ns_remove(name, permanent=False):
    return argparse.Namespace(name=name, permanent=permanent)


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
            raw = await client.hget("sq:sched:defs", name)
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
            raw = await client.hget("sq:sched:defs", name)
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
            assert await client.hget("sq:sched:defs", name) is None
            assert await client.zscore("sq:sched:zset", name) is None
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
            await client.set("sq:lock:scheduler:leader", "w1:42:abcdef12")

            buf = io.StringIO()
            with redirect_stdout(buf):
                await cli._run_schedule_status(_ns_status(format="json"))
            import json
            data = json.loads(buf.getvalue())
            assert data["leader"] == "w1:42:abcdef12"
        finally:
            await client.delete("sq:lock:scheduler:leader")
            await client.aclose()
