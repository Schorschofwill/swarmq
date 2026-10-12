"""Chaos U2.2 — F2 Backend (Valkey) restart.

Compose runs Valkey with ``--appendonly yes`` (see
``tests/chaos/compose.yml``) so every write that completes before the
restart is durable across the container bounce. Tests pin the
contract four ways:

1. ``get_result`` still returns the stored bytes after restart (AOF).
2. ``get_progress`` still returns the last HASH after restart (AOF).
3. A Pub/Sub subscriber kept alive across the restart re-subscribes via
   the :meth:`Client.watch_progress` fix shipped in Phase 0 and sees a
   post-restart publish — regression test for the toxiproxy-spike bug.
4. The cancel flag survives the outage (``is_cancelled`` stays True).

All tests use the ``chaos_stack`` fixture; the container is restarted
directly via ``docker restart {project}-valkey-1``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import time
import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.client import Client
from swarmq.progress import ProgressInfo
from tests.chaos.conftest import ChaosStack

pytestmark = [pytest.mark.timeout(120)]


def _docker_restart(container: str) -> None:
    subprocess.run(
        ["docker", "restart", container],
        check=True,
        capture_output=True,
        timeout=60,
    )


async def _wait_valkey_reachable(url: str, timeout: float = 30.0) -> None:
    import valkey.asyncio

    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client = valkey.asyncio.Valkey.from_url(url)
            try:
                await client.ping()
                return
            finally:
                await client.aclose()
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(0.3)
    raise RuntimeError(f"valkey not reachable within {timeout}s: {last_exc!r}")


# ---------------------------------------------------------------------------


class TestBackendRestart:
    async def test_result_storage_survives_restart(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """SET + restart + GET returns the original bytes (AOF on)."""
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        try:
            task_id = f"task-{uuid.uuid4().hex[:10]}"
            await backend.store_result(
                task_id,
                b"persistent-payload",
                ttl_ms=60_000,
            )
            # AOF flushes on every write (default: appendfsync
            # everysec). Let at least one fsync window pass so the
            # commit lands on disk.
            await asyncio.sleep(1.5)

            _docker_restart(f"{chaos_stack.project}-valkey-1")
            await _wait_valkey_reachable(chaos_stack.valkey_url)

            # New backend instance after restart — the old one's
            # socket has been torn down anyway, this keeps the test
            # reading on a fresh connection.
            backend2 = ValkeyBackend(chaos_stack.valkey_url)
            await backend2.connect()
            try:
                got = await backend2.get_result(task_id)
                assert got == b"persistent-payload", f"result not durable across restart: {got!r}"
            finally:
                await backend2.close()
        finally:
            with contextlib.suppress(Exception):
                await backend.close()

    async def test_progress_hash_survives_restart(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """Progress HASH is readable after restart (AOF on)."""
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        try:
            task_id = f"task-{uuid.uuid4().hex[:10]}"
            fields = {
                "current": "42",
                "total": "100",
                "percent": "42.5",
                "message": "half-way",
                "updated_at": "1700000000.0",
            }
            await backend.set_progress(task_id, fields, ttl_ms=60_000)
            await asyncio.sleep(1.5)  # AOF fsync window

            _docker_restart(f"{chaos_stack.project}-valkey-1")
            await _wait_valkey_reachable(chaos_stack.valkey_url)

            backend2 = ValkeyBackend(chaos_stack.valkey_url)
            await backend2.connect()
            try:
                raw = await backend2.get_progress(task_id)
                assert raw is not None, "progress HASH missing after restart — AOF off?"
                info = ProgressInfo.from_hash(raw)
                assert info is not None
                assert info.percent == pytest.approx(42.5)
                assert info.message == "half-way"
            finally:
                await backend2.close()
        finally:
            with contextlib.suppress(Exception):
                await backend.close()

    async def test_pubsub_subscriber_resubscribes_after_restart(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """watch_progress() reconnects + delivers a post-restart publish.

        Regression for the Phase-0 resubscribe fix: valkey-py's
        retry/backoff handles commands but NOT the long-held Pub/Sub
        socket. ``watch_progress`` must re-subscribe on its own so the
        caller sees updates after the backend bounced.
        """
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        try:
            # Use Client to get watch_progress (works without broker for
            # pure backend-only progress streaming).
            # Client needs a broker argument; a minimal unconnected
            # RabbitMQBroker works since watch_progress only touches
            # the backend.
            from swarmq.broker.rabbitmq import RabbitMQBroker

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            client = Client(broker, backend)

            task_id = f"task-{uuid.uuid4().hex[:10]}"

            # Publish a known HASH so the iterator's initial snapshot
            # is deterministic.
            await backend.set_progress(
                task_id,
                {
                    "current": "10",
                    "total": "100",
                    "percent": "10.0",
                    "message": "pre",
                    "updated_at": "1700000000.0",
                },
                ttl_ms=60_000,
            )

            received: list[ProgressInfo] = []
            iterator_ready = asyncio.Event()
            stop_collecting = asyncio.Event()

            async def collect() -> None:
                # Realistic-consumer loop: a full backend outage makes
                # the in-generator HASH fallback raise
                # BackendConnectionError (documented in
                # test_network_chaos.py::test_pubsub_disconnect_...),
                # which propagates out of the async generator. Callers
                # wrap watch_progress in a restart loop. The d2d23f2
                # resubscribe fix stays load-bearing because, without
                # it, a freshly-started iterator subscribes through the
                # same cached-dead pubsub and never sees live publishes.
                while not stop_collecting.is_set():
                    try:
                        async for info in client.watch_progress(task_id):
                            received.append(info)
                            iterator_ready.set()
                            if stop_collecting.is_set():
                                return
                    except asyncio.CancelledError:
                        raise
                    except BaseException:
                        await asyncio.sleep(0.3)

            collect_task = asyncio.create_task(collect())
            try:
                await iterator_ready.wait()
                # Give the subscriber time to enter its select loop
                # + yield the initial snapshot.
                await asyncio.sleep(0.5)
                assert received, (
                    "baseline failed: subscriber never saw the initial HASH snapshot — watch_progress wiring is broken"
                )

                _docker_restart(f"{chaos_stack.project}-valkey-1")
                await _wait_valkey_reachable(chaos_stack.valkey_url)
                # Let the in-generator resubscribe path land. Valkey-py's
                # pubsub socket is definitively dead after a container
                # restart; the except-handler in watch_progress needs
                # enough real time to see the raise, call
                # _subscribe_and_drain, and return to the get_message
                # loop. 4s covers the 0.5s backoff + socket reconnect.
                await asyncio.sleep(4.0)

                # New backend for publishing — original backend's
                # socket went down with the restart.
                pub_backend = ValkeyBackend(chaos_stack.valkey_url)
                await pub_backend.connect()
                try:
                    post_fields = {
                        "current": "90",
                        "total": "100",
                        "percent": "90.0",
                        "message": "post",
                        "updated_at": "1700000001.0",
                    }
                    # Prime the HASH so the resubscribe path's HASH
                    # fallback has a post-restart snapshot available.
                    await pub_backend.set_progress(
                        task_id,
                        post_fields,
                        ttl_ms=60_000,
                    )
                    # Spam publishes across a generous window — at
                    # least one must land after watch_progress has
                    # re-subscribed. Also re-write the HASH on every
                    # loop so the fallback path yields a fresh value
                    # when the exception-handler branch runs.
                    deadline = time.monotonic() + 30.0
                    while time.monotonic() < deadline:
                        if any(info.message == "post" for info in received):
                            break
                        await pub_backend.set_progress(
                            task_id,
                            post_fields,
                            ttl_ms=60_000,
                        )
                        await pub_backend.publish_progress(
                            task_id,
                            json.dumps(post_fields).encode(),
                        )
                        await asyncio.sleep(0.5)
                finally:
                    await pub_backend.close()

                assert any(info.message == "post" for info in received), (
                    f"subscriber never saw post-restart update; received={[i.message for i in received]!r}"
                )
            finally:
                stop_collecting.set()
                collect_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await collect_task
        finally:
            with contextlib.suppress(Exception):
                await backend.close()

    async def test_cancel_flag_survives_restart(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """set_cancel_flag → restart → is_cancelled still True (AOF on)."""
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        try:
            task_id = f"task-{uuid.uuid4().hex[:10]}"
            newly_set = await backend.set_cancel_flag(task_id, ttl_ms=120_000)
            assert newly_set is True
            await asyncio.sleep(1.5)  # AOF fsync

            _docker_restart(f"{chaos_stack.project}-valkey-1")
            await _wait_valkey_reachable(chaos_stack.valkey_url)

            backend2 = ValkeyBackend(chaos_stack.valkey_url)
            await backend2.connect()
            try:
                assert await backend2.is_cancelled(task_id) is True, "cancel flag lost across restart"
            finally:
                await backend2.close()
        finally:
            with contextlib.suppress(Exception):
                await backend.close()
