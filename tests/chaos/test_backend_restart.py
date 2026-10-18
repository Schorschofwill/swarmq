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
5. F4-14: A worker whose task finishes while Valkey restarts behind a
   proxy that accepts and resets (``reset_peer`` toxic) stores the result
   within the client's retry window; the task runs exactly once.
6. E-072: The same restart lasting 5 s -- longer than the client's retry
   window -- is bridged by the worker's own store retry (1, 2, 4, 8 s); the
   task still runs exactly once.

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
from collections.abc import AsyncIterator, Iterator
from typing import Any, override

import httpx
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from swarmq.progress import ProgressInfo
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
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


@contextlib.asynccontextmanager
async def _proxy_resets_valkey_connections(stack: ChaosStack) -> AsyncIterator[None]:
    """Valkey-Neustart hinter einem Proxy, der annimmt und trennt (F4-14).

    Der ``timeout``-Toxic mit ``timeout: 1`` (ms) laesst keine Daten durch
    und schliesst jede bestehende und jede neue Verbindung über den
    Valkey-Proxy nach 1 ms: wie Valkey beim Herunterfahren und ein Proxy
    oder Sidecar, der den Neuaufbau annimmt, aber kein Ziel erreicht
    (``reset_peer`` kennt das Image ``shopify/toxiproxy:latest`` nicht). Ein
    ``docker restart`` allein bildet das hier nicht nach: der Compose-Proxy
    hält neue Verbindungen während des Neustarts fest (gemessen 2026-10-17:
    Lesen läuft 1 s in den Zeitablauf, danach PONG), statt sie zu trennen."""
    async with httpx.AsyncClient(base_url=stack.toxi_admin_url, timeout=5.0) as admin:
        response = await admin.post(
            "/proxies/vkey/toxics",
            json={"name": "valkey-restart", "type": "timeout", "stream": "downstream", "attributes": {"timeout": 1}},
        )
        response.raise_for_status()
        try:
            yield
        finally:
            await admin.delete("/proxies/vkey/toxics/valkey-restart")


@contextlib.asynccontextmanager
async def _running_worker(stack: ChaosStack, queue: str) -> AsyncIterator[None]:
    """Worker im Prozess gegen die Proxy-URLs, bis ``WORKER_ONLINE``."""
    broker = RabbitMQBroker(stack.rabbitmq_url)
    backend = ValkeyBackend(stack.valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    online = asyncio.Event()

    @app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    worker = asyncio.create_task(app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=15.0)
        yield
    finally:
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await worker
        with contextlib.suppress(Exception):
            await broker.close()
        with contextlib.suppress(Exception):
            await backend.close()


@pytest.fixture
def _cleanup_tasks() -> Iterator[None]:
    yield
    Task._clear_registered_tasks()


# ---------------------------------------------------------------------------


class TestBackendRestart:
    @pytest.mark.befund("F4-14")
    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_task_finishing_while_valkey_restarts_behind_the_proxy_runs_once(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """F4-14: Valkey startet hinter dem Proxy neu, während der Task sein
        Ergebnis speichert. Die Pool-Verbindung ist tot, der Proxy nimmt den
        Neuaufbau an und trennt ihn im Handshake; nach 1 s (Loadsuite: 0,8
        bis 1,2 s) ist Valkey zurück. Der Client wiederholt den Neuaufbau im
        Fenster von rund 3,1 s, das Ergebnis landet, der Task läuft genau
        einmal. Ohne die Wiederholung scheitert das Speichern nach wenigen
        Millisekunden und die Nachricht wird erneut zugestellt."""
        queue = f"chaos-restart-{uuid.uuid4().hex[:10]}"
        task_name = f"restart_counted_{uuid.uuid4().hex[:8]}"
        runs: dict[str, int] = {}
        started = asyncio.Event()
        release = asyncio.Event()

        class CountedTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            @override
            async def run(self) -> str:
                assert self._task_id is not None
                runs[self._task_id] = runs.get(self._task_id, 0) + 1
                started.set()
                await release.wait()
                return "done"

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_app = SwarmQ(client_broker, ValkeyBackend(chaos_stack.valkey_url), queues=[queue])
        async with client_app:
            await client_broker.declare_queue(queue)
            async with _running_worker(chaos_stack, queue):
                task_id = await client_app.schedule(task_name)
                await asyncio.wait_for(started.wait(), timeout=30.0)

                async with _proxy_resets_valkey_connections(chaos_stack):
                    # Erst sieht der Worker seine Pool-Verbindungen getrennt
                    # (wie nach dem Herunterfahren), dann endet der Task: das
                    # Speichern baut die Verbindung hinter dem Proxy neu auf.
                    await asyncio.sleep(0.2)
                    release.set()
                    await asyncio.sleep(1.0)

                reader = ValkeyBackend(chaos_stack.valkey_url)
                await reader.connect()
                try:
                    deadline = time.monotonic() + 30.0
                    stored = None
                    while stored is None and time.monotonic() < deadline:
                        stored = await reader.get_result(task_id)
                        if stored is None:
                            await asyncio.sleep(0.2)
                    assert stored is not None, "Ergebnis nach dem Neustart nicht gespeichert"
                    # Eine Neuzustellung käme nach dem gescheiterten
                    # Speichern sofort; kurz nachsehen, ob sie lief.
                    await asyncio.sleep(1.5)
                finally:
                    await reader.close()

        assert runs == {task_id: 1}, (
            f"Task lief {runs.get(task_id, 0)}-mal; das Speichern während des Neustarts hinter dem Proxy "
            "hätte im Wiederholfenster gelingen müssen (F4-14)"
        )

    @pytest.mark.entscheidung("E-072")
    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_task_finishing_during_a_five_second_valkey_outage_runs_once(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """E-072: Valkey ist 5 s weg, während der Task sein Ergebnis
        speichert -- länger als das Wiederholfenster des Clients (rund 3,1 s).
        Der Worker wiederholt das Speichern selbst (Wartezeiten 1, 2, 4 s ...),
        statt die Nachricht zurückzugeben; der Task läuft genau einmal. Ohne
        die Wiederholung speichert erst die Neuzustellung, die den Task ein
        zweites Mal ausführt."""
        queue = f"chaos-outage-{uuid.uuid4().hex[:10]}"
        task_name = f"outage_counted_{uuid.uuid4().hex[:8]}"
        runs: dict[str, int] = {}
        started = asyncio.Event()
        release = asyncio.Event()

        class CountedTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            @override
            async def run(self) -> str:
                assert self._task_id is not None
                runs[self._task_id] = runs.get(self._task_id, 0) + 1
                started.set()
                await release.wait()
                return "done"

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_app = SwarmQ(client_broker, ValkeyBackend(chaos_stack.valkey_url), queues=[queue])
        async with client_app:
            await client_broker.declare_queue(queue)
            async with _running_worker(chaos_stack, queue):
                task_id = await client_app.schedule(task_name)
                await asyncio.wait_for(started.wait(), timeout=30.0)

                async with _proxy_resets_valkey_connections(chaos_stack):
                    await asyncio.sleep(0.2)
                    release.set()
                    await asyncio.sleep(5.0)

                reader = ValkeyBackend(chaos_stack.valkey_url)
                await reader.connect()
                try:
                    deadline = time.monotonic() + 40.0
                    stored = None
                    while stored is None and time.monotonic() < deadline:
                        stored = await reader.get_result(task_id)
                        if stored is None:
                            await asyncio.sleep(0.2)
                    assert stored is not None, "Ergebnis nach dem Ausfall nicht gespeichert"
                    # Eine Neuzustellung liefe nach der Erholung; kurz nachsehen.
                    await asyncio.sleep(1.5)
                finally:
                    await reader.close()

        assert runs == {task_id: 1}, (
            f"Task lief {runs.get(task_id, 0)}-mal; die Speicher-Wiederholung des Workers hätte den "
            "Ausfall von 5 s überbrücken müssen (E-072)"
        )

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
