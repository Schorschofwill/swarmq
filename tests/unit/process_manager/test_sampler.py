"""Sampler-Tests (Plan U21): Factory-Seam (``AdminFactory``) — Timeout nach
``SAMPLE_TIMEOUT_S``, Multi-Queue-Summe, Factory genau einmal pro Sample."""

from __future__ import annotations

import asyncio

from swarmq.process_manager.sampler import _make_queue_depth_sampler


class FakeAdmin:
    def __init__(self, depths=None, hang=False):
        self.depths = depths or {}
        self.hang = hang
        self.connect_calls = 0
        self.closed = False

    async def connect(self):
        self.connect_calls += 1
        if self.hang:
            await asyncio.sleep(3600)

    async def queue_depth(self, queue):
        return self.depths.get(queue, 0)

    async def close(self):
        self.closed = True


class TestQueueDepthSampler:
    def test_sums_multi_queue_depth(self):
        admin = FakeAdmin(depths={"a": 3, "b": 7})
        factory_calls = []

        def factory():
            factory_calls.append(1)
            return admin

        sampler = _make_queue_depth_sampler(factory, ["a", "b", "c"])
        # "c" existiert nicht → zählt 0 (Fake liefert 0 wie queue_depth
        # für eine frisch deklarierte/leere Queue).
        assert sampler() == 10
        assert admin.closed is True

    def test_factory_called_exactly_once_per_sample(self):
        admin = FakeAdmin()
        factory_calls = []

        def factory():
            factory_calls.append(1)
            return admin

        sampler = _make_queue_depth_sampler(factory, ["a"])
        sampler()
        assert len(factory_calls) == 1
        sampler()
        assert len(factory_calls) == 2  # Wegwerf-Verbindung pro Sample

    def test_timeout_does_not_hang(self, monkeypatch):
        monkeypatch.setattr("swarmq.process_manager.sampler.SAMPLE_TIMEOUT_S", 0.2)
        admin = FakeAdmin(hang=True)
        sampler = _make_queue_depth_sampler(lambda: admin, ["a"])
        try:
            sampler()
        except TimeoutError:
            pass  # heutige Semantik: der Timeout schlägt zum Aufrufer durch


class ReadyAdmin(FakeAdmin):
    """Broker mit ``ready_depth`` (Plan 0.12.0, MR 10): nur die Haupt-Queue."""

    def __init__(self, ready, depths):
        super().__init__(depths=depths)
        self.ready = ready

    async def ready_depth(self, queue):
        return self.ready.get(queue, 0)


class TestReadyDepthForAutoscaling:
    """Plan 0.12.0, MR 10 (Abschnitt 1, F9): der Autoscaler zaehlt nur die
    Haupt-Queue. Lange Rueckstellungen in den Delay-Stufen skalieren nicht."""

    def test_sums_ready_depth_of_the_main_queues(self):
        admin = ReadyAdmin(ready={"a": 2, "b": 1}, depths={"a": 50, "b": 70})

        sampler = _make_queue_depth_sampler(lambda: admin, ["a", "b"])

        assert sampler() == 3

    def test_magicmock_broker_falls_back_to_queue_depth(self):
        # Q15: ein MagicMock hat jedes Attribut; ``ready_depth`` liefert dort
        # nichts Await-bares. Der Sampler faellt auf ``queue_depth`` zurueck.
        from unittest.mock import AsyncMock, MagicMock

        admin = MagicMock()
        admin.connect = AsyncMock()
        admin.close = AsyncMock()
        admin.queue_depth = AsyncMock(return_value=5)

        assert _make_queue_depth_sampler(lambda: admin, ["a"])() == 5

    def test_failing_ready_depth_falls_back_to_queue_depth(self):
        class Failing(FakeAdmin):
            async def ready_depth(self, queue):
                raise RuntimeError("not supported")

        admin = Failing(depths={"a": 6})

        assert _make_queue_depth_sampler(lambda: admin, ["a"])() == 6
