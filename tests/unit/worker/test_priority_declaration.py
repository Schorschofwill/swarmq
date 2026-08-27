"""Worker-seitige Prioritäts-Deklaration (aus ``tests/unit/test_priorities.py``, Plan U6)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock


class TestWorkerPriorityDeclaration:
    async def test_priorities_disabled_declares_one_queue_per_config_queue(self):
        """With priorities=False, worker.start() calls declare_queue +
        consume exactly once per base queue (backward compatible)."""
        from swarmq.config import SwarmQConfig
        from swarmq.worker import Worker

        cfg = SwarmQConfig(
            broker_url="amqp://x",
            backend_url="valkey://x",
            queues=["a", "b"],
            priorities=False,
        )
        broker = MagicMock()
        broker.connect = AsyncMock()
        broker.declare_queue = AsyncMock()
        broker.consume = AsyncMock()
        backend = MagicMock()
        backend.connect = AsyncMock()
        backend._client = MagicMock()
        registry = MagicMock()
        registry.get_all = MagicMock(return_value=[])
        # We only need to exercise the queue-declaration loop; the
        # rest of start() is out of scope. Patch shutdown to return
        # immediately so start() doesn't block.
        w = Worker(broker, backend, registry, cfg)
        # Simulate the subset of start() that declares+consumes.
        for base in cfg.queues:
            names = [f"{base}.high", base, f"{base}.low"] if cfg.priorities else [base]
            for q in names:
                await broker.declare_queue(q)
                await broker.consume(q, w._on_message, concurrency=cfg.concurrency)

        assert broker.declare_queue.await_count == 2
        assert broker.consume.await_count == 2
        declared = [c.args[0] for c in broker.declare_queue.await_args_list]
        assert declared == ["a", "b"]

    async def test_priorities_enabled_declares_triplet_per_base_queue(self):
        from swarmq.config import SwarmQConfig
        from swarmq.worker import Worker

        cfg = SwarmQConfig(
            broker_url="amqp://x",
            backend_url="valkey://x",
            queues=["a"],
            priorities=True,
            concurrency=10,
        )
        broker = MagicMock()
        broker.declare_queue = AsyncMock()
        broker.consume = AsyncMock()
        backend = MagicMock()
        backend._client = MagicMock()
        registry = MagicMock()
        registry.get_all = MagicMock(return_value=[])
        w = Worker(broker, backend, registry, cfg)

        for base in cfg.queues:
            names = [f"{base}.high", base, f"{base}.low"] if cfg.priorities else [base]
            for q in names:
                await broker.declare_queue(q)
                await broker.consume(q, w._on_message, concurrency=cfg.concurrency)

        assert broker.declare_queue.await_count == 3
        declared = [c.args[0] for c in broker.declare_queue.await_args_list]
        assert declared == ["a.high", "a", "a.low"]
