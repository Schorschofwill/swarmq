"""WorkerConfig-Tests (Plan U21) — verbatim."""

from __future__ import annotations

import pytest

from swarmq.process_manager import (
    WorkerConfig,
)
from tests.unit.process_manager._helpers import _make_test_cfg


class TestWorkerConfig:
    """ce-review F-1: pickle-boundary invariant + choices enforcement."""

    def test_accepts_valid_fields(self):
        cfg = _make_test_cfg()
        assert cfg.queues == ("default",)
        assert cfg.log_level == "ERROR"

    def test_rejects_list_queues(self):
        with pytest.raises(TypeError, match="pickle-safe"):
            WorkerConfig(
                module="x", queues=["a"], concurrency=1,
                log_level="INFO", log_format="human",
                burst=False, burst_idle_timeout=5.0, priorities=False,
                metrics_port=None, health_port=None,
            )

    def test_rejects_invalid_log_level(self):
        with pytest.raises(ValueError, match="log_level"):
            _make_test_cfg(log_level="TRACE")

    def test_pickle_roundtrip_preserves_fields(self):
        """Regression-guard: spawn passes WorkerConfig across a pickle
        boundary. A future non-primitive field would break this test
        before breaking production."""
        import pickle
        cfg = _make_test_cfg()
        restored = pickle.loads(pickle.dumps(cfg))
        assert restored == cfg

    def test_frozen_prevents_reassignment(self):
        import dataclasses

        cfg = _make_test_cfg()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.module = "mutated"

    def test_reload_defaults_to_false_and_empty_watch_dirs(self):
        """R5.3 Unit 2: new fields default to (False, ())."""
        cfg = _make_test_cfg()
        assert cfg.reload is False
        assert cfg.watch_dirs == ()

    def test_reload_fields_pickle_roundtrip(self):
        """R5.3 Unit 2: extended fields survive the spawn boundary."""
        import pickle
        cfg = _make_test_cfg(reload=True, watch_dirs=("/tmp/a", "/tmp/b"))
        restored = pickle.loads(pickle.dumps(cfg))
        assert restored.reload is True
        assert restored.watch_dirs == ("/tmp/a", "/tmp/b")
        assert restored == cfg

    def test_rejects_list_watch_dirs(self):
        """R5.3 Unit 2: watch_dirs must be tuple, not list."""
        with pytest.raises(TypeError, match="pickle-safe"):
            _make_test_cfg(watch_dirs=["/tmp/x"])
