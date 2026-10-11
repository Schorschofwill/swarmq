"""Import-Fallback von ``swarmq.metrics`` ohne ``prometheus_client``.

Das Test-Extra installiert ``prometheus_client``; deshalb laeuft der
``except ImportError``-Zweig sonst nie. Die Fixture simuliert das fehlende
Paket ueber ``sys.modules`` und importiert das Modul frisch. ``monkeypatch``
stellt danach das Original-Modul in ``sys.modules`` und am Paket wieder her,
so dass andere Tests unbeeinflusst bleiben.

Test-Liste:
- Fallback-Flag ist aus, ``Metrics.enabled`` ist False, keine Registry
- ``render()`` liefert den Platzhalter
- ``CONTENT_TYPE`` faellt auf den Text-Exposition-Standard zurueck
- Metrik-Attribute sind No-ops, auch verkettet ueber ``labels()``
- Die Hooks ``count_*`` laufen ohne Fehler
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Any

import pytest

import swarmq


@pytest.fixture
def metrics_without_prometheus(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Frisch importiertes ``swarmq.metrics`` bei fehlendem ``prometheus_client``."""
    # None in sys.modules laesst ``import`` mit ModuleNotFoundError scheitern.
    monkeypatch.setitem(sys.modules, "prometheus_client", None)
    monkeypatch.setitem(sys.modules, "prometheus_client.core", None)
    # Original merken: monkeypatch setzt beides beim Teardown zurueck.
    monkeypatch.delitem(sys.modules, "swarmq.metrics", raising=False)
    monkeypatch.setattr(swarmq, "metrics", getattr(swarmq, "metrics", None), raising=False)
    return importlib.import_module("swarmq.metrics")


class TestImportFallback:
    def test_flags_and_registry_signal_disabled_metrics(self, metrics_without_prometheus: ModuleType) -> None:
        mod = metrics_without_prometheus
        m = mod.Metrics()

        assert mod._HAVE_PROMETHEUS is False
        assert m.enabled is False
        assert m.registry is None

    def test_render_returns_placeholder(self, metrics_without_prometheus: ModuleType) -> None:
        body = metrics_without_prometheus.Metrics().render()

        assert body == b"# swarmq metrics disabled - install prometheus_client\n"

    def test_content_type_falls_back_to_text_exposition_default(self, metrics_without_prometheus: ModuleType) -> None:
        assert metrics_without_prometheus.CONTENT_TYPE == "text/plain; version=0.0.4; charset=utf-8"

    def test_metric_operations_are_chainable_noops(self, metrics_without_prometheus: ModuleType) -> None:
        m: Any = metrics_without_prometheus.Metrics()

        m.tasks_total.labels(task_name="t", queue="q", outcome="completed").inc()
        m.tasks_active.labels(queue="q").inc()
        m.tasks_active.labels(queue="q").dec()
        m.queue_depth.labels(queue="q", kind="pending").set(3)
        m.task_duration_seconds.labels(task_name="t", queue="q").observe(0.5)
        m.dlq_messages.remove("q")
        m.count_consumer_reregistration("q", "cancelled")
        m.count_execution_guard_event("blocked")

        assert m.tasks_total.labels(queue="q") is m.tasks_total
        assert m.render() == b"# swarmq metrics disabled - install prometheus_client\n"

    def test_original_module_is_restored_after_fixture(self) -> None:
        # Gegenprobe ohne Fixture: Das echte Modul mit prometheus_client ist aktiv.
        from swarmq.metrics import _HAVE_PROMETHEUS

        assert _HAVE_PROMETHEUS is True
