"""Die Logger-Isolations-Fixture muss jedes geladene ``swarmq.*``-Modul
erfassen — nicht nur ein festes Tupel (Plan U2, Risiko „still partiell":
neue Submodule der Extraktionen fielen sonst ohne Fehler heraus)."""

from __future__ import annotations

import sys
import types

import pytest
import structlog

from tests.unit._structlog_cache import reset_swarmq_logger_caches


@pytest.fixture
def synthetic_module() -> types.ModuleType:
    mod = types.ModuleType("swarmq._synthetic_cache_probe")
    mod.logger = structlog.get_logger()
    sys.modules[mod.__name__] = mod
    yield mod
    sys.modules.pop(mod.__name__, None)


def test_reset_covers_module_that_was_imported_later(synthetic_module: types.ModuleType) -> None:
    synthetic_module.logger.__dict__["bind"] = object()

    reset = reset_swarmq_logger_caches()

    assert "swarmq._synthetic_cache_probe.logger" in reset
    assert "bind" not in synthetic_module.logger.__dict__


def test_reset_ignores_modules_outside_the_package() -> None:
    mod = types.ModuleType("notswarmq_probe")
    mod.logger = structlog.get_logger()
    mod.logger.__dict__["bind"] = object()
    sys.modules[mod.__name__] = mod
    try:
        assert "notswarmq_probe.logger" not in reset_swarmq_logger_caches()
    finally:
        sys.modules.pop(mod.__name__, None)
