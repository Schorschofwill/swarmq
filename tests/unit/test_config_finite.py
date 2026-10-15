"""Endliche Fristen in der Konfiguration (F3-04, E-063).

Die Felderliste x ``inf``/``nan`` x Argument/Env deckt der Nachweis
``test_befund_f3_04_finite_deadlines.py`` ab. Hier stehen die Grenzfälle:
welche Meldung bei abgeleiteten Feldern kommt, die Felder ohne Env-Wert,
``none`` als „unbegrenzt“, die Prüfreihenfolge zu ``> 0`` und der Weg über
``swarmq worker --check``.

Testliste:
# [x] retry_max_delay=inf nennt retry_max_delay, nicht scheduler_dedup_ttl
# [x] SWARMQ_CONSUMER_TIMEOUT_S=inf nennt consumer_timeout_s, nicht execution_guard_done_ttl_s
# [x] shutdown_timeout=inf -> Fehler; shutdown_timeout=0 bleibt erlaubt
# [x] bind_retry_budget_s=inf/nan -> Fehler
# [x] dead_letter_ttl_s=None und SWARMQ_DEAD_LETTER_TTL_S=none bleiben gültig
# [x] -inf in einem Feld mit "> 0" meldet weiter "must be > 0"
# [x] swarmq worker --check mit SWARMQ_CONSUMER_TIMEOUT_S=inf -> ok: false
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from swarmq.cli import main
from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError

_MODULE = "config_finite_check_tasks"

_TASK_SOURCE = """\
from swarmq import Task, TaskInfo


class ConfigFiniteCheckTask(Task):
    task_info = TaskInfo(name="config_finite_check_task", queues=["default"])

    async def run(self) -> None:
        return None
"""


def _cfg(**kwargs: Any) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)


@pytest.mark.parametrize("value", [float("inf"), float("nan")])
def test_infinite_retry_max_delay_names_the_cause(value: float) -> None:
    with pytest.raises(ConfigurationError, match=r"^retry_max_delay must be a finite number"):
        _cfg(retry_max_delay=value)


def test_infinite_consumer_timeout_from_env_names_the_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_CONSUMER_TIMEOUT_S", "inf")
    with pytest.raises(ConfigurationError, match=r"^consumer_timeout_s must be a finite number"):
        _cfg()


@pytest.mark.parametrize("value", [float("inf"), float("nan")])
def test_infinite_shutdown_timeout_is_rejected(value: float) -> None:
    with pytest.raises(ConfigurationError, match=r"^shutdown_timeout must be a finite number"):
        _cfg(shutdown_timeout=value)


def test_zero_shutdown_timeout_stays_allowed() -> None:
    assert _cfg(shutdown_timeout=0).shutdown_timeout == 0


@pytest.mark.parametrize("value", [float("inf"), float("nan")])
def test_infinite_bind_retry_budget_is_rejected(value: float) -> None:
    with pytest.raises(ConfigurationError, match=r"^bind_retry_budget_s must be a finite number"):
        _cfg(bind_retry_budget_s=value)


def test_unlimited_dead_letter_ttl_stays_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _cfg(dead_letter_ttl_s=None).dead_letter_ttl_s is None
    monkeypatch.setenv("SWARMQ_DEAD_LETTER_TTL_S", "none")
    assert _cfg().dead_letter_ttl_s is None


def test_negative_infinity_keeps_the_positive_message() -> None:
    with pytest.raises(ConfigurationError, match=r"^consumer_timeout_s must be > 0, got -inf"):
        _cfg(consumer_timeout_s=float("-inf"))


@pytest.fixture
def task_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Gültiges Task-Modul auf ``sys.path``; räumt ``sys.modules`` auf."""
    (tmp_path / f"{_MODULE}.py").write_text(_TASK_SOURCE, encoding="utf-8")
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    yield _MODULE
    sys.modules.pop(_MODULE, None)


def test_worker_check_rejects_infinite_consumer_timeout(
    task_module: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SWARMQ_CONSUMER_TIMEOUT_S", "inf")

    code = main(["worker", f"--module={task_module}", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert (payload["ok"], code, payload["error_type"]) == (False, 1, "ConfigurationError")
    assert "consumer_timeout_s" in payload["error"]
