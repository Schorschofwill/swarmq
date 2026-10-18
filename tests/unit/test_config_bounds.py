"""Konfigurationswerte innerhalb der technischen Grenze ihres Ziels (E-063,
Nachtrag 2026-10-17; PF-10, PF-11, F3-38).

Jeder Wert über der Grenze ist ein ``ConfigurationError`` mit Feldnamen
(Exit 78, E-057), als Argument wie als Env-Wert. Grenzen: Fristen bis
``MAX_DEADLINE_S`` (gemessen gegen Valkey ``PX`` und RabbitMQ-``expiration``,
siehe ``tests/integration/backend/test_deadline_bounds_valkey.py`` und
``tests/integration/broker/test_expiration_bounds_rabbitmq.py``), Zählfelder
bis ``sys.float_info.max`` (``delivery_limit`` als ``x-delivery-limit`` an
RabbitMQ bis 2**63-1), ``concurrency`` bis 65535 (AMQP short in
``basic.qos``).

Testliste:
# [x] SWARMQ_SHUTDOWN_TIMEOUT mit 310 Stellen -> ConfigurationError mit shutdown_timeout
# [x] retry_max_delay=10**400 -> ConfigurationError mit retry_max_delay (vor der Ableitung)
# [x] lock_wait_max_s=1e306 -> ConfigurationError mit lock_wait_max_s
# [x] concurrency=65536 -> ConfigurationError; 65535 gültig
# [x] Frist genau an der Grenze gültig, knapp darüber abgelehnt (Argument und Env)
# [x] Zählfeld genau float_info.max gültig, darüber abgelehnt
# [x] scheduler_publish_stepdown_s (nicht unter _FINITE_FIELDS) über der Grenze -> Fehler
# [x] result_ttl über der Grenze -> Fehler (Valkey EX)
# [x] swarmq worker --check und swarmq check melden den Wert vor dem Start
# [x] delivery_limit (x-delivery-limit an RabbitMQ) bis 2**63-1, 2**63 abgelehnt (Argument und Env)
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
from swarmq.config_validation import MAX_DEADLINE_MS, MAX_DEADLINE_S
from swarmq.exceptions import ConfigurationError

pytestmark = pytest.mark.entscheidung("E-063")

_TOO_MANY_DIGITS = "1" + "0" * 309


def _cfg(**kwargs: Any) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)


def test_shutdown_timeout_with_310_digits_names_the_field(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_SHUTDOWN_TIMEOUT", _TOO_MANY_DIGITS)

    with pytest.raises(ConfigurationError, match=r"^shutdown_timeout must be at most"):
        _cfg()


def test_huge_retry_max_delay_is_rejected_before_the_derived_fields() -> None:
    # Ohne Vorprüfung warf schon ``retry_max_delay + 300.0`` (Default von
    # ``scheduler_dedup_ttl``) einen OverflowError.
    with pytest.raises(ConfigurationError, match=r"^retry_max_delay must be at most"):
        _cfg(retry_max_delay=10**400)


def test_concurrency_above_the_amqp_short_is_rejected() -> None:
    # F3-38: ``basic.qos`` trägt prefetch_count als short; pamqp warf beim Start.
    with pytest.raises(ConfigurationError, match=r"^concurrency must be at most 65535, got 65536"):
        _cfg(concurrency=65536)


def test_concurrency_at_the_amqp_short_is_valid() -> None:
    assert _cfg(concurrency=65535).concurrency == 65535


_FLOAT_MAX_INT = int(sys.float_info.max)


def test_count_field_up_to_float_max_is_valid() -> None:
    # Zählfelder sind keine Fristen; ihre Grenze ist die von ``math.isfinite``.
    config = _cfg(log_queue_size=_FLOAT_MAX_INT)

    assert config.log_queue_size == _FLOAT_MAX_INT


def test_count_field_above_float_max_names_the_field(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_DELIVERY_LIMIT", _TOO_MANY_DIGITS)

    with pytest.raises(ConfigurationError, match=r"^delivery_limit must be at most"):
        _cfg()


def test_delivery_limit_above_the_amqp_long_long_names_the_field() -> None:
    # ``delivery_limit`` geht als ``x-delivery-limit`` an RabbitMQ: 2**63-1
    # nimmt der Broker an, 2**63 ließ schon pamqp mit TypeError scheitern
    # (Exit 1 statt 78).
    with pytest.raises(ConfigurationError, match=rf"^delivery_limit must be at most {2**63 - 1}, got {2**63}$"):
        _cfg(delivery_limit=2**63)


@pytest.mark.parametrize(("raw", "valid"), [(str(2**63 - 1), True), (str(2**63), False)])
def test_delivery_limit_bound_from_env(raw: str, valid: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_DELIVERY_LIMIT", raw)

    if valid:
        assert _cfg().delivery_limit == 2**63 - 1
    else:
        with pytest.raises(ConfigurationError, match=r"^delivery_limit must be at most 9223372036854775807"):
            _cfg()


def test_lock_wait_max_s_far_above_the_bound_names_the_field() -> None:
    # PF-11: ``1e306 * 1000`` ist ``inf``; angenommen warf das Lock-Gate bei
    # jedem Konflikt einen OverflowError.
    with pytest.raises(ConfigurationError, match=r"^lock_wait_max_s must be at most 315360000 seconds, got 1e\+306"):
        _cfg(lock_wait_max_s=1e306)


def test_the_bound_is_ten_years_in_whole_milliseconds() -> None:
    assert (MAX_DEADLINE_S, MAX_DEADLINE_MS) == (315_360_000, 315_360_000_000)


# Fristen, die als Per-Message-Expiration (RabbitMQ) oder als ``PX``/``EX``
# (Valkey) enden, dazu ``shutdown_timeout`` als Ganzzahl.
_DEADLINES = pytest.mark.parametrize(
    "field_name",
    ["retry_max_delay", "lock_wait_max_s", "dead_letter_ttl_s", "execution_guard_ttl_s", "shutdown_timeout"],
)


@_DEADLINES
def test_deadline_exactly_at_the_bound_is_valid(field_name: str) -> None:
    config = _cfg(**{field_name: MAX_DEADLINE_S})

    assert getattr(config, field_name) == MAX_DEADLINE_S


@_DEADLINES
def test_deadline_just_above_the_bound_names_the_field(field_name: str) -> None:
    with pytest.raises(ConfigurationError, match=rf"^{field_name} must be at most {MAX_DEADLINE_S} seconds"):
        _cfg(**{field_name: MAX_DEADLINE_S + 1})


@pytest.mark.parametrize(
    ("raw", "valid"), [(str(MAX_DEADLINE_S), True), (f"{MAX_DEADLINE_S}.001", False)], ids=["at", "above"]
)
def test_deadline_bound_from_env(raw: str, valid: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_RETRY_MAX_DELAY", raw)

    if valid:
        assert _cfg().retry_max_delay == MAX_DEADLINE_S
    else:
        with pytest.raises(ConfigurationError, match=r"^retry_max_delay must be at most"):
            _cfg()


@pytest.mark.parametrize(("value", "valid"), [(MAX_DEADLINE_S, True), (1e306, False)], ids=["at", "above"])
def test_publish_stepdown_bound(value: float, valid: bool) -> None:
    # Eigene Prüfung in ``_validate_scheduler_relations`` (0 schaltet ab).
    if valid:
        assert _cfg(scheduler_publish_stepdown_s=value).scheduler_publish_stepdown_s == value
    else:
        with pytest.raises(
            ConfigurationError,
            match=r"^scheduler_publish_stepdown_s must be >= 0, finite and at most 315360000 seconds",
        ):
            _cfg(scheduler_publish_stepdown_s=value)


def test_publish_stepdown_as_huge_int_names_the_field() -> None:
    # ``math.isfinite`` auf einem int über float_info.max wirft OverflowError.
    with pytest.raises(
        ConfigurationError, match=r"^scheduler_publish_stepdown_s must be >= 0, finite and at most 315360000 seconds"
    ):
        _cfg(scheduler_publish_stepdown_s=10**400)


@pytest.mark.parametrize(
    ("value", "valid"), [(MAX_DEADLINE_S, True), (MAX_DEADLINE_S + 0.5, False)], ids=["at", "above"]
)
def test_result_ttl_bound(value: float, valid: bool) -> None:
    # ``result_ttl`` endet als ``EX`` in Valkey; über der Grenze auch nach dem Aufrunden.
    if valid:
        assert _cfg(result_ttl=value).result_ttl == MAX_DEADLINE_S
    else:
        with pytest.raises(ConfigurationError, match=r"^result_ttl must be at most"):
            _cfg(result_ttl=value)


_MODULE = "config_bounds_check_tasks"

_TASK_SOURCE = """\
from swarmq import Task, TaskInfo


class ConfigBoundsCheckTask(Task):
    task_info = TaskInfo(name="config_bounds_check_task", queues=["default"])

    async def run(self) -> None:
        return None
"""


@pytest.fixture
def task_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Gültiges Task-Modul auf ``sys.path``; räumt ``sys.modules`` auf."""
    (tmp_path / f"{_MODULE}.py").write_text(_TASK_SOURCE, encoding="utf-8")
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    yield _MODULE
    sys.modules.pop(_MODULE, None)


def test_worker_check_reports_a_value_above_the_bound(
    task_module: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SWARMQ_LOCK_WAIT_MAX_S", "1e306")

    code = main(["worker", f"--module={task_module}", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert (payload["ok"], code, payload["error_type"]) == (False, 1, "ConfigurationError")
    assert "lock_wait_max_s" in payload["error"]


def test_env_check_ends_with_78_before_connecting(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Port 1 ist nie erreichbar: Exit 78 kommt vor jeder Verbindung (F3-18).
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://127.0.0.1:1/0")
    monkeypatch.delenv("SWARMQ_BROKER_URL", raising=False)
    monkeypatch.setenv("SWARMQ_SHUTDOWN_TIMEOUT", _TOO_MANY_DIGITS)

    code = main(["check", "--format", "json"])

    assert code == 78
    assert "shutdown_timeout must be at most" in capsys.readouterr().err
