"""Nachweis F3-17: ``swarmq worker --check`` meldet ``ok: true`` für Env-Werte,
an denen der Start mit Exit 78 scheitert.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-17, Verdacht R3D-05).

``_run_worker_check`` löst die Worker-Optionen auf, baut aber kein
``SwarmQConfig`` und durchläuft nicht den Concurrency-Pfad des Starts. Der
Start lehnt ``SWARMQ_SCHEDULER=maybe``, ``SWARMQ_CONCURRENCY=0``,
``SWARMQ_SHUTDOWN_TIMEOUT=abc`` und ``SWARMQ_VALKEY_EVICTION_CHECK=bogus``
mit ``ConfigurationError`` (Exit 78) ab; ``--check`` meldet ``ok: true``
und Exit 0. Zusage: ``cli.md:11-12`` („validates exactly the values the
workers would get“), Plan 2026-08-28-001:306.

Entscheidung Georg 2026-10-15 (Nr. 8): ``--check`` prüft alle Env-Werte, die
der Start mit 78 ablehnt, aber NICHT fehlende Broker-/Backend-URLs. Die
Tests setzen deshalb keine URLs und erwarten trotzdem nur am fehlerhaften
Wert ein ``ok: false`` (Exit 1, ``error_type`` ``ConfigurationError``,
wie bei den übrigen Prüfungen von ``--check``).

Das Task-Modul liegt mit einem registrierten Task in ``tmp_path``, damit
ein späteres „keine Tasks registriert“ im Check die Tests nicht aus dem
falschen Grund grün macht.

Testliste:
# [x] SWARMQ_SCHEDULER=maybe → ok: false, rc 1
# [x] SWARMQ_CONCURRENCY=0 → ok: false, rc 1
# [x] SWARMQ_SHUTDOWN_TIMEOUT=abc → ok: false, rc 1
# [x] SWARMQ_VALKEY_EVICTION_CHECK=bogus → ok: false, rc 1
# [x] gültige Umgebung ohne Broker-/Backend-URL → ok: true, rc 0 (E-062)
# [x] SWARMQ_QUEUE_TYPE=foo → ok: false (jeder Wert, den SwarmQConfig ablehnt)
# [x] Gleichlauf: jeder abgelehnte Wert lässt auch den Start mit 78 enden
# [x] --log-format human überstimmt SWARMQ_LOG_FORMAT=xml → ok: true
# [x] Gleichlauf im Flag-Fall (N-1): --concurrency 4 / --no-scheduler neben
#     ungültigem Env-Wert → --check und Start entscheiden gleich

Behoben in 0.12.3: ``--check`` baut die Konfiguration über denselben Helfer
wie der Pre-Flight des Starts (``process_manager.check.start_config``).
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from swarmq.cli import main

_MODULE = "f3_17_check_tasks"

_TASK_SOURCE = """\
from swarmq import Task, TaskInfo


class F317CheckTask(Task):
    task_info = TaskInfo(name="f3_17_check_task", queues=["default"])

    async def run(self) -> None:
        return None
"""

#: Env-Variablen, die die Tests setzen oder deren Fremdwert sie verfälschen könnte.
_CHECKED_ENV = (
    "SWARMQ_SCHEDULER",
    "SWARMQ_CONCURRENCY",
    "SWARMQ_SHUTDOWN_TIMEOUT",
    "SWARMQ_VALKEY_EVICTION_CHECK",
    "SWARMQ_QUEUE_TYPE",
    "SWARMQ_LOG_FORMAT",
    "SWARMQ_BROKER_URL",
    "SWARMQ_BACKEND_URL",
)

#: Env-Werte, die der Start mit ``ConfigurationError`` (Exit 78) ablehnt.
_REJECTED_ENV = [
    ("SWARMQ_SCHEDULER", "maybe"),
    ("SWARMQ_CONCURRENCY", "0"),
    ("SWARMQ_SHUTDOWN_TIMEOUT", "abc"),
    ("SWARMQ_VALKEY_EVICTION_CHECK", "bogus"),
]


@pytest.fixture
def task_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Gültiges Task-Modul auf ``sys.path``, saubere Umgebung; räumt ``sys.modules`` auf."""
    (tmp_path / f"{_MODULE}.py").write_text(_TASK_SOURCE, encoding="utf-8")
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    for name in _CHECKED_ENV:
        monkeypatch.delenv(name, raising=False)
    yield _MODULE
    sys.modules.pop(_MODULE, None)


@pytest.mark.befund("F3-17")
@pytest.mark.entscheidung("E-062")
@pytest.mark.parametrize(("variable", "value"), _REJECTED_ENV)
def test_worker_check_rejects_env_values_the_start_rejects(
    task_module: str,
    variable: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(variable, value)

    code = main(["worker", f"--module={task_module}", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False, f"{variable}={value}: der Start endet mit 78, --check meldet ok: true"
    assert code == 1
    assert payload["error_type"] == "ConfigurationError"


@pytest.mark.entscheidung("E-062")
def test_worker_check_does_not_require_broker_and_backend_urls(
    task_module: str, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["worker", f"--module={task_module}", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True, payload["error"]
    assert code == 0


def test_worker_check_rejects_an_invalid_queue_type(
    task_module: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SWARMQ_QUEUE_TYPE", "foo")

    code = main(["worker", f"--module={task_module}", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert code == 1
    assert "queue_type" in payload["error"]


def test_worker_check_lets_the_log_format_flag_override_an_invalid_env_value(
    task_module: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SWARMQ_LOG_FORMAT", "xml")

    code = main(["worker", f"--module={task_module}", "--log-format", "human", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True, payload["error"]
    assert code == 0


class _UnreachedProcessManager:
    """Ersetzt den Supervisor: der Pre-Flight soll vorher abbrechen."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.reloader: object = None

    def run(self) -> int:
        return 0


@pytest.mark.parametrize(("variable", "value"), [*_REJECTED_ENV, ("SWARMQ_QUEUE_TYPE", "foo")])
def test_the_start_rejects_every_value_the_check_rejects(
    task_module: str,
    variable: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from swarmq.process_manager import run as run_module
    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    monkeypatch.setattr(run_module, "ProcessManager", _UnreachedProcessManager)
    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://guest:guest@f3-17.invalid:5699/")
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://f3-17.invalid:6399/0")
    monkeypatch.setenv(variable, value)

    code = main(["worker", f"--module={task_module}"])

    capsys.readouterr()
    assert code == CONFIG_ERROR_EXIT_CODE


@pytest.mark.parametrize(
    ("flags", "variable", "value"),
    [
        (["--concurrency", "4"], "SWARMQ_CONCURRENCY", "0"),
        (["--no-scheduler"], "SWARMQ_SCHEDULER", "maybe"),
    ],
    ids=["concurrency", "scheduler"],
)
def test_check_and_start_decide_alike_when_a_flag_overrides_an_invalid_env_value(
    task_module: str,
    flags: list[str],
    variable: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Gleichlauf im Flag-Fall (Review U9, offener Punkt N-1, KTD15).

    Ein Flag sollte den ungültigen Env-Wert überstimmen (CLI > Env); heute
    bauen Pre-Flight und ``--check`` die Konfiguration aber ohne die
    aufgelösten Worker-Optionen, also lehnen beide ab. N-1 bleibt offen;
    der Test verhindert nur, dass ``--check`` und Start auseinanderlaufen,
    wenn N-1 an nur einer Stelle behoben wird.
    """
    from swarmq.process_manager import run as run_module
    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    monkeypatch.setattr(run_module, "ProcessManager", _UnreachedProcessManager)
    monkeypatch.setenv(variable, value)

    main(["worker", f"--module={task_module}", *flags, "--check"])
    check_ok = json.loads(capsys.readouterr().out)["ok"]
    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://guest:guest@f3-17.invalid:5699/")
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://f3-17.invalid:6399/0")
    start_code = main(["worker", f"--module={task_module}", *flags])
    capsys.readouterr()

    start_ok = start_code != CONFIG_ERROR_EXIT_CODE
    assert check_ok == start_ok, f"{flags} mit {variable}={value}: --check ok={check_ok}, Start {start_code}"
