"""E2E: dieselbe "App gewinnt"-Regel im IO- und im CPU-Pool (Plan
2026-10-06-1811, U9: R12, R13, KTD9; Analyse S7 und R1).

Ein echter ``swarmq worker`` mit RabbitMQ und Valkey laedt ein App-Modul, das
beim Import selbst Logging einrichtet (``_logging_pools_app``). Im CPU-Pool
importiert der Forkserver das Modul; der Sentinel an Root macht ``basicConfig``
dort so wirkungslos wie im IO-Worker. Vorlage: ``drive_bc.py`` der Analyse.

Test-Liste (jeweils fuer ``io`` und ``cpu``):
- [x] ``basicConfig`` und ``--log-format json``: Jede Task-Logzeile ist JSON,
      die INFO-Zeile ist da, das App-Format nie.
- [x] ``basicConfig`` und ``--log-config``: Das Format der Datei gilt fuer die
      Task-Zeilen.
- [x] ``basicConfig`` ohne Level: Die INFO-Zeile bleibt sichtbar.
- [x] ``basicConfig`` und ``--no-configure-logging``: Das App-Format gilt.
Nur ``cpu``:
- [x] App mit eigenem Queue-Listener (Cookbook-Muster, Start im
      ``--configure``-Hook): Die Zeilen der Compute-Kinder kommen in der Datei
      der App an (R13).
- [x] Startet die App den Listener schon beim Import, bricht der CPU-Worker
      mit Exit 78 ab, und die Thread-Pruefung nennt den App-``QueueListener``
      (U6, KTD7).
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.e2e.test_logging_e2e_runtime import SWARMQ_SCRIPT, _LineCollector, _worker_env
from tests.support.cli_process import terminate_worker_process
from tests.support.fixture_modules import ensure_fixture_registered

APP_MODULE = "tests.e2e._logging_pools_app"

# Wie die uebrigen e2e-Tests mit echtem Worker: Start samt Forkserver, ein
# Roundtrip ueber RabbitMQ/Valkey und der geordnete Teardown.
pytestmark = [pytest.mark.e2e, pytest.mark.timeout(120)]

POOLS = ["io", "cpu"]
CONFIG_EXIT_CODE = 78  # EX_CONFIG: ungueltige Konfiguration, kein Neustart


def _run_worker(
    pool: str,
    mode: str,
    *extra: str,
    tmp_path: Path,
    env_extra: dict[str, str] | None = None,
    task_lines_on_stdout: bool = True,
) -> list[str]:
    """Worker starten, einen Task abarbeiten lassen, beenden; liefert alle
    Ausgabezeilen (stdout und stderr). Im Rechenkind steht die Logzeile vor
    dem Ergebnis im Strom; im IO-Worker wird auf sie gewartet."""
    queue = f"logpools-{pool}-{mode}-{uuid.uuid4().hex[:8]}"
    env = _worker_env(queue)
    env.pop("SWARMQ_FORKSERVER_LOG_SENTINEL", None)
    env["POOLS_APP_MODE"] = mode
    env.update(env_extra or {})
    proc = subprocess.Popen(
        [
            str(SWARMQ_SCRIPT),
            "worker",
            "--module",
            APP_MODULE,
            "--queues",
            queue,
            "--processes",
            "1",
            "--concurrency",
            "1",
            "--pool",
            pool,
            *extra,
        ],
        cwd=str(tmp_path),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    out = _LineCollector(proc)
    try:
        out.wait_for(lambda line: "worker_started" in line, timeout=60)
        assert asyncio.run(_roundtrip(queue, "v1")) == "ok:v1"
        if task_lines_on_stdout:
            out.wait_for(lambda line: "pools_warn_v1" in line, timeout=10)
    finally:
        terminate_worker_process(proc, grace_s=20)
        out.join()
    return out.snapshot()


async def _roundtrip(queue: str, value: str) -> str:
    from tests.support.services import rabbitmq_amqp_url, valkey_db_url

    ensure_fixture_registered(APP_MODULE, "pools_task")
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        task_id = await app.schedule("pools_task", value, queue=queue)
        return await app.get_result(task_id, timeout=30)


def _task_lines(lines: list[str]) -> list[str]:
    return [line for line in lines if "pools_info_" in line or "pools_warn_" in line]


@pytest.fixture(autouse=True)
def _queue_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    # Der Testprozess importiert das App-Modul zum Einreihen -- ohne Modus,
    # also ohne Eingriff in sein eigenes Logging.
    monkeypatch.delenv("POOLS_APP_MODE", raising=False)
    yield


@pytest.mark.parametrize("pool", POOLS)
def test_basicconfig_has_no_effect_and_json_applies_to_every_task_line(pool: str, tmp_path: Path) -> None:
    lines = _task_lines(_run_worker(pool, "basic", "--log-format", "json", tmp_path=tmp_path))

    events = [json.loads(line)["event"] for line in lines]  # jede Zeile ist JSON
    assert "pools_info_v1" in events and "pools_warn_v1" in events, lines


@pytest.mark.parametrize("pool", POOLS)
def test_the_log_config_file_applies_to_the_task_lines(pool: str, tmp_path: Path) -> None:
    cfg = tmp_path / "logcfg.json"
    cfg.write_text(
        json.dumps(
            {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {"f": {"format": "LOGCFG|%(name)s|%(message)s"}},
                "handlers": {"h": {"class": "logging.StreamHandler", "formatter": "f"}},
                "root": {"handlers": ["h"], "level": "INFO"},
            }
        )
    )

    lines = _task_lines(_run_worker(pool, "basic", "--log-config", str(cfg), tmp_path=tmp_path))

    assert sorted(lines) == ["LOGCFG|poolsapp|pools_info_v1", "LOGCFG|poolsapp|pools_warn_v1"], lines


@pytest.mark.parametrize("pool", POOLS)
def test_info_stays_visible_when_basicconfig_sets_no_level(pool: str, tmp_path: Path) -> None:
    lines = _task_lines(_run_worker(pool, "basic_nolevel", "--log-format", "json", tmp_path=tmp_path))

    assert "pools_info_v1" in [json.loads(line)["event"] for line in lines], lines


@pytest.mark.parametrize("pool", POOLS)
def test_without_swarmq_setup_the_basicconfig_format_applies(pool: str, tmp_path: Path) -> None:
    lines = _task_lines(_run_worker(pool, "basic", "--no-configure-logging", tmp_path=tmp_path))

    assert sorted(lines) == ["APPFMT|poolsapp|INFO|pools_info_v1", "APPFMT|poolsapp|WARNING|pools_warn_v1"], lines


def test_an_app_queue_listener_started_in_the_configure_hook_gets_the_compute_child_lines(tmp_path: Path) -> None:
    app_log = tmp_path / "app.log"

    _run_worker(
        "cpu",
        "cookbook",
        "--configure",
        f"{APP_MODULE}:start_listener",
        tmp_path=tmp_path,
        env_extra={"POOLS_APP_LOG": str(app_log)},
        task_lines_on_stdout=False,
    )

    task_lines = [line.split("|", 2)[2] for line in app_log.read_text().splitlines() if "pools_" in line]
    assert sorted(task_lines) == ["poolsapp|pools_info_v1", "poolsapp|pools_warn_v1"]


def test_an_app_queue_listener_started_at_import_is_named_by_the_thread_check(tmp_path: Path) -> None:
    queue = f"logpools-cpu-start-{uuid.uuid4().hex[:8]}"
    env = _worker_env(queue)
    env.update(POOLS_APP_MODE="cookbook_start", POOLS_APP_LOG=str(tmp_path / "app.log"))
    proc = subprocess.Popen(
        [str(SWARMQ_SCRIPT), "worker", "--module", APP_MODULE, "--queues", queue, "--processes", "1", "--pool", "cpu"],
        cwd=str(tmp_path),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    out = _LineCollector(proc)
    try:
        assert proc.wait(timeout=60) == CONFIG_EXIT_CODE
    finally:
        terminate_worker_process(proc, grace_s=20)
        out.join()
    assert any("QueueListener" in line for line in out.snapshot()), out.snapshot()[-20:]
