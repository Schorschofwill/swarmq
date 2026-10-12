"""Eingebettete Beispiel-App fuer ``test_logging_e2e_runtime`` (Plan
2026-10-06-0831, R5/R6, KTD10).

Laeuft als eigener Prozess, damit das Logging des Testprozesses unberuehrt
bleibt::

    python -m tests.e2e._logging_runtime_app {queue|default} QUEUE BROKER BACKEND

Die App richtet stdlib-Logging selbst ein (``basicConfig`` auf einen
``StringIO`` mit Handler-Thread, ``%(threadName)s`` und ``%(task_id)s``), plant drei Tasks,
arbeitet sie mit einem eingebetteten Burst-Worker ab, loggt zum Schluss 200
Probe-Zeilen ueber einen ``swarmq.*``-Logger und verlaesst dann ``async with``.
Auf stdout steht genau eine JSON-Zusammenfassung: Root-/``swarmq``-Zustand vor
und nach ``SwarmQ()``, nach dem Verlassen, die Ergebnisse und alle Logzeilen.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import sys
import threading
from typing import Any, override

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task, TaskInfo

LINE_FORMAT = "%(handler_thread)s|%(threadName)s|%(name)s|%(message)s|%(task_id)s"
PROBE_COUNT = 200


class _AnnotateRecord(logging.Filter):
    """Haengt den Thread an, in dem der Handler der App laeuft.

    ``%(threadName)s`` ist der Thread, der den Record *erzeugt* hat (stdlib
    setzt ihn im ``LogRecord``-Konstruktor) -- es bleibt im Opt-in der
    Aufrufer. Wo der Handler arbeitet, zeigt nur ``current_thread()`` beim
    Filtern, und Handler-Filter laufen im selben Thread wie ``emit``.

    Records ohne Task-Kontext bekommen ``task_id='-'`` -- sonst scheitert
    ``%(task_id)s`` an jeder Zeile ausserhalb eines Tasks."""

    @override
    def filter(self, record: logging.LogRecord) -> bool:
        record.handler_thread = threading.current_thread().name
        if not hasattr(record, "task_id"):
            record.task_id = "-"
        return True


def _snapshot() -> dict[str, Any]:
    root = logging.getLogger()
    swarmq_logger = logging.getLogger("swarmq")
    return {
        "root_handlers": [id(h) for h in root.handlers],
        "root_handler_types": [type(h).__name__ for h in root.handlers],
        "root_level": root.level,
        "swarmq_handlers": [type(h).__name__ for h in swarmq_logger.handlers],
        "swarmq_propagate": swarmq_logger.propagate,
        "swarmq_level": swarmq_logger.level,
        "threads": sorted(t.name for t in threading.enumerate()),
    }


async def _run(mode: str, queue: str, broker_url: str, backend_url: str) -> dict[str, Any]:
    task_name = f"logrt_add_{queue}"

    class _AddTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self, a: int, b: int) -> int:
            return a + b

    opt_in: dict[str, Any] = {"log_queue": True, "log_level": "INFO"} if mode == "queue" else {}
    before = _snapshot()
    client = SwarmQ(RabbitMQBroker(broker_url), ValkeyBackend(backend_url), queues=[queue], **opt_in)
    after_init = _snapshot()
    async with client:
        await client.broker.declare_queue(queue)
        task_ids = [await client.schedule(task_name, i, 1, queue=queue) for i in range(3)]
        worker = SwarmQ(
            RabbitMQBroker(broker_url),
            ValkeyBackend(backend_url),
            queues=[queue],
            burst_mode=True,
            burst_idle_timeout=0.5,
            **opt_in,
        )
        async with worker:
            await asyncio.wait_for(worker.start_worker(), timeout=30.0)
        results = [await client.get_result(tid, timeout=10.0) for tid in task_ids]
        probe = logging.getLogger("swarmq.e2e_probe")
        for seq in range(PROBE_COUNT):
            probe.info("exit_probe_%d", seq)
    after_exit = _snapshot()
    return {
        "before": before,
        "after_init": after_init,
        "after_exit": after_exit,
        "task_ids": task_ids,
        "results": results,
    }


def main() -> int:
    mode, queue, broker_url, backend_url = sys.argv[1:5]
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(LINE_FORMAT))
    handler.addFilter(_AnnotateRecord())
    # Im Opt-in bleibt das Root-Level WARNING: ``log_level="INFO"`` am Logger
    # ``swarmq`` muss INFO trotzdem durchlassen (KTD10).
    logging.basicConfig(level=logging.WARNING if mode == "queue" else logging.INFO, handlers=[handler])
    summary = asyncio.run(_run(mode, queue, broker_url, backend_url))
    summary["lines"] = stream.getvalue().splitlines()
    sys.stdout.write(json.dumps(summary) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
