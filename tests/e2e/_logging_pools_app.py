"""App-Modul fuer ``test_logging_pools_e2e`` (Plan 2026-10-06-1811, U9: R12, R13).

Wie eine App, die beim Import selbst Logging einrichtet. Was sie einrichtet,
waehlt ``POOLS_APP_MODE`` -- nur in der Umgebung des Workers gesetzt, damit der
Testprozess das Modul zum Einreihen importieren kann, ohne sein eigenes Root
anzufassen:

- ``basic``: ``basicConfig(level=INFO, format="APPFMT|...")``
- ``basic_nolevel``: ``basicConfig(format="APPFMT|...")``, Root-Level bleibt
- ``cookbook``: dictConfig mit ``QueueHandler`` vor einer Datei
  (``POOLS_APP_LOG``) und einem StreamHandler; den Listener startet erst der ``--configure``-Hook
  ``start_listener`` -- also nur im Worker, nie im Forkserver
- ``cookbook_start``: wie ``cookbook``, der Listener startet aber schon beim
  Import
"""

from __future__ import annotations

import logging
import logging.config
import logging.handlers
import os
from typing import Any

from swarmq.task import Task, TaskInfo

APP_FORMAT = "APPFMT|%(name)s|%(levelname)s|%(message)s"
_MODE = os.environ.get("POOLS_APP_MODE")

if _MODE == "basic":
    logging.basicConfig(level=logging.INFO, format=APP_FORMAT)
elif _MODE == "basic_nolevel":
    logging.basicConfig(format=APP_FORMAT)
elif _MODE in ("cookbook", "cookbook_start"):
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "plain": {"format": "COOKBOOK|%(process)d|%(name)s|%(message)s"},
                "json": {"()": "swarmq.logging.build_formatter", "json": True, "colors": False},
            },
            "handlers": {
                "file": {"class": "logging.FileHandler", "filename": os.environ["POOLS_APP_LOG"], "formatter": "plain"},
                "queue_handler": {"class": "logging.handlers.QueueHandler", "handlers": ["file"]},
                # Damit der Test den Start des Workers samt Feldern auf stderr sieht.
                "err": {"class": "logging.StreamHandler", "formatter": "json"},
            },
            "root": {"level": "INFO", "handlers": ["queue_handler", "err"]},
        }
    )
    if _MODE == "cookbook_start":
        # Cookbook wortwoertlich: Start beim Import -- auch im Forkserver.
        _handler = logging.getHandlerByName("queue_handler")
        assert isinstance(_handler, logging.handlers.QueueHandler)
        assert _handler.listener is not None
        _handler.listener.start()


def start_listener(app: Any) -> None:
    """``--configure``-Hook: startet den Listener der App (Cookbook-Muster)."""
    handler = logging.getHandlerByName("queue_handler")
    assert isinstance(handler, logging.handlers.QueueHandler)
    assert handler.listener is not None
    handler.listener.start()


log = logging.getLogger("poolsapp")


class PoolsTask(Task):
    task_info = TaskInfo(name="pools_task", queues=[os.environ.get("CLI_TEST_QUEUE", "pools")], max_retries=0)

    async def run(self, value: str) -> str:
        log.info("pools_info_%s", value)
        log.warning("pools_warn_%s", value)
        return f"ok:{value}"
