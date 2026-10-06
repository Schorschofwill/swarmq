"""App-Modul, das beim Import sein eigenes Logging einrichtet (e2e zu R12).

Fuer ``swarmq worker --no-configure-logging``: swarmq richtet nichts ein,
die Zeilen kommen im Format dieser App. ``%(task_id)s`` liest das Feld als
LogRecord-Attribut (R2); Records ohne Task-Kontext bekommen ``-``.
Nur im Worker-Prozessbaum importieren -- im Testprozess wuerde es den
Root-Logger von pytest umbauen.
"""

from __future__ import annotations

import logging
import sys

from tests.e2e._logging_fixture_tasks import LogAsyncEcho, LogCpuSum  # noqa: F401 -- registriert die Tasks

APP_PREFIX = "APPFMT"

_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(
    logging.Formatter(
        f"{APP_PREFIX}|%(process)d|%(name)s|%(levelname)s|%(message)s|task_id=%(task_id)s",
        defaults={"task_id": "-"},
    )
)
_root = logging.getLogger()
_root.handlers[:] = [_handler]
_root.setLevel(logging.INFO)
