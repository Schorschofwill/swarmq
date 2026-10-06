"""Logging-Konfiguration aus einer dictConfig-Datei (Plan 2026-10-06-0831, KTD9).

Ausgelagert aus ``swarmq.logging`` (800-Zeilen-Guard): Laden und Pruefen der
Datei sowie die Listener der ``QueueHandler``, die ``dictConfig`` anlegt, aber
nicht startet.
"""

from __future__ import annotations

import json as jsonlib
import logging
import logging.handlers
import queue
import threading
from collections.abc import Iterator
from typing import Any

from swarmq.exceptions import LoggingConfigError

CONFIG_LISTENERS: list[logging.handlers.QueueListener] = []
"""Von ``configure_logging`` gestartete Listener aus einer dictConfig-Datei;
``shutdown_logging`` stoppt und leert sie."""


def load_log_config(path: str) -> str:
    """Laedt eine dictConfig-Datei und liefert sie als JSON-String (KTD9).

    Fehlt ``disable_existing_loggers``, gilt ``False``: sonst wuerden
    bereits importierte ``swarmq.*``- und ``aio_pika``-Logger stumm. Der
    String ist pickle-sicher und geht an die Kindprozesse, die die Datei
    selbst nie lesen.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            config = _parse_log_config(path, fh.read())
    except (OSError, ValueError, ImportError) as exc:
        # ValueError deckt JSONDecodeError und UnicodeDecodeError ab.
        raise LoggingConfigError(path, exc) from exc
    if not isinstance(config, dict):
        cause = TypeError(f"expected a mapping, got {type(config).__name__}")
        raise LoggingConfigError(path, cause) from cause
    config.setdefault("disable_existing_loggers", False)
    return jsonlib.dumps(config)


def _parse_log_config(path: str, text: str) -> Any:
    """JSON immer, YAML nur mit installiertem PyYAML (KTD9)."""
    if not path.endswith((".yaml", ".yml")):
        return jsonlib.loads(text)
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        msg = "YAML logging configs need PyYAML (pip install pyyaml)"
        raise ImportError(msg) from exc
    return yaml.safe_load(text)


def _iter_loggers() -> Iterator[tuple[str, logging.Logger]]:
    """Name und Logger aller angelegten Logger ohne Root und Platzhalter;
    ueber eine Kopie, weil andere Threads Logger anlegen koennen."""
    for name, lg in list(logging.Logger.manager.loggerDict.items()):
        if isinstance(lg, logging.Logger):
            yield name, lg


def start_queue_listeners() -> None:
    """Startet die Listener aller ``QueueHandler`` aus der dictConfig (KTD9).

    ``dictConfig`` legt sie an, startet sie aber nicht. Gestoppt -- und damit
    geleert -- werden sie per ``shutdown_logging``, also auch bei
    Prozessende (``atexit``).
    """
    loggers = [logging.getLogger()] + [lg for _, lg in _iter_loggers()]
    for handler in {h for lg in loggers for h in lg.handlers}:
        listener = getattr(handler, "listener", None)
        if (
            isinstance(handler, logging.handlers.QueueHandler)
            and isinstance(listener, logging.handlers.QueueListener)
            and getattr(listener, "_thread", None) is None
        ):
            listener.start()
            CONFIG_LISTENERS.append(listener)
            _restart_lazily(handler, listener)


def stop_queue_listeners() -> None:
    """Stoppt und leert die gestarteten dictConfig-Listener (KTD5).

    Der stdlib-``stop`` reiht den Sentinel per ``put_nowait`` ein und wirft
    bei voller Queue ``queue.Full`` -- mitten in ``atexit`` oder einem
    ``finally``. Dann wird blockierend nachgereicht: Der laufende Listener
    schafft Platz, wie bei ``_SwarmqQueueListener.enqueue_sentinel``.
    """
    while CONFIG_LISTENERS:
        listener = CONFIG_LISTENERS.pop()
        try:
            listener.stop()
        except queue.Full:
            listener.queue.put(listener._sentinel)  # type: ignore[attr-defined]
            thread = getattr(listener, "_thread", None)
            if thread is not None:
                thread.join()
            setattr(listener, "_thread", None)  # noqa: B010


_CONFIG_LISTENER_LOCK = threading.Lock()
"""Schuetzt den Wiederstart der dictConfig-Listener; eigenes Lock, weil ein
``_SwarmqQueueListener`` aus ``swarmq.logging`` in der Datei beim Start
dessen ``_LISTENER_LOCK`` nimmt."""


def _restart_lazily(
    handler: logging.handlers.QueueHandler, listener: logging.handlers.QueueListener
) -> None:
    """Wie beim eingebauten Handler (KTD5): Nach ``shutdown_logging`` startet
    der naechste Eintrag den Listener wieder, und das naechste
    ``shutdown_logging`` leert ihn. Das ist die Leerung pro Job im
    wiederverwendeten Compute-Kind (KTD14)."""
    enqueue = handler.enqueue

    def enqueue_restarting(record: logging.LogRecord) -> None:
        if getattr(listener, "_thread", None) is None:
            with _CONFIG_LISTENER_LOCK:
                if getattr(listener, "_thread", None) is None:
                    listener.start()
                    CONFIG_LISTENERS.append(listener)
        enqueue(record)

    # Am Exemplar: die Klasse des Handlers gehoert der dictConfig der App.
    setattr(handler, "enqueue", enqueue_restarting)  # noqa: B010
