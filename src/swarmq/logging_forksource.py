"""Zustand der Fork-Quelle (Forkserver) fuer das Logging (Plan 2026-10-06-1811,
U6/U9, KTD6/KTD9).

Gebuendelt an einer Stelle: das Flag "dieser Prozess ist eine Fork-Quelle"
(dort startet kein Listener-Thread) und der Platzhalter-Handler, den der
Forkserver vor dem App-Import an Root haengt. Gesetzt von
``swarmq.worker.compute.preload``; gelesen von ``swarmq.logging_queue`` (Flag)
und ``swarmq.logging.configure_child_logging`` (Platzhalter). Importiert kein
anderes swarmq-Modul.
"""

from __future__ import annotations

import logging
import os
from typing import Any


class _ForkSourceSentinel(logging.Handler):
    """Platzhalter an Root im Forkserver (Plan 2026-10-06-1811, U9, KTD9).

    Haengt vor jedem App-Import an Root: Ein ``basicConfig`` der App findet
    dann einen Handler vor und bleibt wirkungslos -- wie im IO-Worker, der
    vor dem App-Import einrichtet. Ohne Thread (KTD6).

    Kein reiner NullHandler: Records ab ``logging.lastResort.level`` gibt er
    an ``logging.lastResort`` weiter. Warnungen aus dem Forkserver bleiben so
    sichtbar -- genau einmal: Mit einem Handler an Root greift ``lastResort``
    selbst nicht. Ein per Fork entstandenes Kind beginnt ohne Sentinel
    (:func:`_after_fork_in_child`).
    Weitergegeben wird wie in der stdlib nur, wenn kein anderer Handler auf
    dem Weg des Records liegt (Schluss-Review).
    """

    def emit(self, record: logging.LogRecord) -> None:
        last = logging.lastResort
        if last is not None and record.levelno >= last.level and not self._others_on_path(record):
            last.handle(record)

    def _others_on_path(self, record: logging.LogRecord) -> bool:
        """Faende ``Logger.callHandlers`` ohne den Sentinel einen Handler?
        Die Opt-in-Queue zaehlt nicht (``forwards_to_root``): Sie reicht den
        Record selbst an die Root-Handler weiter, also an diesen Sentinel."""
        current: Any = logging.Logger.manager.loggerDict.get(record.name)
        if not isinstance(current, logging.Logger):
            current = logging.getLogger()
        while current is not None:
            if any(
                h is not self and not getattr(h, "forwards_to_root", False)
                for h in current.handlers
            ):
                return True
            current = current.parent if current.propagate else None
        return False


class _ForkSource:
    """Zustand der Fork-Quelle dieses Prozesses."""

    def __init__(self) -> None:
        self.active = False
        """Ist dieser Prozess eine Fork-Quelle (Forkserver)? Dann geben
        Queue-Handler synchron aus, statt einen Listener-Thread zu starten
        (KTD6)."""
        self.sentinel: _ForkSourceSentinel | None = None
        """Der vom Forkserver geerbte Sentinel; ``configure_child_logging``
        vergleicht mit ihm wie ``react_to_app_logging`` mit dem Schnappschuss
        vor dem App-Import. Auch dann bekannt, wenn die App ihn von Root
        entfernt hat."""
        self.detached_at_fork = False
        """Hing der Sentinel beim Fork an Root? Im Kind hat ihn der Fork-Hook
        abgehaengt; ``configure_child_logging`` wertet das wie seine
        Anwesenheit."""


FORK_SOURCE = _ForkSource()


def mark_fork_source() -> None:
    """Markiert den Prozess als Fork-Quelle: Queue-Handler geben synchron
    ueber die Ziel-Handler aus, statt einen Listener-Thread zu starten, und
    der Forkserver bleibt einfaedig (KTD6). Der Fork-Hook hebt es im Kind
    auf."""
    FORK_SOURCE.active = True


def install_fork_source_sentinel() -> None:
    """Haengt den Sentinel an Root (``swarmq.worker.compute.preload``).

    Hat Root schon Handler, hat ein vorher importiertes Hauptmodul (Start per
    Pfad, KTD6-Grenze) das Logging eingerichtet: Es gewinnt wie bisher, und
    es gibt keinen Sentinel.
    """
    root = logging.getLogger()
    if not root.handlers:
        FORK_SOURCE.sentinel = _ForkSourceSentinel()
        root.addHandler(FORK_SOURCE.sentinel)


def take_fork_source_sentinel() -> tuple[_ForkSourceSentinel | None, bool]:
    """Fuer ``configure_child_logging``: der geerbte Sentinel und ob er beim
    Fork an Root hing; danach ist beides vergessen."""
    sentinel, detached = FORK_SOURCE.sentinel, FORK_SOURCE.detached_at_fork
    FORK_SOURCE.sentinel, FORK_SOURCE.detached_at_fork = None, False
    return sentinel, detached


def _after_fork_in_child() -> None:
    """``after_in_child``: Das Kind ist keine Fork-Quelle mehr (KTD6), der
    naechste Eintrag startet seinen Listener. Und es beginnt ohne Sentinel an
    Root (Schluss-Review) -- auch eines, das die App selbst vom gemeinsamen
    Forkserver holt (``ProcessPoolExecutor``) und das
    ``configure_child_logging`` nie ruft. Dort wirkt ``basicConfig`` wieder.
    Ob er an Root hing, merkt sich das Kind fuer ``configure_child_logging``."""
    FORK_SOURCE.active = False
    sentinel = FORK_SOURCE.sentinel
    root = logging.getLogger()
    if sentinel is not None and sentinel in root.handlers:
        root.removeHandler(sentinel)
        FORK_SOURCE.detached_at_fork = True


os.register_at_fork(after_in_child=_after_fork_in_child)
