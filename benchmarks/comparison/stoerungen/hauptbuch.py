"""Hauptbuch des Garantie-Vergleichs (Plan 2026-10-24-1118, U9, KTD14).

Eine SQLite-Datei auf dem Host, damit keine der gestörten Komponenten das
Buch mitreißt. Der Erzeuger schreibt jeden Publish mit Ausgang, die Worker
der fünf Systeme jede Ausführung über dieselbe Task-Funktion. Jeder Zugriff
öffnet eine eigene Verbindung: Geschrieben wird aus vielen Prozessen und
Threads, auch aus geforkten Pool-Prozessen.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from pathlib import Path

from stoerungen.bilanz import Ausfuehrung, Publish

# WAL: Leser und Schreiber blockieren sich nicht, und ein Prozess, der
# mitten im Schreiben stirbt (SIGKILL des Workers), lässt das Buch heil.
_SCHEMA = """
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS publish (kennung TEXT PRIMARY KEY, ausgang TEXT NOT NULL, zeit REAL NOT NULL);
CREATE TABLE IF NOT EXISTS beginn (kennung TEXT NOT NULL, zeit REAL NOT NULL);
CREATE TABLE IF NOT EXISTS ausfuehrung (kennung TEXT NOT NULL, zeit REAL NOT NULL);
CREATE TABLE IF NOT EXISTS ereignis (art TEXT NOT NULL, zeit REAL NOT NULL);
CREATE TABLE IF NOT EXISTS worker (pid INTEGER NOT NULL);
"""


#: Pfad des Buchs für die Worker; der Lauf setzt ihn, die Worker erben ihn.
UMGEBUNG = "VERGLEICH_HAUPTBUCH"


class Hauptbuch:
    """Zugriff auf eine Hauptbuch-Datei."""

    def __init__(self, pfad: Path | str) -> None:
        self.pfad = Path(pfad)
        db = self._verbindung()
        try:
            db.executescript(_SCHEMA)
        finally:
            db.close()

    def _verbindung(self) -> sqlite3.Connection:
        return sqlite3.connect(self.pfad, timeout=30.0)

    def _schreiben(self, sql: str, *werte: object) -> None:
        db = self._verbindung()
        try:
            with db:
                db.execute(sql, werte)
        finally:
            db.close()

    def publish(self, kennung: str, ausgang: str) -> None:
        self._schreiben("INSERT INTO publish VALUES (?, ?, ?)", kennung, ausgang, time.time())

    def begonnen(self, kennung: str) -> None:
        self._schreiben("INSERT INTO beginn VALUES (?, ?)", kennung, time.time())

    def ausgefuehrt(self, kennung: str) -> None:
        self._schreiben("INSERT INTO ausfuehrung VALUES (?, ?)", kennung, time.time())

    def ereignis(self, art: str) -> None:
        """Hält einen Zeitpunkt des Laufs fest, etwa den Valkey-Neustart ohne Persistenz (E-077)."""
        self._schreiben("INSERT INTO ereignis VALUES (?, ?)", art, time.time())

    def ereignis_zeit(self, art: str) -> float | None:
        """Erster Zeitpunkt des Ereignisses ``art``, ``None`` ohne Ereignis."""
        db = self._verbindung()
        try:
            [(zeit,)] = db.execute("SELECT MIN(zeit) FROM ereignis WHERE art = ?", (art,)).fetchall()
        finally:
            db.close()
        return None if zeit is None else float(zeit)

    def worker(self, pid: int) -> None:
        """Merkt die PID eines gestarteten Workers, damit der Vergleich keinen zurücklässt."""
        self._schreiben("INSERT INTO worker VALUES (?)", pid)

    def worker_pids(self) -> list[int]:
        db = self._verbindung()
        try:
            return [pid for (pid,) in db.execute("SELECT pid FROM worker")]
        finally:
            db.close()

    def _zahl(self, sql: str) -> int:
        db = self._verbindung()
        try:
            [(zahl,)] = db.execute(sql).fetchall()
        finally:
            db.close()
        return int(zahl)

    def laufend(self) -> int:
        """Begonnene Tasks ohne Ausführung: Belegt, dass eine Störung einen laufenden Task trifft."""
        return self._zahl(
            "SELECT COUNT(DISTINCT kennung) FROM beginn WHERE kennung NOT IN (SELECT kennung FROM ausfuehrung)"
        )

    def zeilen(self) -> tuple[list[Publish], list[Ausfuehrung]]:
        """Alle Publishes und Ausführungen für ``bilanz.bilanz``."""
        db = self._verbindung()
        try:
            publishes = [Publish(k, a) for k, a in db.execute("SELECT kennung, ausgang FROM publish")]
            ausfuehrungen = [Ausfuehrung(k, z) for k, z in db.execute("SELECT kennung, zeit FROM ausfuehrung")]
        finally:
            db.close()
        return publishes, ausfuehrungen


def garantie(kennung: str, dauer_ms: int) -> None:
    """Gemeinsame Task der Worker: arbeitet ``dauer_ms`` und schreibt die Ausführung ins Buch.

    Die Ausführung zählt erst am Ende: Ein Task, den ein SIGKILL mitten im
    Lauf trifft, ist nicht ausgeführt.
    """
    buch = Hauptbuch(os.environ[UMGEBUNG])
    buch.begonnen(kennung)
    time.sleep(dauer_ms / 1000)
    buch.ausgefuehrt(kennung)


async def garantie_async(kennung: str, dauer_ms: int) -> None:
    """``garantie`` für Systeme mit asynchronen Tasks (swarmq, arq, taskiq)."""
    buch = Hauptbuch(os.environ[UMGEBUNG])
    buch.begonnen(kennung)
    await asyncio.sleep(dauer_ms / 1000)
    buch.ausgefuehrt(kennung)
