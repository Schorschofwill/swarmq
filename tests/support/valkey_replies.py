"""Wortlaut von Valkey-Fehlerantworten, die mehrere Tests nachstellen."""

from __future__ import annotations

#: Antwort auf einen Befehl gegen einen Key mit falschem Typ (``BackendResponseError``).
WRONGTYPE_REPLY = "WRONGTYPE Operation against a key holding the wrong kind of value"
