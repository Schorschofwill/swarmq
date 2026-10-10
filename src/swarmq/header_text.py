"""Begrenzte Freitext-Header (BZ-17, Plan 0.12.0 MR 2).

Freitext wie Fehlertexte landet in AMQP-Headern; der Content-Header muss in
einen Frame passen (``frame_max``), sonst schliesst der Broker die
Verbindung. Neutral (Q16): Worker und Broker nutzen denselben Helfer.
"""

from __future__ import annotations

HEADER_TEXT_LIMIT_BYTES = 8 * 1024
_TAIL_BYTES = 2 * 1024


def bounded_header_text(text: str) -> str:
    """``text``, begrenzt auf ``HEADER_TEXT_LIMIT_BYTES`` (UTF-8).

    Laengerer Text behaelt den Anfang (rund 6 KiB, Ausnahme und erste
    Frames) und die letzten 2 KiB (innerster Frame); dazwischen steht ein
    Vermerk ueber die Kuerzung.
    """
    # Einzelne Surrogates (surrogateescape) sind kein UTF-8; sichtbar
    # escapen statt am Publish zu scheitern.
    raw = text.encode(errors="backslashreplace")
    if len(raw) <= HEADER_TEXT_LIMIT_BYTES:
        return raw.decode()
    marker = f"\n[... {len(raw) - HEADER_TEXT_LIMIT_BYTES} bytes truncated ...]\n"
    head_bytes = HEADER_TEXT_LIMIT_BYTES - _TAIL_BYTES - len(marker.encode())
    head = raw[:head_bytes].decode(errors="ignore")
    tail = raw[-_TAIL_BYTES:].decode(errors="ignore")
    return head + marker + tail
