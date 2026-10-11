"""Header-Schutz an der Broker-Grenze (BZ-17, Plan 0.12.0 MR 2).

Der AMQP-Content-Header muss in einen Frame passen (``frame_max``); ein
zu grosser Header schliesst die ganze Verbindung, und alle unbestaetigten
Nachbar-Zustellungen kommen zurueck. Ab der Haelfte von ``frame_max``
(Rest fuer die uebrigen Properties) werden lange Texte gekuerzt.
"""

from __future__ import annotations

from typing import Any

import pamqp.encode

from swarmq.exceptions import MessageHeadersTooLarge
from swarmq.header_text import bounded_header_text


def _encoded_size(headers: dict[str, Any]) -> int:
    return len(pamqp.encode.field_table(headers))


def guard_headers(headers: dict[str, Any] | None, frame_max: int) -> dict[str, Any] | None:
    """``headers``, so dass der Content-Header sicher in einen Frame passt.

    Unveraendert, solange die kodierten Header hoechstens ``frame_max / 2``
    Bytes brauchen; sonst eine Kopie mit gekuerzten Texten
    (``bounded_header_text``). Passt auch die nicht, ``MessageHeadersTooLarge``.
    """
    if not headers:
        return headers
    limit = frame_max // 2
    if _encoded_size(headers) <= limit:
        return headers
    bounded = {name: bounded_header_text(value) if isinstance(value, str) else value for name, value in headers.items()}
    size = _encoded_size(bounded)
    if size > limit:
        raise MessageHeadersTooLarge(
            f"message headers take {size} bytes even with long texts truncated; "
            f"the limit is {limit} bytes (half of frame_max {frame_max})"
        )
    return bounded


# RabbitMQ-Default; gilt, solange der ausgehandelte Wert nicht lesbar ist.
DEFAULT_FRAME_MAX = 131_072


def negotiated_frame_max(connection: Any) -> int:
    """Das mit dem Broker ausgehandelte ``frame_max`` der Verbindung.

    Nicht lesbar (keine oder getrennte Verbindung, Test-Double, anderer
    Transport) -> ``DEFAULT_FRAME_MAX``. Zugriffspfad laut Spike S6.
    """
    try:
        frame_max = connection.transport.connection.connection_tune.frame_max
    except AttributeError:
        return DEFAULT_FRAME_MAX
    if isinstance(frame_max, int) and not isinstance(frame_max, bool) and frame_max > 0:
        return frame_max
    return DEFAULT_FRAME_MAX
