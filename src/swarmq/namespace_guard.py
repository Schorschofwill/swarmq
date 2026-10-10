"""0.11.1 verweigert einen gesetzten Namensraum (Rollback-Ziel vor 0.12).

Ab 0.12 setzt ``SWARMQ_NAMESPACE`` ein Präfix vor alle Valkey-Keys und
Pub/Sub-Kanäle. 0.11.x kennt das Präfix nicht; ein Rollback fiele still in
den Zustand ohne Präfix (Termine, Locks, Ergebnisse unter anderen Keys).
Darum endet der Start hier mit ``ConfigurationError`` (Exit 78).

Die Variable ist bewusst kein Feld von ``SwarmQConfig``: 0.11.1 unterstützt
sie nicht, es lehnt sie nur ab.
"""

from __future__ import annotations

import os

from swarmq.exceptions import ConfigurationError

NAMESPACE_ENV = "SWARMQ_NAMESPACE"


def reject_namespace() -> None:
    """``ConfigurationError``, wenn ``SWARMQ_NAMESPACE`` gesetzt ist."""
    value = os.environ.get(NAMESPACE_ENV, "")
    if not value:
        return
    msg = (
        f"{NAMESPACE_ENV}={value!r} is set, but a namespace requires swarmq >= 0.12; "
        f"this is swarmq 0.11.x, which would silently use the keys without namespace. "
        f"Unset {NAMESPACE_ENV} or run swarmq >= 0.12."
    )
    raise ConfigurationError(msg)
