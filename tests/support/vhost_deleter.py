"""Löscht Worker-vhosts in einem abgelösten Prozess (TS-05).

Bricht der Client ein ``DELETE /vhosts/<v>`` ab, bricht RabbitMQ (4.3.6 und
4.2.9) auch die Löschung ab: vhost und ein Teil der Queues bleiben liegen.
Am Ende einer xdist-Sitzung zählt der Abbau aber zur pytest-timeout-Frist des
letzten Tests. ``worker_vhosts`` startet dieses Skript deshalb abgelöst
(``services.delete_vhosts_detached``); es lässt jedem ``DELETE`` Zeit und
löscht auf allen Brokern gleichzeitig.

Aufruf: ``python vhost_deleter.py <management-url> <vhost-pfadsegment> ...``.
Das Skript läuft über seinen Pfad, nicht als Modul, und importiert nur httpx.
"""

from __future__ import annotations

import contextlib
import sys
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor

import httpx

# Unter Last braucht RabbitMQ für eine vhost mit Hunderten Quorum-Queues Sekunden.
DELETE_TIMEOUT_S = 120.0


def delete_vhosts(targets: Sequence[tuple[str, str]], *, transport: httpx.BaseTransport | None = None) -> None:
    """Löscht je Paar aus Management-URL und vhost-Pfadsegment die vhost."""

    def delete(target: tuple[str, str]) -> None:
        base_url, vhost_api = target
        with (
            contextlib.suppress(httpx.HTTPError),  # Aufräumen ist best effort
            httpx.Client(
                base_url=base_url, auth=("guest", "guest"), timeout=DELETE_TIMEOUT_S, transport=transport
            ) as client,
        ):
            client.delete(f"/vhosts/{vhost_api}")

    with ThreadPoolExecutor(max_workers=max(len(targets), 1)) as pool:
        list(pool.map(delete, targets))


def main(argv: Sequence[str]) -> None:
    delete_vhosts(list(zip(argv[::2], argv[1::2], strict=True)))


if __name__ == "__main__":
    main(sys.argv[1:])
