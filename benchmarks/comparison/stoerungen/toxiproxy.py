"""Schlanker Client für die REST-API von Toxiproxy (Vorlage: ``tests/chaos/conftest.py``).

Nur Standardbibliothek, weil er in den Umgebungen aller fünf Systeme läuft.
Abschalten (``enabled: false``) schließt alle offenen Verbindungen des
Proxys und nimmt keine neuen an: die Netztrennung des Vergleichs.
"""

from __future__ import annotations

import json
import urllib.request


def _setzen(admin_url: str, proxy: str, *, aktiv: bool) -> None:
    anfrage = urllib.request.Request(  # noqa: S310 - feste http-Adresse des eigenen Stacks
        f"{admin_url}/proxies/{proxy}",
        data=json.dumps({"enabled": aktiv}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(anfrage, timeout=10) as antwort:  # noqa: S310 - wie oben
        antwort.read()


def trennen(admin_url: str, proxies: list[str]) -> None:
    """Schaltet die Proxys ab."""
    for proxy in proxies:
        _setzen(admin_url, proxy, aktiv=False)


def verbinden(admin_url: str, proxies: list[str]) -> None:
    """Schaltet die Proxys wieder ein."""
    for proxy in proxies:
        _setzen(admin_url, proxy, aktiv=True)
