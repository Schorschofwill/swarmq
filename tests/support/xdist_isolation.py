"""Was der Namensraum je xdist-Prozess nicht trennt (Beschleunigung Stufe 3).

``tests/support/services.py`` gibt jedem xdist-Prozess eine eigene vhost und
Valkey-Datenbank. Ein Test, der den ganzen Dienst verändert (Speicheralarm des
Brokers, Skript-Cache von Valkey), trägt ``exclusive``: ``scripts/run-tests.sh``
wählt ihn im parallelen Durchgang mit ``SWARMQ_TEST_EXCLUSIVE=skip`` ab und
führt ihn danach allein mit ``only`` aus. Tests mit Toxiproxy (feste
Proxy-Namen und -Ports) bekommen die xdist-Gruppe ``toxiproxy`` und laufen
unter ``--dist loadgroup`` nacheinander auf einem Prozess.
"""

from __future__ import annotations

import os

import pytest

EXCLUSIVE_ENV = "SWARMQ_TEST_EXCLUSIVE"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "exclusive: verändert einen Dienst als Ganzes; läuft nach dem parallelen Durchgang allein"
    )


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    mode = os.environ.get(EXCLUSIVE_ENV, "")
    if mode not in ("", "skip", "only"):
        msg = f"{EXCLUSIVE_ENV}={mode!r}: expected 'skip', 'only' or unset"
        raise pytest.UsageError(msg)
    keep: list[pytest.Item] = []
    drop: list[pytest.Item] = []
    for item in items:
        if "toxiproxy" in getattr(item, "fixturenames", ()) and item.get_closest_marker("xdist_group") is None:
            # Feste Proxy-Namen und -Ports: alle Toxiproxy-Tests auf einem Prozess.
            item.add_marker(pytest.mark.xdist_group("toxiproxy"))
        exclusive = item.get_closest_marker("exclusive") is not None
        selected = {"": True, "skip": not exclusive, "only": exclusive}[mode]
        (keep if selected else drop).append(item)
    if drop:
        config.hook.pytest_deselected(items=drop)
        items[:] = keep
