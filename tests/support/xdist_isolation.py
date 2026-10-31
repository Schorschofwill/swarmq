"""Was der Namensraum je xdist-Prozess nicht trennt (Beschleunigung Stufe 3).

``tests/support/services.py`` gibt jedem xdist-Prozess eine eigene vhost und
Valkey-Datenbank. Ein Test, der den ganzen Dienst verändert (Speicheralarm des
Brokers, Skript-Cache von Valkey), trägt ``exclusive``: ``scripts/run-tests.sh``
wählt ihn im parallelen Durchgang mit ``SWARMQ_TEST_EXCLUSIVE=skip`` ab und
führt ihn danach allein mit ``only`` aus. Ebenso ``timing``: Tests mit engem
Zeitbudget gegen die echte Uhr, die parallele Last sonst reißen lässt. Tests mit Toxiproxy (feste
Proxy-Namen und -Ports) bekommen die xdist-Gruppe ``toxiproxy`` und laufen
unter ``--dist loadgroup`` nacheinander auf einem Prozess.

pytest-xdist 3.8 schreibt die Gruppe als Suffix nach ``item._nodeid``; seit
pytest 9.2 bildet ``Item.nodeid`` die ID aus ``_id``, die Gruppe ginge
verloren (TS-04). ``pytest_collection_finish`` reicht sie darum nach ``_id``
weiter, bevor der xdist-Prozess die IDs an den Scheduler meldet.
"""

from __future__ import annotations

import os

import pytest
from _pytest.nodeid import coerce_node_id

EXCLUSIVE_ENV = "SWARMQ_TEST_EXCLUSIVE"
_SERIAL_MARKERS = ("exclusive", "timing")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "exclusive: verändert einen Dienst als Ganzes; läuft nach dem parallelen Durchgang allein"
    )
    config.addinivalue_line(
        "markers", "timing: enges Zeitbudget gegen die echte Uhr; läuft nach dem parallelen Durchgang allein"
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
        exclusive = any(item.get_closest_marker(name) is not None for name in _SERIAL_MARKERS)
        selected = {"": True, "skip": not exclusive, "only": exclusive}[mode]
        (keep if selected else drop).append(item)
    if drop:
        config.hook.pytest_deselected(items=drop)
        items[:] = keep


@pytest.hookimpl(tryfirst=True)
def pytest_collection_finish(session: pytest.Session) -> None:
    for item in session.items:
        # TS-04: vor xdists eigenem Hook, der die IDs an den Scheduler schickt.
        grouped = getattr(item, "_nodeid", None)
        if isinstance(grouped, str) and grouped != item.nodeid:
            item._id = coerce_node_id(grouped)
