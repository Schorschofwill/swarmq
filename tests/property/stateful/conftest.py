"""Store-Factory Fake der Zustandsmaschinen (Plan 2026-10-17-0941, KTD2).

Die Maschinen bekommen eine Coroutine-Factory, die je Hypothesis-Beispiel im
Loop der Maschine eine frische Umgebung öffnet; hier ist das
``open_inmemory_env`` (``InMemoryBackend`` samt Stores gegen eine
``FakeClock``). ``tests/integration/stateful/conftest.py`` bindet dieselbe
Fixture-Signatur an die Valkey-Stores. Hypothesis-Profil und Marker
``property`` kommen aus ``tests/property/conftest.py``.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from tests.support.stateful import InMemoryEnv, open_inmemory_env


@pytest.fixture
def open_env() -> Callable[[], Coroutine[Any, Any, InMemoryEnv]]:
    return open_inmemory_env
