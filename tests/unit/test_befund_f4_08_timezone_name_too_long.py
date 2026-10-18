"""Nachweis F4-08: Eine Zeitzone mit einem Pfadteil über 255 Byte ergibt
``OSError`` statt des zugesagten Fehlers.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-08; Verdacht
R4F-02; Gegenprüfung ``6-gegenpruefung-F.md``; Fuzz-Treffer in
``tests/fuzz/test_config_env_fuzz.py``, geschrumpft
``SWARMQ_SCHEDULER_TIMEZONE="a" * 256``).

``normalize_timezone`` (``config.py``) fängt nur ``ZoneInfoNotFoundError`` und
``ValueError``; ``zoneinfo`` sucht den Namen als Datei und wirft für einen
Pfadteil über 255 Byte ``OSError`` (ENAMETOOLONG). Folge für
``SWARMQ_SCHEDULER_TIMEZONE``: Exit 1 mit Neustarts statt 78 (E-057).

Erwartet:
- ``configuration.md:123`` (``SWARMQ_SCHEDULER_TIMEZONE``): „An unknown zone is
  a configuration error.“ -> ``ConfigurationError``.
- ``configuration.md:482`` (``TaskInfo.timezone``): „an unknown zone raises
  ``ValueError``“ (E-065), Docstring ``normalize_timezone``.

``next_fire_times`` wirft für unbekannte Zonen heute ``ZoneInfoNotFoundError``;
dafür gibt es keine Zusage, es gehört nicht in diesen Nachweis.

Behoben in 0.13.0 (U16): ``normalize_timezone`` fängt auch ``OSError``;
``cron_time._zone`` wandelt ihn in ``ZoneInfoNotFoundError``
(``tests/unit/scheduler/test_cron_time.py``).

Testliste:
# [x] SWARMQ_SCHEDULER_TIMEZONE = "a" * 256 / "Europe/" + "x" * 300 -> ConfigurationError
# [x] TaskInfo(timezone="a" * 256 / "Europe/" + "x" * 300) -> ValueError
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError
from swarmq.task import TaskInfo

pytestmark = pytest.mark.timeout(5)

_LONG_ZONES = pytest.mark.parametrize("zone", ["a" * 256, "Europe/" + "x" * 300], ids=["256", "europe-300"])


@pytest.mark.befund("F4-08")
@_LONG_ZONES
def test_scheduler_timezone_with_a_too_long_name_is_a_configuration_error(
    zone: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWARMQ_SCHEDULER_TIMEZONE", zone)

    error: BaseException | None = None
    try:
        SwarmQConfig(broker_url="amqp://x", backend_url="redis://x")
    except Exception as exc:
        error = exc

    assert isinstance(error, ConfigurationError), f"statt ConfigurationError (Exit 78): {error!r}"
    assert "scheduler_timezone" in str(error)


@pytest.mark.befund("F4-08")
@_LONG_ZONES
def test_task_timezone_with_a_too_long_name_raises_value_error(zone: str) -> None:
    error: BaseException | None = None
    try:
        TaskInfo(name="f4_08_tz", queues=["default"], cron="0 3 * * *", timezone=zone)
    except Exception as exc:
        error = exc

    assert isinstance(error, ValueError), f"statt ValueError: {error!r}"
