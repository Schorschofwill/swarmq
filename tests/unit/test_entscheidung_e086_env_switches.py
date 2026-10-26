"""Entscheidung E-086: Schalter aus der Umgebung werden streng gelesen, der
Loglevel überall ohne Rücksicht auf Groß- und Kleinschreibung.

Register: docs/befunde/doku-pruefung-2026-10.md (LÜCKEN V4a und V4b,
entschieden von Georg am 2026-10-26).

V4a: ``SWARMQ_PRIORITIES=on`` galt still als „aus“, weil nur ``true``, ``1``
und ``yes`` als „an“ zählten; der Nachbarschalter ``SWARMQ_SCHEDULER``
verlangt gerade ``on``/``off`` und lehnt Unbekanntes ab.

Testliste:
# [x] on/yes/1/true (beliebige Schreibung) -> an, off/no/0/false -> aus
# [x] unbekannter Wert -> ConfigurationError mit Variablenname
# [x] SWARMQ_LOG_JSON und SWARMQ_CONFIGURE_LOGGING folgen derselben Regel
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from swarmq.cli.parser import _build_parser
from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError
from swarmq.logging_settings import resolve_logging_settings

pytestmark = pytest.mark.entscheidung("E-086")


def _config() -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("on", True),
        ("ON", True),
        (" Yes ", True),
        ("1", True),
        ("true", True),
        ("off", False),
        ("No", False),
        ("0", False),
        ("FALSE", False),
    ],
)
def test_bool_switch_accepts_on_and_off_words(monkeypatch: pytest.MonkeyPatch, raw: str, *, expected: bool) -> None:
    monkeypatch.setenv("SWARMQ_PRIORITIES", raw)

    assert _config().priorities is expected


@pytest.mark.parametrize("raw", ["enable", "tru", "y", "2"])
def test_unknown_bool_switch_value_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("SWARMQ_PRIORITIES", raw)

    with pytest.raises(ConfigurationError, match="SWARMQ_PRIORITIES"):
        _config()


def test_log_json_alias_follows_the_same_rule() -> None:
    cli = SimpleNamespace(log_format=None)

    assert resolve_logging_settings(cli, {"SWARMQ_LOG_JSON": "on"}, None).format == "json"
    with pytest.raises(ConfigurationError, match="SWARMQ_LOG_JSON"):
        resolve_logging_settings(cli, {"SWARMQ_LOG_JSON": "maybe"}, None)


def test_configure_logging_follows_the_same_rule() -> None:
    cli = SimpleNamespace(configure_logging=None)

    assert resolve_logging_settings(cli, {"SWARMQ_CONFIGURE_LOGGING": "off"}, None).configure is False
    with pytest.raises(ConfigurationError, match="SWARMQ_CONFIGURE_LOGGING"):
        resolve_logging_settings(cli, {"SWARMQ_CONFIGURE_LOGGING": "maybe"}, None)


# V4b: Der Loglevel galt eingebettet nur in Großbuchstaben, über
# ``SWARMQ_LOG_LEVEL`` im Worker in jeder Schreibung, als Flag wieder nur groß.
# Testliste:
# [x] SwarmQConfig(log_level="debug") und log_lifecycle_level -> Großbuchstaben
# [x] SWARMQ_LOG_LEVEL=debug eingebettet -> DEBUG
# [x] swarmq worker --log-level debug -> DEBUG
# [x] unbekannter Level bleibt ein ConfigurationError


def test_embedded_log_levels_ignore_case() -> None:
    config = SwarmQConfig(
        broker_url="amqp://x", backend_url="valkey://x", log_level="debug", log_lifecycle_level="Info"
    )

    assert (config.log_level, config.log_lifecycle_level) == ("DEBUG", "INFO")


def test_embedded_log_level_from_env_ignores_case(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_LOG_LEVEL", "warning")

    assert _config().log_level == "WARNING"


def test_cli_log_level_flag_ignores_case() -> None:
    args = _build_parser().parse_args(["worker", "--module", "app.tasks", "--log-level", "debug"])

    assert args.log_level == "DEBUG"


def test_unknown_log_level_stays_a_configuration_error() -> None:
    with pytest.raises(ConfigurationError, match="log_level"):
        SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", log_level="verbose")
