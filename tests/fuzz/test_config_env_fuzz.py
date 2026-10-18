"""Fuzz: ``SwarmQConfig()`` unter gesetzten ``SWARMQ_*``-Variablen
(Fehlersuche Runde 4, U16, R20, KTD8).

Ziel ist die Konstruktion ``SwarmQConfig(...)``: ``__post_init__`` wendet die
Umgebung über ``_ENV_OVERRIDES`` und ``config.parse_env_value`` an und prüft
danach (es gibt kein ``from_env``). Je Beispiel werden ein bis sechs
Variablen per ``MonkeyPatch.context`` gesetzt, alle übrigen entfernt.

Zusagen:

* Ein ungültiger Wert ist ein ``ConfigurationError`` (Exit 78, E-057), der
  die Variable oder das Feld nennt (E-063: "Meldung nennt das ursächliche
  Feld"; ``config_validation._validate_ranges`` und ``_validate_core``). Nie ein roher
  ``ValueError``/``OverflowError``/``OSError`` (Plan U16, Approach 3).
* ``inf`` und ``nan`` in Frist-, TTL- und Zeitfeldern sind ein
  ``ConfigurationError`` (E-063, E-055 Nachtrag für ``SWARMQ_RESULT_TTL``).
* Eine angenommene Konfiguration hat endliche Fristen, die sich in ganze
  Millisekunden umrechnen lassen (E-063, Begründung: "Die Werte werden in
  ganze Millisekunden umgerechnet") und höchstens ``MAX_DEADLINE_MS`` ergeben
  (E-063, Nachtrag 2026-10-17: Grenze von RabbitMQ-``expiration`` und Valkey
  ``PX``).

Allow-List: ``ConfigurationError``. Jede andere Ausnahme ist ein Treffer.

Treffer (Bericht docs/befunde/fehlersuche-runde-4-recherche/5-fuzz.md), alle
in 0.13.0 behoben (U16), die Ausklammerungen sind entfernt:

* F4-08 / R4F-02 (FEHLER): ``SWARMQ_SCHEDULER_TIMEZONE`` mit einem Pfadteil über
  255 Byte ergab ``OSError`` (ENAMETOOLONG) aus ``zoneinfo``. Nachweis
  ``tests/unit/test_befund_f4_08_timezone_name_too_long.py``.
* PF-10 / R4F-03 (Produktfrage A, E-063 Nachtrag): Ganzzahlfeld aus
  ``_FINITE_FIELDS`` größer als ``sys.float_info.max`` ergab ``OverflowError``
  aus ``math.isfinite``.
* PF-11 / R4F-04 (Produktfrage A, E-063 Nachtrag): endliche Frist über
  ~1.8e305 s wurde angenommen; ``wert * 1000`` war ``inf``. Regeltests in
  ``tests/unit/test_config_bounds.py``.

Testliste:
# [x] einzelne Variable, beliebiger Text -> ok oder ConfigurationError mit Namen
# [x] SWARMQ_SCHEDULER_TIMEZONE mit Pfadteilen bis 300 Zeichen (F4-08)
# [x] mehrere Variablen zugleich (Relationen) -> ok oder ConfigurationError mit Namen
# [x] inf/nan in jedem float-Feld -> ConfigurationError (E-063)
# [x] angenommene Fristen * 1000 endlich und höchstens MAX_DEADLINE_MS (PF-11)
"""

from __future__ import annotations

import math
from typing import Any

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from swarmq.config import SwarmQConfig
from swarmq.config_validation import MAX_DEADLINE_MS
from swarmq.exceptions import ConfigurationError

pytestmark = [pytest.mark.timeout(60)]

# (Variable, Feld, Art) aus der Konfiguration selbst, damit jede Variable
# vorkommt; gelesen wird nur die Liste, geprüft wird das Verhalten.
_OVERRIDES: tuple[tuple[str, str, str], ...] = SwarmQConfig._ENV_OVERRIDES
_FIELD_OF = {env: field for env, field, _kind in _OVERRIDES}
_FLOAT_VARS = sorted(env for env, _field, kind in _OVERRIDES if kind in {"float", "optional_float"})
# SWARMQ_LOG_DRAIN_TIMEOUT hat eine eigene Obergrenze (threading.TIMEOUT_MAX)
# und gibt keine Millisekunden an Broker oder Valkey.
_MS_VARS = [env for env in _FLOAT_VARS if env != "SWARMQ_LOG_DRAIN_TIMEOUT"]

# Umgebungswerte: kein NUL (setenv), keine einzelnen Surrogates (UTF-8); ein
# surrogateescape-Byte kommt über ``_specials`` (``\udcff``).
_env_chars = st.characters(codec="utf-8", exclude_characters="\x00")

_number_text = st.one_of(
    st.floats(allow_nan=True, allow_infinity=True).map(repr),
    st.integers(-(10**6), 10**6).map(str),
    st.integers(0, 400).map(lambda digits: "1" + "0" * digits),
)
_specials = st.sampled_from(
    [
        "inf",
        "-inf",
        "nan",
        "NaN",
        "Infinity",
        "1e400",
        "-1e400",
        "1e300",
        "-1",
        "0",
        "0.0",
        "-0.0",
        " 5 ",
        "\t7\n",
        "٣",
        "1_000",
        "0x10",
        "none",
        "NONE",
        "true",
        "yes",
        "json",
        "human",
        "INFO",
        "info",
        "a=DEBUG,b=info",
        "=DEBUG",
        "a=",
        "cpu",
        "io",
        "quorum",
        "classic",
        "strict",
        "WARN",
        "off",
        "on",
        "log",
        "shared",
        "separate",
        "at-least-once",
        "Europe/Berlin",
        "UTC",
        "Etc/UTC",
        "posixrules",
        "../etc/passwd",
        "\udcff",
        "sq",
        "App1",
        "9" * 5000,
    ]
)
# Pfadteile ab 256 Byte ergeben in ``zoneinfo`` ENAMETOOLONG (F4-08).
_zone_part = st.one_of(
    st.text(alphabet=_env_chars, min_size=1, max_size=24),
    st.integers(1, 300).map(lambda length: "a" * length),
)
_zone_text = st.lists(_zone_part, min_size=1, max_size=3).map("/".join)
_env_values = st.one_of(_specials, _number_text, st.text(alphabet=_env_chars, max_size=24), _zone_text).filter(
    bool  # ein leerer Wert gilt wie ungesetzt
)


_env = st.dictionaries(st.sampled_from([env for env, _field, _kind in _OVERRIDES]), _env_values, min_size=1, max_size=6)


def _build(env: dict[str, str]) -> SwarmQConfig:
    with pytest.MonkeyPatch.context() as mp:
        for name, _field, _kind in _OVERRIDES:
            mp.delenv(name, raising=False)
        for name, value in env.items():
            mp.setenv(name, value)
        return SwarmQConfig(broker_url="amqp://x", backend_url="redis://x")


def _names_a_cause(message: str, env: dict[str, str]) -> bool:
    return any(name in message or _FIELD_OF[name] in message for name in env)


def _build_or_configuration_error(env: dict[str, str]) -> SwarmQConfig | None:
    try:
        return _build(env)
    except ConfigurationError as exc:
        message = str(exc)
    assert _names_a_cause(message, env), f"{message} nennt keine der Variablen {sorted(env)}"
    return None


@given(env=_env)
@example(env={"SWARMQ_RESULT_TTL": "1.5"})
@example(env={"SWARMQ_METRICS_PORT": "-1"})
@example(env={"SWARMQ_CONCURRENCY": "1e3"})
@example(env={"SWARMQ_SCHEDULER_TIMEZONE": "Europe/Nowhere"})
@example(env={"SWARMQ_SCHEDULER_TIMEZONE": "a" * 256})
@example(env={"SWARMQ_SHUTDOWN_TIMEOUT": "1" + "0" * 309})
@settings(max_examples=500, deadline=None)
def test_env_gives_a_valid_config_or_a_configuration_error(env: dict[str, str]) -> None:
    _build_or_configuration_error(env)


@given(zone=_zone_text)
@example(zone="Europe/Berlin")
@example(zone="../etc/passwd")
@example(zone="a" * 256)
@settings(max_examples=500, deadline=None)
def test_scheduler_timezone_is_a_zone_or_a_configuration_error(zone: str) -> None:
    _build_or_configuration_error({"SWARMQ_SCHEDULER_TIMEZONE": zone})


@given(name=st.sampled_from(_FLOAT_VARS), raw=st.sampled_from(["inf", "-inf", "nan", "1e400", "Infinity"]))
@settings(max_examples=500, deadline=None)
def test_non_finite_float_is_a_configuration_error(name: str, raw: str) -> None:
    with pytest.raises(ConfigurationError) as caught:
        _build({name: raw})

    assert _names_a_cause(str(caught.value), {name: raw})


@given(env=st.dictionaries(st.sampled_from(_MS_VARS), _number_text, min_size=1, max_size=3))
@example(env={"SWARMQ_LOCK_WAIT_MAX_S": "1e306"})
@example(env={"SWARMQ_BACKEND_DEFER_MAX_DELAY": "1.797693134862316e+305"})
@settings(max_examples=500, deadline=None)
def test_accepted_deadlines_convert_to_milliseconds(env: dict[str, str]) -> None:
    config = _build_or_configuration_error(env)
    if config is None:
        return
    for name, raw in env.items():
        value: Any = getattr(config, _FIELD_OF[name])
        if value is not None:
            assert math.isfinite(value * 1000), f"{name}={raw!r}: {value} * 1000 ist nicht endlich"
            assert value * 1000 <= MAX_DEADLINE_MS, f"{name}={raw!r}: {value} s liegt über der Fristgrenze"
