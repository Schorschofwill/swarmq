"""Logging-Einstellungen und ihre Aufloesung (Plan 2026-10-06, U5, KTD7/KTD8).

``LoggingSettings`` buendelt alle einstellbaren Teile des Loggings in einem
unveraenderlichen, pickle-sicheren Objekt. Der Supervisor loest es einmal auf
und gibt es aufgeloest an seine Kinder weiter.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from swarmq.exceptions import ConfigurationError

#: Gueltige Level fuer alle Logging-Einstellungen; CRITICAL ueberall (KTD8).
VALID_LOG_LEVELS: tuple[str, ...] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
VALID_LOG_FORMATS: tuple[str, ...] = ("human", "json")


@dataclass(frozen=True)
class LoggingSettings:
    """Aufgeloeste Logging-Einstellungen eines Prozesses (R13)."""

    level: str = "INFO"
    # Kam das Level aus CLI, Env oder SwarmQConfig? Nur dann wirkt es nach
    # einer dictConfig zusaetzlich per ``setLevel`` (KTD8).
    level_explicit: bool = False
    format: str = "human"
    log_config: str | None = None
    # Die geprueft geparste dictConfig als JSON-String (KTD9). Ein String
    # bleibt pickle-sicher; gefuellt wird das Feld erst beim Laden der Datei.
    log_config_json: str | None = None
    configure: bool = True
    logger_levels: tuple[tuple[str, str], ...] = ()
    queue_size: int = 10_000
    lifecycle_level: str = "DEBUG"


def fallback_log_format(cli: object | None, env: Mapping[str, str]) -> str:
    """Format fuer die Fehlerzeile, wenn :func:`resolve_logging_settings`
    scheitert (U7, R14): CLI, dann Env, sonst ``human``."""
    log_format = getattr(cli, "log_format", None) or _env_format(env)
    return log_format if log_format in VALID_LOG_FORMATS else "human"


def resolve_logging_settings(
    cli: object | None,
    env: Mapping[str, str],
    config: Mapping[str, object] | None,
) -> LoggingSettings:
    """Einstellungen mit der Prioritaet CLI > Env > SwarmQConfig > Default.

    ``cli`` ist eine argparse-Namespace (fehlende oder ``None``-Attribute
    gelten als "nicht angegeben"), ``config`` die ausdruecklich uebergebenen
    ``SwarmQConfig``-Argumente. Ungueltige Werte enden in einem
    ``ConfigurationError``, der die Quelle nennt.
    """
    sources = _Sources(cli, env, config or {})
    log_format = sources.pick(
        "log_format", "--log-format",
        "SWARMQ_LOG_FORMAT" if env.get("SWARMQ_LOG_FORMAT") else "SWARMQ_LOG_JSON",
        "log_json", default="human",
        env_value=_env_format(env), config_value=_config_format(config),
    )
    level = sources.pick("log_level", "--log-level", "SWARMQ_LOG_LEVEL", "log_level", "INFO")
    queue_size = sources.pick(
        "log_queue_size", "--log-queue-size", "SWARMQ_LOG_QUEUE_SIZE", "log_queue_size", 10_000,
    )
    lifecycle_level = sources.pick(
        "log_lifecycle_level", "--log-lifecycle-level",
        "SWARMQ_LOG_LIFECYCLE_LEVEL", "log_lifecycle_level", "DEBUG",
    )
    log_config = sources.pick(
        "log_config", "--log-config", "SWARMQ_LOG_CONFIG", "log_config", None,
    )
    raw_configure = env.get("SWARMQ_CONFIGURE_LOGGING")
    configure = sources.pick(
        "configure_logging", "--no-configure-logging", "SWARMQ_CONFIGURE_LOGGING",
        "configure_logging", default=True,
        env_value=_parse_bool(raw_configure) if raw_configure else None,
    )
    _check_combinations(log_config, log_format, configure)
    return LoggingSettings(
        level=_level(level),
        level_explicit=level.explicit,
        format=_choice(log_format, VALID_LOG_FORMATS),
        log_config=None if log_config.value is None else str(log_config.value),
        configure=bool(configure.value),
        logger_levels=_logger_levels(cli, env, config),
        queue_size=_positive_int(queue_size),
        lifecycle_level=_level(lifecycle_level),
    )


@dataclass(frozen=True)
class _Picked:
    """Ein aufgeloester Wert und die Quelle, aus der er stammt."""

    value: object
    source: str

    @property
    def explicit(self) -> bool:
        return self.source != "default"


_UNSET = object()


@dataclass(frozen=True)
class _Sources:
    cli: object | None
    env: Mapping[str, str]
    config: Mapping[str, object]

    def pick(
        self,
        cli_attr: str,
        cli_flag: str,
        env_name: str | None,
        config_field: str | None,
        default: object,
        *,
        env_value: object = _UNSET,
        config_value: object = _UNSET,
    ) -> _Picked:
        """Der erste angegebene Wert in der Reihenfolge CLI, Env, Config."""
        if env_value is _UNSET:
            env_value = self.env.get(env_name or "") or None
        if config_value is _UNSET:
            config_value = self.config.get(config_field or "")
        candidates = (
            (getattr(self.cli, cli_attr, None), cli_flag),
            (env_value, env_name),
            (config_value, f"SwarmQConfig.{config_field}"),
        )
        for value, source in candidates:
            if value is not None:
                return _Picked(value, str(source))
        return _Picked(default, "default")


def _check_combinations(log_config: _Picked, log_format: _Picked, configure: _Picked) -> None:
    """Widerspruechliche Kombinationen sind Konfigurationsfehler (KTD8)."""
    if log_config.value is None:
        return
    if log_format.explicit:
        # Mit einer eigenen dictConfig bestimmt die Datei das Format; ein
        # zusaetzlich gesetztes Format waere stillschweigend wirkungslos.
        msg = (
            f"{log_config.source} cannot be combined with {log_format.source}: "
            "the logging config file defines the output format"
        )
        raise ConfigurationError(msg)
    if not configure.value:
        msg = (
            f"{configure.source} cannot be combined with {log_config.source}: "
            "a logging config file needs logging to be configured"
        )
        raise ConfigurationError(msg)


def _positive_int(picked: _Picked) -> int:
    try:
        number = int(str(picked.value).strip())
    except ValueError:
        number = 0
    if number <= 0:
        msg = f"{picked.source} must be a positive integer, got {picked.value!r}"
        raise ConfigurationError(msg)
    return number


def _logger_levels(
    cli: object | None, env: Mapping[str, str], config: Mapping[str, object] | None,
) -> tuple[tuple[str, str], ...]:
    # Eigene Aufloesung: Die drei Quellen liefern verschiedene Formen
    # (Liste von ``NAME=LEVEL``, CSV, Paare).
    if entries := getattr(cli, "logger_levels", None):
        return parse_logger_levels(entries, "--logger-level")
    if raw := env.get("SWARMQ_LOGGER_LEVELS"):
        return parse_logger_levels(split_csv(raw), "SWARMQ_LOGGER_LEVELS")
    pairs = None if config is None else config.get("logger_levels")
    if pairs:
        return validate_logger_levels(pairs, "SwarmQConfig.logger_levels")
    return ()


def split_csv(raw: str) -> list[str]:
    """Kommagetrennte Eintraege ohne leere Stuecke (wie ``SWARMQ_QUEUES``)."""
    return [item.strip() for item in raw.split(",") if item.strip()]


def validate_logger_levels(pairs: object, source: str) -> tuple[tuple[str, str], ...]:
    """``(NAME, LEVEL)``-Paare pruefen und das Level normalisieren."""
    entries: list[str] = []
    for pair in pairs if isinstance(pairs, (tuple, list)) else [pairs]:
        if not (isinstance(pair, (tuple, list)) and len(pair) == 2):
            msg = f"{source} expects (NAME, LEVEL) pairs, got {pair!r}"
            raise ConfigurationError(msg)
        entries.append(f"{pair[0]}={pair[1]}")
    return parse_logger_levels(entries, source)


def parse_logger_levels(
    entries: Iterable[str], source: str,
) -> tuple[tuple[str, str], ...]:
    """``NAME=LEVEL``-Eintraege als pickle-sicheres Tupel von Paaren."""
    pairs: list[tuple[str, str]] = []
    for entry in entries:
        name, sep, level = entry.partition("=")
        if not sep or not name.strip():
            msg = f"{source} expects NAME=LEVEL, got {entry!r}"
            raise ConfigurationError(msg)
        pairs.append((name.strip(), check_log_level(level, f"{source} entry {entry!r}")))
    return tuple(pairs)


def _level(picked: _Picked) -> str:
    return check_log_level(picked.value, picked.source)


def check_log_level(value: object, source: str) -> str:
    """Ein Level pruefen und in Grossbuchstaben zurueckgeben."""
    level = str(value).strip().upper()
    if level not in VALID_LOG_LEVELS:
        msg = f"{source} must be one of {list(VALID_LOG_LEVELS)}, got {value!r}"
        raise ConfigurationError(msg)
    return level


def _choice(picked: _Picked, choices: tuple[str, ...]) -> str:
    if picked.value not in choices:
        msg = f"{picked.source} must be one of {list(choices)}, got {picked.value!r}"
        raise ConfigurationError(msg)
    return str(picked.value)


def _env_format(env: Mapping[str, str]) -> str | None:
    """``SWARMQ_LOG_FORMAT``, sonst der Alias ``SWARMQ_LOG_JSON`` (KTD8)."""
    if raw := env.get("SWARMQ_LOG_FORMAT"):
        return raw.strip().lower()
    if raw := env.get("SWARMQ_LOG_JSON"):
        return "json" if _parse_bool(raw) else "human"
    return None


def _config_format(config: Mapping[str, object] | None) -> str | None:
    if config is None or config.get("log_json") is None:
        return None
    return "json" if config["log_json"] else "human"


def _parse_bool(raw: str) -> bool:
    # Dieselbe Lesart wie die ``bool``-Overrides in ``SwarmQConfig``.
    return raw.strip().lower() in ("true", "1", "yes")
