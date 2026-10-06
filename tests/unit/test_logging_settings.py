"""Einstellungsmodell und Prioritaet fuers Logging (U5, KTD7/KTD8).

Test-Liste:
- [x] Ohne Angaben: INFO, human, eingebaut, Queue 10 000, Lebenszyklus DEBUG
- [x] SWARMQ_LOG_FORMAT=json ohne CLI ergibt json; mit --log-format human gewinnt human (AE5)
- [x] SWARMQ_LOG_JSON=true ohne SWARMQ_LOG_FORMAT ergibt json; beide gesetzt: FORMAT gewinnt
- [x] Prioritaet CLI > Env > SwarmQConfig-Argument > Default fuer jedes Feld
- [x] --logger-level (wiederholt) und SWARMQ_LOGGER_LEVELS ergeben dieselben Paare
- [x] Logger-Level ohne ``=`` und mit unbekanntem Level: ConfigurationError mit praeziser Meldung
- [x] --log-config + ausdrueckliches Format, --no-configure-logging + --log-config: ConfigurationError
- [x] Queue-Groesse <= 0 oder nicht numerisch: ConfigurationError
- [x] Ungueltiges Level/Format/Lebenszyklus-Level: ConfigurationError
- [x] CRITICAL wird akzeptiert
- [x] SWARMQ_CONFIGURE_LOGGING=false schaltet die Einrichtung ab
- [x] Fehlende Namespace-Attribute zaehlen als "nicht angegeben"
- [x] LoggingSettings ist unveraenderlich und pickle-sicher
- [x] ``level_explicit``: wahr bei Level aus CLI, Env oder SwarmQConfig, sonst falsch (U7, KTD8)
- [x] fallback_log_format: Format fuer die Fehlerzeile, wenn die Aufloesung scheitert --
      CLI, dann Env, ungueltig oder fehlend ``human`` (U7, R14)

Plan 2026-10-06-1811, U4 (R14, KTD11) -- ``log_drain_timeout``:
- [x] Prioritaet CLI > Env > SwarmQConfig > Default 5.0; Bruchteile erlaubt
- [x] Wert <= 0, nicht numerisch oder nicht endlich: ConfigurationError mit Quelle
- [x] Wert > ``threading.TIMEOUT_MAX`` (etwa ``1e10``): ConfigurationError --
      ``Thread.join`` wuerfe sonst ``OverflowError`` (Review A1)
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from swarmq.exceptions import ConfigurationError
from swarmq.logging_settings import (
    LoggingSettings,
    fallback_log_format,
    resolve_logging_settings,
)


def _cli(**kwargs: object) -> SimpleNamespace:
    return SimpleNamespace(**kwargs)


class TestDefaults:
    def test_without_any_input_the_builtin_defaults_apply(self) -> None:
        settings = resolve_logging_settings(_cli(), env={}, config=None)

        assert settings == LoggingSettings(
            level="INFO",
            format="human",
            log_config=None,
            log_config_json=None,
            configure=True,
            logger_levels=(),
            queue_size=10_000,
            lifecycle_level="DEBUG",
        )


class TestFormat:
    def test_env_log_format_json_applies_without_cli_option(self) -> None:
        settings = resolve_logging_settings(
            _cli(log_format=None), env={"SWARMQ_LOG_FORMAT": "json"}, config=None,
        )

        assert settings.format == "json"

    def test_cli_log_format_beats_env_log_format(self) -> None:
        """AE5: ``SWARMQ_LOG_FORMAT=json`` plus ``--log-format human`` ergibt human."""
        settings = resolve_logging_settings(
            _cli(log_format="human"), env={"SWARMQ_LOG_FORMAT": "json"}, config=None,
        )

        assert settings.format == "human"

    def test_env_log_json_true_is_an_alias_for_json(self) -> None:
        settings = resolve_logging_settings(
            _cli(), env={"SWARMQ_LOG_JSON": "true"}, config=None,
        )

        assert settings.format == "json"

    def test_env_log_format_wins_over_env_log_json(self) -> None:
        settings = resolve_logging_settings(
            _cli(),
            env={"SWARMQ_LOG_FORMAT": "human", "SWARMQ_LOG_JSON": "true"},
            config=None,
        )

        assert settings.format == "human"

    def test_config_log_json_is_used_below_env(self) -> None:
        assert resolve_logging_settings(
            _cli(), env={}, config={"log_json": True},
        ).format == "json"
        assert resolve_logging_settings(
            _cli(), env={"SWARMQ_LOG_FORMAT": "human"}, config={"log_json": True},
        ).format == "human"


class TestPriority:
    def test_level_follows_cli_then_env_then_config(self) -> None:
        env = {"SWARMQ_LOG_LEVEL": "WARNING"}
        config = {"log_level": "ERROR"}

        assert resolve_logging_settings(_cli(log_level="DEBUG"), env, config).level == "DEBUG"
        assert resolve_logging_settings(_cli(log_level=None), env, config).level == "WARNING"
        assert resolve_logging_settings(_cli(), {}, config).level == "ERROR"

    def test_level_is_explicit_only_when_a_source_gave_it(self) -> None:
        # U7 (KTD8): Nach einer dictConfig wirkt nur ein ausdrueckliches Level.
        env = {"SWARMQ_LOG_LEVEL": "WARNING"}

        assert resolve_logging_settings(_cli(log_level="DEBUG"), {}, None).level_explicit
        assert resolve_logging_settings(_cli(), env, None).level_explicit
        assert resolve_logging_settings(_cli(), {}, {"log_level": "ERROR"}).level_explicit
        assert not resolve_logging_settings(_cli(), {}, None).level_explicit

    def test_queue_size_follows_cli_then_env_then_config(self) -> None:
        env = {"SWARMQ_LOG_QUEUE_SIZE": "200"}
        config = {"log_queue_size": 300}

        assert resolve_logging_settings(_cli(log_queue_size="100"), env, config).queue_size == 100
        assert resolve_logging_settings(_cli(), env, config).queue_size == 200
        assert resolve_logging_settings(_cli(), {}, config).queue_size == 300

    def test_drain_timeout_follows_cli_then_env_then_config_then_default(self) -> None:
        env = {"SWARMQ_LOG_DRAIN_TIMEOUT": "2.5"}
        config = {"log_drain_timeout": 3}

        assert resolve_logging_settings(
            _cli(log_drain_timeout="0.25"), env, config,
        ).drain_timeout == 0.25
        assert resolve_logging_settings(_cli(), env, config).drain_timeout == 2.5
        assert resolve_logging_settings(_cli(), {}, config).drain_timeout == 3.0
        assert resolve_logging_settings(_cli(), {}, None).drain_timeout == 5.0

    def test_lifecycle_level_follows_cli_then_env_then_config(self) -> None:
        env = {"SWARMQ_LOG_LIFECYCLE_LEVEL": "WARNING"}
        config = {"log_lifecycle_level": "ERROR"}

        assert resolve_logging_settings(
            _cli(log_lifecycle_level="INFO"), env, config,
        ).lifecycle_level == "INFO"
        assert resolve_logging_settings(_cli(), env, config).lifecycle_level == "WARNING"
        assert resolve_logging_settings(_cli(), {}, config).lifecycle_level == "ERROR"

    def test_log_config_follows_cli_then_env_then_config(self) -> None:
        env = {"SWARMQ_LOG_CONFIG": "env.json"}
        config = {"log_config": "config.json"}

        assert resolve_logging_settings(
            _cli(log_config="cli.json"), env, config,
        ).log_config == "cli.json"
        assert resolve_logging_settings(_cli(), env, config).log_config == "env.json"
        assert resolve_logging_settings(_cli(), {}, config).log_config == "config.json"

    def test_configure_switch_follows_cli_then_env_then_config(self) -> None:
        assert resolve_logging_settings(
            _cli(configure_logging=False), {"SWARMQ_CONFIGURE_LOGGING": "true"}, None,
        ).configure is False
        assert resolve_logging_settings(
            _cli(configure_logging=None),
            {"SWARMQ_CONFIGURE_LOGGING": "false"},
            {"configure_logging": True},
        ).configure is False
        assert resolve_logging_settings(
            _cli(), {}, {"configure_logging": False},
        ).configure is False
        assert resolve_logging_settings(
            _cli(), {"SWARMQ_CONFIGURE_LOGGING": "true"}, {"configure_logging": False},
        ).configure is True


class TestLoggerLevels:
    def test_repeated_cli_option_yields_pairs(self) -> None:
        settings = resolve_logging_settings(
            _cli(logger_levels=["aio_pika=WARNING", "valkey=ERROR"]), {}, None,
        )

        assert settings.logger_levels == (("aio_pika", "WARNING"), ("valkey", "ERROR"))

    def test_env_form_yields_the_same_pairs_as_the_cli_form(self) -> None:
        from_env = resolve_logging_settings(
            _cli(), {"SWARMQ_LOGGER_LEVELS": "aio_pika=WARNING, valkey=ERROR"}, None,
        )
        from_cli = resolve_logging_settings(
            _cli(logger_levels=["aio_pika=WARNING", "valkey=ERROR"]), {}, None,
        )

        assert from_env.logger_levels == from_cli.logger_levels

    def test_config_pairs_apply_below_env(self) -> None:
        config = {"logger_levels": (("aiormq", "error"),)}

        assert resolve_logging_settings(_cli(), {}, config).logger_levels == (
            ("aiormq", "ERROR"),
        )
        assert resolve_logging_settings(
            _cli(), {"SWARMQ_LOGGER_LEVELS": "valkey=INFO"}, config,
        ).logger_levels == (("valkey", "INFO"),)

    def test_entry_without_equals_sign_is_a_configuration_error(self) -> None:
        with pytest.raises(
            ConfigurationError, match=r"--logger-level expects NAME=LEVEL, got 'aio_pika'",
        ):
            resolve_logging_settings(_cli(logger_levels=["aio_pika"]), {}, None)

    def test_unknown_level_in_entry_is_a_configuration_error(self) -> None:
        with pytest.raises(
            ConfigurationError, match=r"SWARMQ_LOGGER_LEVELS.*aio_pika.*'LOUD'",
        ):
            resolve_logging_settings(
                _cli(), {"SWARMQ_LOGGER_LEVELS": "aio_pika=LOUD"}, None,
            )

    def test_empty_logger_name_is_a_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError, match=r"--logger-level.*'=DEBUG'"):
            resolve_logging_settings(_cli(logger_levels=["=DEBUG"]), {}, None)


class TestValidation:
    def test_unknown_env_level_is_a_configuration_error_naming_the_source(self) -> None:
        with pytest.raises(ConfigurationError, match=r"SWARMQ_LOG_LEVEL.*'LOUD'"):
            resolve_logging_settings(_cli(), {"SWARMQ_LOG_LEVEL": "LOUD"}, None)

    def test_critical_is_accepted(self) -> None:
        settings = resolve_logging_settings(_cli(log_level="CRITICAL"), {}, None)

        assert settings.level == "CRITICAL"

    def test_unknown_env_format_is_a_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError, match=r"SWARMQ_LOG_FORMAT.*'xml'"):
            resolve_logging_settings(_cli(), {"SWARMQ_LOG_FORMAT": "xml"}, None)

    @pytest.mark.parametrize("raw", ["0", "-5", "abc", "1.5"])
    def test_non_positive_or_non_numeric_queue_size_is_a_configuration_error(
        self, raw: str,
    ) -> None:
        with pytest.raises(ConfigurationError, match=rf"SWARMQ_LOG_QUEUE_SIZE.*{raw!r}"):
            resolve_logging_settings(_cli(), {"SWARMQ_LOG_QUEUE_SIZE": raw}, None)

    @pytest.mark.parametrize("raw", ["0", "-1", "abc", "nan", "inf", "1e10"])
    def test_non_positive_non_numeric_or_infinite_drain_timeout_is_a_configuration_error(
        self, raw: str,
    ) -> None:
        with pytest.raises(ConfigurationError, match=rf"SWARMQ_LOG_DRAIN_TIMEOUT.*{raw!r}"):
            resolve_logging_settings(_cli(), {"SWARMQ_LOG_DRAIN_TIMEOUT": raw}, None)


class TestInvalidCombinations:
    def test_log_config_with_explicit_log_format_is_a_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError, match=r"--log-config.*--log-format"):
            resolve_logging_settings(
                _cli(log_config="x.json", log_format="json"), {}, None,
            )

    def test_no_configure_with_log_config_is_a_configuration_error(self) -> None:
        with pytest.raises(
            ConfigurationError, match=r"--no-configure-logging.*--log-config",
        ):
            resolve_logging_settings(
                _cli(configure_logging=False, log_config="x.json"), {}, None,
            )


class TestModel:
    def test_missing_namespace_counts_as_nothing_given(self) -> None:
        assert resolve_logging_settings(None, {}, None) == LoggingSettings()

    def test_settings_are_frozen_and_survive_pickle(self) -> None:
        import dataclasses
        import pickle

        settings = LoggingSettings(
            level="WARNING",
            log_config="/etc/swarmq/logging.json",
            log_config_json='{"version": 1}',
            logger_levels=(("aio_pika", "WARNING"),),
        )

        assert pickle.loads(pickle.dumps(settings)) == settings
        with pytest.raises(dataclasses.FrozenInstanceError):
            settings.level = "DEBUG"  # type: ignore[misc]


class TestFallbackLogFormat:
    """U7 (R14): Scheitert die Aufloesung, erscheint ``worker_config_error``
    trotzdem im gewaehlten Format, soweit es sich bestimmen laesst."""

    def test_cli_then_env_then_human(self) -> None:
        env = {"SWARMQ_LOG_FORMAT": "json"}

        assert fallback_log_format(_cli(log_format="human"), env) == "human"
        assert fallback_log_format(_cli(), env) == "json"
        assert fallback_log_format(_cli(), {"SWARMQ_LOG_JSON": "true"}) == "json"
        assert fallback_log_format(None, {}) == "human"

    def test_an_invalid_format_falls_back_to_human(self) -> None:
        assert fallback_log_format(_cli(), {"SWARMQ_LOG_FORMAT": "xml"}) == "human"
