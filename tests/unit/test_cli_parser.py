"""CLI-Parser-Optionen (U5, Plan 2026-09-08-001)."""

from __future__ import annotations

import pytest


class TestConfigureHookOption:
    """U5 (Plan 2026-09-08-001, R9/R11/KTD5): ``--configure MODULE:CALLABLE``.

    Wer Middleware oder Signal-Handler am CLI-Worker haben wollte, musste
    bisher ein eigenes Worker-Entry schreiben -- genau das hat die
    Lasttest-Suite getan. Der Hook ist ausdruecklich (im ``--help`` sichtbar)
    statt einer Namenskonvention im Task-Modul.
    """

    def test_accepts_module_and_callable(self):
        from swarmq.cli._shared import _configure_hook

        assert _configure_hook("pkg.mod:func") == "pkg.mod:func"

    @pytest.mark.parametrize(
        "raw",
        [
            "pkg.mod",  # kein Callable
            ":func",  # kein Modul
            "pkg mod:func",  # Leerzeichen
            "pkg.mod:",  # leeres Callable
            "pkg.mod:a:b",  # zwei Doppelpunkte
            "pkg/mod.py:f",  # Pfad statt Modul
        ],
    )
    def test_rejects_malformed_values(self, raw):
        import argparse

        from swarmq.cli._shared import _configure_hook

        with pytest.raises(argparse.ArgumentTypeError, match="--configure"):
            _configure_hook(raw)

    def test_parser_exposes_the_option(self):
        from swarmq.cli.parser import _build_parser

        args = _build_parser().parse_args(["worker", "--module", "app.tasks", "--configure", "app.hooks:setup"])
        assert args.configure == "app.hooks:setup"

    def test_default_is_none(self):
        from swarmq.cli.parser import _build_parser

        args = _build_parser().parse_args(["worker", "--module", "app.tasks"])
        assert args.configure is None


class TestLoggingOptions:
    """Plan 2026-10-06 U5 (R13/KTD7/KTD8): Logging-Optionen des Workers.

    Test-Liste:
    - [x] Ohne Flags sind alle Logging-Werte ``None`` (Prioritaet KTD7)
    - [x] Alle Optionen werden gelesen, ``--logger-level`` wiederholbar
    - [x] CRITICAL fuer ``--log-level`` und ``--log-lifecycle-level``
    - [x] ``--log-drain-timeout`` (Plan 2026-10-06-1811, U4) in beiden Tests oben
    """

    @staticmethod
    def _parse(*extra: str):
        from swarmq.cli.parser import _build_parser

        return _build_parser().parse_args(["worker", "--module", "app.tasks", *extra])

    def test_all_logging_values_default_to_none(self):
        args = self._parse()
        assert (
            args.log_level,
            args.log_format,
            args.log_config,
            args.configure_logging,
            args.logger_levels,
            args.log_queue_size,
            args.log_lifecycle_level,
            args.log_drain_timeout,
        ) == (None,) * 8

    def test_all_options_are_read_and_logger_level_repeats(self):
        args = self._parse(
            "--log-config",
            "logging.json",
            "--no-configure-logging",
            "--logger-level",
            "aio_pika=WARNING",
            "--logger-level",
            "valkey=ERROR",
            "--log-queue-size",
            "500",
            "--log-lifecycle-level",
            "INFO",
            "--log-drain-timeout",
            "0.5",
        )
        assert args.log_config == "logging.json"
        assert args.configure_logging is False
        assert args.logger_levels == ["aio_pika=WARNING", "valkey=ERROR"]
        assert args.log_queue_size == "500"
        assert args.log_lifecycle_level == "INFO"
        assert args.log_drain_timeout == "0.5"

    def test_repeated_logger_level_resolves_like_the_env_form(self):
        from swarmq.logging_settings import resolve_logging_settings

        args = self._parse("--logger-level", "aio_pika=WARNING", "--logger-level", "valkey=ERROR")
        from_cli = resolve_logging_settings(args, {}, None)
        from_env = resolve_logging_settings(
            None,
            {"SWARMQ_LOGGER_LEVELS": "aio_pika=WARNING,valkey=ERROR"},
            None,
        )
        assert (
            from_cli.logger_levels
            == from_env.logger_levels
            == (
                ("aio_pika", "WARNING"),
                ("valkey", "ERROR"),
            )
        )

    def test_critical_is_accepted(self):
        args = self._parse("--log-level", "CRITICAL", "--log-lifecycle-level", "CRITICAL")
        assert (args.log_level, args.log_lifecycle_level) == ("CRITICAL", "CRITICAL")
