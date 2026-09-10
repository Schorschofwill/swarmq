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
            "pkg.mod",        # kein Callable
            ":func",          # kein Modul
            "pkg mod:func",   # Leerzeichen
            "pkg.mod:",       # leeres Callable
            "pkg.mod:a:b",    # zwei Doppelpunkte
            "pkg/mod.py:f",   # Pfad statt Modul
        ],
    )
    def test_rejects_malformed_values(self, raw):
        import argparse

        from swarmq.cli._shared import _configure_hook

        with pytest.raises(argparse.ArgumentTypeError, match="--configure"):
            _configure_hook(raw)

    def test_parser_exposes_the_option(self):
        from swarmq.cli.parser import _build_parser

        args = _build_parser().parse_args(
            ["worker", "--module", "app.tasks", "--configure", "app.hooks:setup"]
        )
        assert args.configure == "app.hooks:setup"

    def test_default_is_none(self):
        from swarmq.cli.parser import _build_parser

        args = _build_parser().parse_args(["worker", "--module", "app.tasks"])
        assert args.configure is None
