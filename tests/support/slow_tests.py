"""Laufzeitbericht langsamer Unit-Tests (Plan 2026-10-13-0902, U5, KTD4).

Meldet jede Setup- und Call-Phase eines Tests über ``THRESHOLD_S`` in der Zusammenfassung
und in ``slow-unit-tests.json`` im Arbeitsverzeichnis des Laufs. Langsame
Tests bleiben, wie sie sind (lieber langsam und genau); der Bericht ändert
den Exit-Code nicht. ``tests/unit/conftest.py`` holt ``pytest_configure``
in seinen Namensraum.

Unter Coverage-Messung schweigt der Bericht: Die Zeiten sind dort
aufgebläht. Erkannt an ``COVERAGE_RUN``, das ``coverage run`` für sich und
alle Kindprozesse setzt.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

THRESHOLD_S = 0.1
REPORT_FILE = "slow-unit-tests.json"


class SlowTestReport:
    """Sammelt die Phasen über ``THRESHOLD_S`` einer Sitzung."""

    def __init__(self) -> None:
        self.slow: list[dict[str, object]] = []

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when in {"setup", "call"} and report.duration > THRESHOLD_S:
            self.slow.append({"nodeid": report.nodeid, "phase": report.when, "duration_s": report.duration})

    def pytest_terminal_summary(self, terminalreporter: pytest.TerminalReporter) -> None:
        Path(REPORT_FILE).write_text(json.dumps(self.slow, indent=2), encoding="utf-8")
        if not self.slow:
            return
        terminalreporter.section("Unit-Tests über 100 ms")
        for entry in self.slow:
            terminalreporter.write_line(f"{entry['duration_s'] * 1000:6.0f} ms  {entry['phase']:5}  {entry['nodeid']}")


def pytest_configure(config: pytest.Config) -> None:
    if os.environ.get("COVERAGE_RUN") != "true":
        config.pluginmanager.register(SlowTestReport(), "swarmq-slow-test-report")
