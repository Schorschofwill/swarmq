"""Wertung des Hauptbuchs im Garantie-Vergleich (Plan 2026-10-24-1118, KTD14, R21).

``bilanz.py`` wertet die Zeilen eines Hauptbuchs rein funktional aus: keine
Datei, keine Dienste. Das Modul liegt unter ``benchmarks/comparison`` und
wird wie dort über das Verzeichnis selbst importiert.

Test-Liste:
- [x] ein bestätigter Publish ohne Ausführung ist ein Verlust
- [x] ein bestätigter Publish mit einer Ausführung ist weder Verlust noch Duplikat
- [x] zwei Ausführungen sind ein Duplikat
- [x] ein unbestimmter Publish ohne Ausführung ist kein Verlust und steht in eigener Spalte (E-042)
- [x] ein abgelehnter Publish ist kein Verlust
- [x] ein Duplikat nach einem Valkey-Neustart ohne Persistenz steht in eigener Spalte (E-077)
- [x] ein Duplikat, dessen Ausführungen beide vor dem Datenverlust liegen, bleibt ein Duplikat
- [x] ein leeres Hauptbuch ergibt einen Fehler statt „0 Verluste“
- [x] eine Bilanz ohne bestätigten Publish oder ohne Ausführung gilt als leerer Lauf
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest

VERGLEICH_DIR = Path(__file__).resolve().parents[2] / "benchmarks" / "comparison"


@pytest.fixture
def bilanz(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    """``stoerungen.bilanz`` wie beim Lauf aus ``benchmarks/comparison`` importiert."""
    monkeypatch.syspath_prepend(str(VERGLEICH_DIR))
    yield importlib.import_module("stoerungen.bilanz")
    for name in [n for n in sys.modules if n.split(".")[0] == "stoerungen"]:
        del sys.modules[name]


def test_a_confirmed_publish_without_execution_is_a_loss(bilanz: ModuleType) -> None:
    ergebnis = bilanz.bilanz([bilanz.Publish("a", bilanz.BESTAETIGT)], [])

    assert ergebnis.bestaetigt == 1
    assert ergebnis.verluste == 1


def test_a_confirmed_publish_with_one_execution_is_neither_loss_nor_duplicate(bilanz: ModuleType) -> None:
    ergebnis = bilanz.bilanz([bilanz.Publish("a", bilanz.BESTAETIGT)], [bilanz.Ausfuehrung("a")])

    assert ergebnis.verluste == 0
    assert ergebnis.duplikate == 0
    assert ergebnis.ausgefuehrt == 1


def test_two_executions_are_a_duplicate(bilanz: ModuleType) -> None:
    publishes = [bilanz.Publish("a", bilanz.BESTAETIGT), bilanz.Publish("b", bilanz.BESTAETIGT)]
    ausfuehrungen = [bilanz.Ausfuehrung("a"), bilanz.Ausfuehrung("a"), bilanz.Ausfuehrung("b")]

    ergebnis = bilanz.bilanz(publishes, ausfuehrungen)

    assert ergebnis.duplikate == 1
    assert ergebnis.ausgefuehrt == 2
    assert ergebnis.verluste == 0


@pytest.mark.entscheidung("E-042")
def test_an_indeterminate_publish_without_execution_is_no_loss_and_has_its_own_column(bilanz: ModuleType) -> None:
    publishes = [bilanz.Publish("a", bilanz.BESTAETIGT), bilanz.Publish("b", bilanz.UNBESTIMMT)]

    ergebnis = bilanz.bilanz(publishes, [bilanz.Ausfuehrung("a")])

    assert ergebnis.verluste == 0
    assert ergebnis.unbestimmt == 1
    assert ergebnis.bestaetigt == 1


def test_a_rejected_publish_is_no_loss(bilanz: ModuleType) -> None:
    publishes = [bilanz.Publish("a", bilanz.BESTAETIGT), bilanz.Publish("b", bilanz.ABGELEHNT)]

    ergebnis = bilanz.bilanz(publishes, [bilanz.Ausfuehrung("a")])

    assert ergebnis.verluste == 0
    assert ergebnis.abgelehnt == 1


@pytest.mark.entscheidung("E-077")
def test_a_duplicate_after_a_valkey_restart_without_persistence_has_its_own_column(bilanz: ModuleType) -> None:
    publishes = [bilanz.Publish("a", bilanz.BESTAETIGT)]
    ausfuehrungen = [bilanz.Ausfuehrung("a", zeit=10.0), bilanz.Ausfuehrung("a", zeit=30.0)]

    ergebnis = bilanz.bilanz(publishes, ausfuehrungen, valkey_datenverlust=20.0)

    assert ergebnis.duplikate_e077 == 1
    assert ergebnis.duplikate == 0


def test_a_duplicate_whose_executions_both_precede_the_data_loss_stays_a_duplicate(bilanz: ModuleType) -> None:
    publishes = [bilanz.Publish("a", bilanz.BESTAETIGT)]
    ausfuehrungen = [bilanz.Ausfuehrung("a", zeit=10.0), bilanz.Ausfuehrung("a", zeit=15.0)]

    ergebnis = bilanz.bilanz(publishes, ausfuehrungen, valkey_datenverlust=20.0)

    assert ergebnis.duplikate == 1
    assert ergebnis.duplikate_e077 == 0


def test_an_empty_ledger_is_an_error_instead_of_zero_losses(bilanz: ModuleType) -> None:
    with pytest.raises(bilanz.LeeresHauptbuch):
        bilanz.bilanz([], [])


def test_a_balance_without_confirmed_publish_or_without_execution_is_an_empty_run(bilanz: ModuleType) -> None:
    ohne_bestaetigung = bilanz.bilanz([bilanz.Publish("a", bilanz.ABGELEHNT)], [bilanz.Ausfuehrung("a")])
    ohne_ausfuehrung = bilanz.bilanz([bilanz.Publish("a", bilanz.BESTAETIGT)], [])
    gemessen = bilanz.bilanz([bilanz.Publish("a", bilanz.BESTAETIGT)], [bilanz.Ausfuehrung("a")])

    assert ohne_bestaetigung.leer
    assert ohne_ausfuehrung.leer
    assert not gemessen.leer
