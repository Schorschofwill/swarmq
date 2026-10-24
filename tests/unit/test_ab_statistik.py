"""Statistik des A/B-Messwerkzeugs (Plan 2026-10-24-1118, U7, KTD12, R5, AE2).

``scripts/ab_statistik.py`` enthält nur reine Funktionen: zweiseitiger
Mann-Whitney-U-Test, Hodges-Lehmann-Verschiebung und das Urteil. Gesichert
ist ein Unterschied bei p < 0,05; ein Gewinn verlangt eine Verschiebung von
mindestens 5 % vom Median der A-Seite in die bessere Richtung und ein Profil
je Seite.

Referenzwerte vorab mit scipy 1.18.1 und numpy 2.5.4 berechnet, nicht als
Abhängigkeit (``uv run --no-project --with scipy --with numpy``):
``scipy.stats.mannwhitneyu(a, b, alternative="two-sided",
method="asymptotic", use_continuity=True)`` für U (der A-Seite) und p,
``numpy.median(numpy.subtract.outer(b, a))`` für Hodges-Lehmann.

Test-Liste:
- [x] Mann-Whitney U und p ohne Bindungen stimmen mit scipy überein
- [x] Mann-Whitney U und p mit Bindungen stimmen mit scipy überein
- [x] Mann-Whitney U und p für überlappende, ungleich große Stichproben
- [x] Hodges-Lehmann-Verschiebung (B minus A) für drei Datensätze
- [x] gesicherte 8 % mehr Durchsatz mit Profilen: Gewinn
- [x] gesicherte 3 % mehr Durchsatz: kein Gewinn (AE2)
- [x] gesicherte 8 % ohne Profile: nicht belegt
- [x] kein gesicherter Unterschied: kein Gewinn
- [x] gesichert schlechter: Verschlechterung, auch ohne Profile
- [x] Richtung kleiner (Latenz): 8 % weniger ist Gewinn, 8 % mehr Verschlechterung
- [x] identische Werte auf beiden Seiten: p = 1, kein Gewinn
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.timeout(5)


def _load() -> ModuleType:
    path = Path(__file__).resolve().parents[2] / "scripts" / "ab_statistik.py"
    spec = importlib.util.spec_from_file_location("ab_statistik", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclass braucht das Modul in sys.modules.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


stat = _load()

_OHNE_BINDUNGEN = (
    [1012.4, 998.1, 1005.7, 1020.3, 990.2, 1001.8, 1015.0, 995.5, 1008.9, 1003.3],
    [1061.2, 1049.7, 1072.4, 1055.0, 1068.8, 1040.1, 1080.3, 1058.6, 1047.2, 1066.5],
)
_MIT_BINDUNGEN = (
    [12.0, 15.0, 15.0, 17.0, 18.0, 18.0, 18.0, 20.0, 21.0, 22.0, 22.0, 25.0],
    [14.0, 15.0, 18.0, 19.0, 20.0, 20.0, 22.0, 23.0, 26.0, 27.0],
)
_UEBERLAPPEND = (
    [3.1, 2.7, 4.4, 3.9, 2.2, 5.0, 3.3, 4.1, 2.9, 3.6, 4.8],
    [3.4, 4.0, 2.5, 4.6, 3.8, 5.2, 4.3, 3.0, 4.9, 3.7],
)

# Zehn Werte um 1000 mit rund 1 % Streuung; die B-Seite ist dieselbe Folge, skaliert.
_STREUUNG = [1004.0, 993.0, 1010.0, 998.0, 1007.0, 990.0, 1002.0, 996.0, 1012.0, 988.0]


def _skaliert(faktor: float) -> list[float]:
    return [wert * faktor for wert in _STREUUNG]


@pytest.mark.parametrize(
    ("daten", "u", "p"),
    [
        (_OHNE_BINDUNGEN, 0.0, 0.00018267179110955002),
        (_MIT_BINDUNGEN, 44.5, 0.31960406927334095),
        (_UEBERLAPPEND, 44.0, 0.4596715909585638),
    ],
    ids=["ohne-bindungen", "mit-bindungen", "ueberlappend"],
)
def test_mann_whitney_matches_the_scipy_reference(daten: tuple[list[float], list[float]], u: float, p: float) -> None:
    result = stat.mann_whitney_u(*daten)

    assert result.u == u
    assert result.p == pytest.approx(p, rel=1e-12)


@pytest.mark.parametrize(
    ("daten", "verschiebung"),
    [(_OHNE_BINDUNGEN, 54.75), (_MIT_BINDUNGEN, 2.0), (_UEBERLAPPEND, 0.3)],
    ids=["ohne-bindungen", "mit-bindungen", "ueberlappend"],
)
def test_hodges_lehmann_matches_the_numpy_reference(
    daten: tuple[list[float], list[float]], verschiebung: float
) -> None:
    assert stat.hodges_lehmann(*daten) == pytest.approx(verschiebung, rel=1e-12)


def test_a_secured_eight_percent_throughput_gain_with_profiles_is_a_gain() -> None:
    auswertung = stat.auswerten(_STREUUNG, _skaliert(1.08), besser=stat.Richtung.GROESSER, profile=True)

    assert auswertung.urteil == stat.Urteil.GEWINN
    assert auswertung.p < 0.05
    assert auswertung.relativ == pytest.approx(0.08, abs=0.005)


def test_a_secured_three_percent_throughput_gain_is_no_gain() -> None:
    # AE2: gesichert, aber unter der 5-%-Schwelle (session-settled).
    auswertung = stat.auswerten(_STREUUNG, _skaliert(1.03), besser=stat.Richtung.GROESSER, profile=True)

    assert auswertung.p < 0.05
    assert auswertung.urteil == stat.Urteil.KEIN_GEWINN


def test_a_secured_eight_percent_gain_without_profiles_is_not_proven() -> None:
    auswertung = stat.auswerten(_STREUUNG, _skaliert(1.08), besser=stat.Richtung.GROESSER, profile=False)

    assert auswertung.urteil == stat.Urteil.NICHT_BELEGT


def test_without_a_secured_difference_there_is_no_gain() -> None:
    # Große Verschiebung (HL rund 13 %) bei wenigen Werten und großer Streuung: p über 0,05.
    a = [100.0, 140.0, 80.0, 120.0, 95.0]
    b = [130.0, 90.0, 150.0, 105.0, 115.0]

    auswertung = stat.auswerten(a, b, besser=stat.Richtung.GROESSER, profile=True)

    assert auswertung.p >= 0.05
    assert auswertung.relativ >= 0.05
    assert auswertung.urteil == stat.Urteil.KEIN_GEWINN


def test_a_secured_loss_is_a_deterioration_even_without_profiles() -> None:
    auswertung = stat.auswerten(_STREUUNG, _skaliert(0.97), besser=stat.Richtung.GROESSER, profile=False)

    assert auswertung.urteil == stat.Urteil.VERSCHLECHTERUNG


@pytest.mark.parametrize(
    ("faktor", "urteil", "relativ"),
    [(0.92, "Gewinn", 0.08), (1.08, "Verschlechterung", -0.08)],
    ids=["weniger-ist-gewinn", "mehr-ist-verschlechterung"],
)
def test_for_latency_smaller_is_better(faktor: float, urteil: str, relativ: float) -> None:
    auswertung = stat.auswerten(_STREUUNG, _skaliert(faktor), besser=stat.Richtung.KLEINER, profile=True)

    assert auswertung.urteil == urteil
    # ``relativ`` ist der Gewinn in der besseren Richtung, ``verschiebung`` bleibt B minus A.
    assert auswertung.relativ == pytest.approx(relativ, abs=0.005)
    assert auswertung.verschiebung == pytest.approx(1000 * (faktor - 1), abs=5)


def test_identical_values_on_both_sides_are_no_gain() -> None:
    werte = [1000.0] * 10

    auswertung = stat.auswerten(werte, werte, besser=stat.Richtung.GROESSER, profile=True)

    assert auswertung.p == 1.0
    assert auswertung.verschiebung == 0.0
    assert auswertung.urteil == stat.Urteil.KEIN_GEWINN
