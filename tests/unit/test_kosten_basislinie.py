"""Basislinie der Kostensuite (Plan 2026-10-24-1118, KTD7).

``scripts/kosten_basislinie.py`` vergleicht gemessene Kosten je Szenario
exakt mit ``tests/kosten/basislinie.toml`` (Ratsche in beide Richtungen) und
schreibt neue Werte mit einem Historieneintrag, der einen auflösbaren Grund
trägt.

Test-Liste:
- [x] gleiche Werte: grün
- [x] ein Wert höher: rot mit Szenario, Metrik, alt und neu
- [x] ein Wert niedriger: rot mit Hinweis auf ``kosten-basislinie``
- [x] eine neue Metrik zählt als Anstieg von 0, eine verschwundene als Senkung auf 0
- [x] andere Python-Version als in der Basislinie: rot mit beiden Versionen
- [x] ein Szenario ohne Basislinienwert: rot statt still neu aufgenommen
- [x] ``kosten-basislinie`` ohne Grund: Abbruch
- [x] ein Grund ohne auflösbaren Verweis (``E-nnn``, Befund-ID, Planpfad): Abbruch
- [x] auflösbar sind ``E-nnn`` mit Datei im Pack, eine Befund-ID aus einem
      Register unter ``docs/befunde/`` und ein existierender Planpfad
- [x] die neue Basislinie trägt die gemessenen Werte, die Python-Version und
      einen Historieneintrag mit alten und neuen Werten
- [x] ein neuer Historieneintrag lässt alle alten unverändert
- [x] ohne Änderung entsteht kein Historieneintrag
- [x] die geschriebene Datei liest ``tomllib`` wieder als dieselben Daten
- [x] Wächter: ein Anstieg in der Historie ohne auflösbaren Verweis wird gemeldet
- [x] Wächter: ``werte``, die vom Nachspielen der Historie abweichen (Handänderung), werden gemeldet
- [x] Wächter: eine Änderung nur mit ``alt`` entfernt beim Nachspielen die Metrik, ein leeres Szenario verschwindet
- [x] das Skript bricht ab, wenn ein Szenario der alten Basislinie in der Messung fehlt
- [x] mit ``--entfernen <szenario>`` fällt das Szenario ausdrücklich aus der Basislinie
- [x] das Skript als Prozess: ohne Grund Exit 2, mit Grund schreibt es die Datei
"""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from tests.support.skripte import lade_skript

pytestmark = pytest.mark.timeout(5)


_SKRIPT = Path(__file__).resolve().parents[2] / "scripts" / "kosten_basislinie.py"


kb = lade_skript("kosten_basislinie", _SKRIPT)

_PY = "3.14.4"


def _basislinie(**werte: dict[str, int]) -> dict[str, object]:
    return {"python": _PY, "werte": werte, "historie": []}


def test_equal_values_pass() -> None:
    basislinie = _basislinie(publish={"broker.publish": 1})

    assert kb.vergleiche(basislinie, "publish", {"broker.publish": 1}, _PY) == []


def test_a_higher_value_fails_with_scenario_metric_old_and_new() -> None:
    basislinie = _basislinie(publish={"broker.publish": 1})

    [fehler] = kb.vergleiche(basislinie, "publish", {"broker.publish": 2}, _PY)

    assert "publish" in fehler
    assert "broker.publish" in fehler
    assert "1 -> 2" in fehler


def test_a_lower_value_fails_with_a_hint_to_update_the_baseline() -> None:
    basislinie = _basislinie(retry={"backend.is_cancelled": 3})

    [fehler] = kb.vergleiche(basislinie, "retry", {"backend.is_cancelled": 2}, _PY)

    assert "retry: backend.is_cancelled" in fehler
    assert "3 -> 2" in fehler
    assert "uv run just kosten-basislinie" in fehler


def test_a_new_metric_counts_as_a_rise_from_zero_and_a_vanished_one_as_a_drop_to_zero() -> None:
    basislinie = _basislinie(einfacher_task={"broker.ack": 1})

    fehler = kb.vergleiche(basislinie, "einfacher_task", {"broker.dead_letter": 1}, _PY)

    assert fehler == [
        "einfacher_task: broker.ack gesunken, 1 -> 0; " + kb._NACHFUEHREN,
        "einfacher_task: broker.dead_letter gestiegen, 0 -> 1",
    ]


def test_another_python_version_fails_with_both_versions() -> None:
    basislinie = _basislinie(publish={"broker.publish": 1})

    [fehler] = kb.vergleiche(basislinie, "publish", {"broker.publish": 1}, "3.14.5")

    assert "3.14.4" in fehler
    assert "3.14.5" in fehler


def test_a_scenario_without_a_baseline_value_fails_instead_of_being_added() -> None:
    basislinie = _basislinie(publish={"broker.publish": 1})

    [fehler] = kb.vergleiche(basislinie, "workflow_kette", {"broker.publish": 2}, _PY)

    assert "workflow_kette" in fehler
    assert "ohne Basislinienwert" in fehler


@pytest.fixture
def wurzel(tmp_path: Path) -> Path:
    """Repo mit einer Entscheidung, einem Befundregister und einem Plan."""
    pack = tmp_path / "compound-packs" / "swarmq-entscheidungen"
    pack.mkdir(parents=True)
    (pack / "e026-delay-stufen-bis-1h.md").write_text("---\nid: E-026\n---\n", encoding="utf-8")
    befunde = tmp_path / "docs" / "befunde"
    befunde.mkdir(parents=True)
    (befunde / "fehlersuche-runde-4-2026-10.md").write_text("| F4-27 | FEHLER |\n", encoding="utf-8")
    (befunde / "README.md").write_text("Beispiel: F9-99\n", encoding="utf-8")
    plans = tmp_path / "docs" / "plans"
    plans.mkdir(parents=True)
    (plans / "2026-10-24-1118-chore-qualitaetsablauf-plan.md").write_text("# Plan\n", encoding="utf-8")
    return tmp_path


def test_an_empty_reason_is_refused(wurzel: Path) -> None:
    assert kb.grund_fehler("  ", wurzel) == "Grund fehlt"


@pytest.mark.parametrize(
    "grund",
    [
        "Mehr Roundtrips für den Ausführungsschutz",
        "Entscheidung E-099 verlangt es",  # keine Datei im Pack
        "Befund F4-99",  # in keinem Register
        "Befund F9-99",  # nur in der README, die kein Register ist
        "Plan docs/plans/2026-01-01-0000-fehlt-plan.md",
    ],
)
def test_a_reason_without_a_resolvable_reference_is_refused(wurzel: Path, grund: str) -> None:
    fehler = kb.grund_fehler(grund, wurzel)

    assert fehler is not None
    assert "Verweis" in fehler


@pytest.mark.parametrize(
    "grund",
    [
        "Delay-Stufen kosten eine Deklaration (E-026)",
        "Fix für F4-27 braucht einen zweiten Aufruf",
        "Erste Basislinie (docs/plans/2026-10-24-1118-chore-qualitaetsablauf-plan.md, U3)",
    ],
)
def test_a_reason_with_a_decision_finding_or_plan_reference_is_accepted(wurzel: Path, grund: str) -> None:
    assert kb.grund_fehler(grund, wurzel) is None


def test_the_new_baseline_holds_the_measurement_and_a_history_entry_with_old_and_new_values() -> None:
    alt = _basislinie(publish={"broker.publish": 1}, retry={"broker.ack": 1})
    messung = {"publish": {"broker.publish": 2}, "retry": {"broker.ack": 1}, "neu": {"module": 3}}

    neu = kb.aktualisiere(alt, messung, "3.14.5", "Grund E-026", "2026-10-24")

    assert neu["python"] == "3.14.5"
    assert neu["werte"] == messung
    assert neu["historie"] == [
        {
            "datum": "2026-10-24",
            "grund": "Grund E-026",
            "python": "3.14.5",
            "aenderungen": [
                {"szenario": "neu", "metrik": "module", "neu": 3},
                {"szenario": "publish", "metrik": "broker.publish", "alt": 1, "neu": 2},
            ],
        }
    ]


def test_a_new_history_entry_leaves_every_old_one_unchanged() -> None:
    frueher = {
        "datum": "2026-10-01",
        "grund": "E-026",
        "python": _PY,
        "aenderungen": [{"szenario": "a", "metrik": "m", "neu": 1}],
    }
    alt = {"python": _PY, "werte": {"a": {"m": 1}}, "historie": [frueher]}

    neu = kb.aktualisiere(alt, {"a": {"m": 2}}, _PY, "F4-27", "2026-10-24")

    assert neu["historie"][0] == frueher
    assert len(neu["historie"]) == 2


def test_without_a_change_no_history_entry_is_added() -> None:
    alt = _basislinie(a={"m": 1})

    neu = kb.aktualisiere(alt, {"a": {"m": 1}}, _PY, "F4-27", "2026-10-24")

    assert neu["historie"] == []


def test_a_new_python_version_alone_gets_a_history_entry() -> None:
    alt = _basislinie(a={"m": 1})

    neu = kb.aktualisiere(alt, {"a": {"m": 1}}, "3.14.5", "docs/plans/x.md", "2026-10-24")

    assert neu["historie"] == [
        {"datum": "2026-10-24", "grund": "docs/plans/x.md", "python": "3.14.5", "aenderungen": []}
    ]


def test_the_written_file_reads_back_as_the_same_data() -> None:
    basislinie = {
        "python": _PY,
        "werte": {"publish": {"amqp.frames": 3, "broker.publish": 1}, "import_swarmq": {"module": 151}},
        "historie": [
            {
                "datum": "2026-10-24",
                "grund": 'Erste "Basislinie" für E-026\\Pfad',
                "python": _PY,
                "aenderungen": [{"szenario": "publish", "metrik": "amqp.frames", "alt": 2, "neu": 3}],
            },
            {"datum": "2026-10-25", "grund": "F4-27", "python": _PY, "aenderungen": []},
        ],
    }

    assert tomllib.loads(kb.als_toml(basislinie)) == basislinie


def test_the_guard_reports_a_rise_in_the_history_without_a_resolvable_reference(wurzel: Path) -> None:
    def eintrag(grund: str, alt: int | None, neu: int) -> dict[str, object]:
        aenderung: dict[str, object] = {"szenario": "retry", "metrik": "broker.publish", "neu": neu}
        if alt is not None:
            aenderung["alt"] = alt
        return {"datum": "2026-10-24", "grund": grund, "python": _PY, "aenderungen": [aenderung]}

    basislinie = {
        "python": _PY,
        "werte": {},
        "historie": [
            eintrag("Erste Basislinie (docs/plans/2026-10-24-1118-chore-qualitaetsablauf-plan.md)", None, 1),
            eintrag("von Hand angehoben", 1, 2),
            eintrag("aufgeräumt", 2, 1),  # eine Senkung braucht im Wächter keinen Verweis
            eintrag("neue Metrik ohne Verweis", None, 1),
        ],
    }

    gemeldet = kb.unbelegte_anstiege(basislinie, wurzel)

    assert len(gemeldet) == 2
    assert "von Hand angehoben" in gemeldet[0]
    assert "neue Metrik ohne Verweis" in gemeldet[1]


def test_the_guard_reports_values_that_differ_from_the_replayed_history() -> None:
    basislinie = {
        "python": _PY,
        "werte": {"retry": {"broker.publish": 1}},
        "historie": [
            {
                "datum": "2026-10-24",
                "grund": "E-026",
                "python": _PY,
                "aenderungen": [{"szenario": "retry", "metrik": "broker.publish", "neu": 2}],
            }
        ],
    }

    [gemeldet] = kb.handaenderungen(basislinie)

    assert "retry: broker.publish" in gemeldet
    assert "2" in gemeldet
    assert "1" in gemeldet


def test_replaying_a_change_with_only_an_old_value_removes_the_metric() -> None:
    def eintrag(*aenderungen: dict[str, object]) -> dict[str, object]:
        return {"datum": "2026-10-24", "grund": "E-026", "python": _PY, "aenderungen": list(aenderungen)}

    basislinie = {
        "python": _PY,
        "werte": {"publish": {"broker.publish": 1}},
        "historie": [
            eintrag(
                {"szenario": "publish", "metrik": "broker.publish", "neu": 1},
                {"szenario": "valkey_retry", "metrik": "valkey.SET", "neu": 1},
            ),
            eintrag({"szenario": "valkey_retry", "metrik": "valkey.SET", "alt": 1}),
        ],
    }

    assert kb.handaenderungen(basislinie) == []


def _skript(wurzel: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SKRIPT), *args, "--wurzel", str(wurzel)],
        capture_output=True,
        text=True,
        check=False,
    )


def _messung(wurzel: Path, **werte: dict[str, int]) -> Path:
    datei = wurzel / "messung.jsonl"
    zeilen = [json.dumps({"szenario": name, "python": _PY, "werte": w}) for name, w in werte.items()]
    datei.write_text("\n".join(zeilen) + "\n", encoding="utf-8")
    return datei


def test_the_script_without_a_reason_aborts_and_leaves_the_baseline_alone(wurzel: Path) -> None:
    """Als Prozess (über 100 ms möglich): ``just kosten-basislinie`` ruft das Skript so auf."""
    messung = _messung(wurzel, publish={"broker.publish": 2})

    result = _skript(wurzel, "", str(messung))

    assert result.returncode == 2
    assert "Grund fehlt" in result.stderr
    assert not (wurzel / "tests" / "kosten" / "basislinie.toml").exists()


def test_the_script_with_a_reason_writes_the_measured_values_and_a_history_entry(wurzel: Path) -> None:
    messung = _messung(wurzel, publish={"broker.publish": 1}, import_swarmq={"module": 151})
    grund = "Erste Basislinie (docs/plans/2026-10-24-1118-chore-qualitaetsablauf-plan.md)"

    result = _skript(wurzel, grund, str(messung))

    assert result.returncode == 0, result.stderr
    geschrieben = tomllib.loads((wurzel / "tests" / "kosten" / "basislinie.toml").read_text(encoding="utf-8"))
    assert geschrieben["python"] == _PY
    assert geschrieben["werte"] == {"import_swarmq": {"module": 151}, "publish": {"broker.publish": 1}}
    [eintrag] = geschrieben["historie"]
    assert eintrag["grund"] == grund
    assert len(eintrag["aenderungen"]) == 2


def _alte_basislinie(wurzel: Path, **werte: dict[str, int]) -> Path:
    pfad = wurzel / "tests" / "kosten" / "basislinie.toml"
    pfad.parent.mkdir(parents=True)
    aenderungen = [
        {"szenario": szenario, "metrik": metrik, "neu": wert}
        for szenario, metriken in werte.items()
        for metrik, wert in metriken.items()
    ]
    historie = [{"datum": "2026-10-24", "grund": "E-026", "python": _PY, "aenderungen": aenderungen}]
    pfad.write_text(kb.als_toml({"python": _PY, "werte": werte, "historie": historie}), encoding="utf-8")
    return pfad


def test_the_script_aborts_when_a_scenario_of_the_old_baseline_is_missing(wurzel: Path) -> None:
    """Etwa der Valkey-Teil, wenn kein Test-Valkey lief: Seine Szenarien fielen sonst still weg."""
    pfad = _alte_basislinie(wurzel, publish={"broker.publish": 1}, valkey_retry={"valkey.SET": 1})
    vorher = pfad.read_text(encoding="utf-8")
    messung = _messung(wurzel, publish={"broker.publish": 2})

    result = _skript(wurzel, "Grund E-026", str(messung))

    assert result.returncode == 2
    assert "valkey_retry" in result.stderr
    assert "--entfernen" in result.stderr
    assert pfad.read_text(encoding="utf-8") == vorher


def test_with_entfernen_the_scenario_leaves_the_baseline_explicitly(wurzel: Path) -> None:
    pfad = _alte_basislinie(wurzel, publish={"broker.publish": 1}, valkey_retry={"valkey.SET": 1})
    messung = _messung(wurzel, publish={"broker.publish": 1})

    result = _skript(wurzel, "Grund E-026", str(messung), "--entfernen", "valkey_retry")

    assert result.returncode == 0, result.stderr
    geschrieben = tomllib.loads(pfad.read_text(encoding="utf-8"))
    assert geschrieben["werte"] == {"publish": {"broker.publish": 1}}
    assert geschrieben["historie"][-1]["aenderungen"] == [
        {"szenario": "valkey_retry", "metrik": "valkey.SET", "alt": 1}
    ]
    assert kb.handaenderungen(geschrieben) == []
