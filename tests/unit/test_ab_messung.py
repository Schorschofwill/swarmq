"""A/B-Messwerkzeug (Plan 2026-10-24-1118, KTD12, R5).

``scripts/ab_messung.py`` legt für zwei Refs je einen git-Worktree an, ruft
den Messbefehl abwechselnd A, B, A, B auf, liest je Lauf eine Kennzahl aus
JSON und schreibt Rohwerte, Test, Verschiebung und Urteil in eine
Ergebnisdatei. Bei p ≥ 0,05 nach 10 Paaren wird einmal auf 30 erweitert.
Je Seite entsteht vorab ein Profil mit ``py-spy record``.

Geprüft am echten Skript in einem eigenen Git-Repository im ``tmp_path``:
Die Refs ``a`` und ``b`` tragen ein Messskript, das je Aufruf den nächsten
Wert seiner Seite ausgibt und die Seite protokolliert. ``py-spy`` ist ein
Stub im ``PATH``.

Über 100 ms je Test, bewusst: Das Skript legt echte Worktrees an und startet
je Lauf einen echten Prozess, ein Nachbau bewiese nichts. Keine Dienste,
deshalb Unit-Tests (AGENTS.md: Begründung im Test).

Test-Liste:
- [x] weniger als 10 Paare werden abgelehnt, ohne einen Lauf
- [x] die Reihenfolge der Aufrufe ist A, B, A, B, jeder Lauf im Worktree seiner Seite
- [x] 10 Paare mit klarer Verschiebung von 8 %: Gewinn, keine Erweiterung, Ergebnisdatei mit Rohwerten
- [x] 10 Paare ohne Unterschied: einmalige Erweiterung auf 30, dann kein Gewinn
- [x] 8 % ohne Profile (py-spy scheitert): nicht belegt, die Meldung von py-spy erscheint
- [x] nach dem Lauf sind die Worktrees entfernt
- [x] ein relativer Ergebnispfad gilt ab dem Aufrufverzeichnis, die Profile liegen daneben
- [x] ein scheiternder Messbefehl: Abbruch mit Meldung, Worktrees entfernt
- [x] der Messbefehl sieht das äußere venv nicht (``just ab`` läuft unter ``uv run``):
      kein ``VIRTUAL_ENV``, dessen ``bin`` nicht im ``PATH``
- [x] py-spy aus dem äußeren venv wird gefunden, sein Kind sieht das venv nicht
- [x] vor dem Profillauf läuft der Befehl je Seite einmal ohne Wertung (das Profil misst den Befehl, nicht das Einrichten)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(30)

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ab_messung.py"

# Gibt je Aufruf den nächsten Wert aus ``werte`` aus (Zählung über das Protokoll),
# davor eine Zeile Fortschritt wie ein echter Benchmark.
_MESS_SH = """#!/usr/bin/env bash
set -eu
seite=$(cat seite)
echo "$seite $PWD" >> "$AB_LOG"
n=$(grep -c "^$seite " "$AB_LOG")
echo "Lauf $n"
echo "{\\"ergebnis\\": {\\"durchsatz\\": $(sed -n "${n}p" werte)}}"
"""

# py-spy-Stub: schreibt die Profildatei hinter ``--output``, protokolliert das Verzeichnis.
_PY_SPY = """#!/usr/bin/env bash
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then out="$2"; fi
  shift
done
echo "$PWD" >> "$PY_SPY_LOG"
if [[ -n "${PY_SPY_RC:-}" ]]; then echo "Error: No python processes found" >&2; exit "$PY_SPY_RC"; fi
echo profil > "$out"
"""

# Dreißig Werte um 1000 mit rund 1 % Streuung.
_BASIS = [
    1004.0, 993.0, 1010.0, 998.0, 1007.0, 990.0, 1002.0, 996.0, 1012.0, 988.0,
    1001.0, 995.0, 1009.0, 999.0, 1005.0, 991.0, 1003.0, 997.0, 1011.0, 989.0,
    1006.0, 994.0, 1008.0, 1000.0, 992.0, 1013.0, 987.0, 1004.5, 996.5, 1002.5,
]  # fmt: skip


_VORLAUF = 1.0


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _repo(tmp_path: Path, werte_a: list[float], werte_b: list[float]) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "mess.sh").write_text(_MESS_SH, encoding="utf-8")
    for seite, werte in (("a", werte_a), ("b", werte_b)):
        (repo / "seite").write_text(f"{seite}\n", encoding="utf-8")
        # Die erste Zeile verbraucht der Vorlauf vor dem Profil.
        (repo / "werte").write_text("".join(f"{wert}\n" for wert in [_VORLAUF, *werte]), encoding="utf-8")
        _git(repo, "add", ".")
        _git(repo, "commit", "-qm", seite)
        _git(repo, "tag", seite)
    return repo


@pytest.fixture
def env(tmp_path: Path) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "py-spy"
    stub.write_text(_PY_SPY, encoding="utf-8")
    stub.chmod(0o755)
    return os.environ | {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "AB_LOG": str(tmp_path / "ab.log"),
        "PY_SPY_LOG": str(tmp_path / "py-spy.log"),
    }


def _messen(repo: Path, env: dict[str, str], *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--a",
            "a",
            "--b",
            "b",
            "--befehl",
            "bash mess.sh",
            "--kennzahl",
            "ergebnis.durchsatz",
            "--besser",
            "groesser",
            "--ergebnis",
            str(repo.parent / "ergebnis.json"),
            *extra,
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_fewer_than_ten_pairs_are_rejected(tmp_path: Path, env: dict[str, str]) -> None:
    repo = _repo(tmp_path, _BASIS, _BASIS)

    result = _messen(repo, env, "--paare", "9")

    assert result.returncode == 2
    assert "10" in result.stderr
    assert not Path(env["AB_LOG"]).exists()


def _log(env: dict[str, str]) -> list[tuple[str, str]]:
    zeilen = Path(env["AB_LOG"]).read_text(encoding="utf-8").splitlines()
    return [(seite, verzeichnis) for seite, verzeichnis in (zeile.split(" ", 1) for zeile in zeilen)]


def test_runs_alternate_a_b_each_in_the_worktree_of_its_side(tmp_path: Path, env: dict[str, str]) -> None:
    repo = _repo(tmp_path, _BASIS, [wert * 1.08 for wert in _BASIS])

    result = _messen(repo, env)

    assert result.returncode == 0, result.stderr
    laeufe = _log(env)
    assert [seite for seite, _ in laeufe] == ["a", "b"] * 11
    verzeichnisse = {seite: {verzeichnis for s, verzeichnis in laeufe if s == seite} for seite in "ab"}
    assert all(len(menge) == 1 for menge in verzeichnisse.values())
    assert verzeichnisse["a"] != verzeichnisse["b"]
    assert str(repo) not in verzeichnisse["a"] | verzeichnisse["b"]


def test_each_side_runs_once_unscored_before_its_profile(tmp_path: Path, env: dict[str, str]) -> None:
    """Der erste Lauf legt oft erst die Umgebung an (``uv venv``); py-spy hinge sich sonst daran."""
    repo = _repo(tmp_path, _BASIS, [wert * 1.08 for wert in _BASIS])
    env["PY_SPY_LOG"] = env["AB_LOG"]

    result = _messen(repo, env)

    assert result.returncode == 0, result.stderr
    zeilen = Path(env["AB_LOG"]).read_text(encoding="utf-8").splitlines()
    assert [zeile.split(" ", 1)[0] if " " in zeile else "profil" for zeile in zeilen[:4]] == [
        "a",
        "b",
        "profil",
        "profil",
    ]
    assert _ergebnis(repo)["werte_a"] == _BASIS[:10]


def _ergebnis(repo: Path) -> dict[str, object]:
    return json.loads((repo.parent / "ergebnis.json").read_text(encoding="utf-8"))  # type: ignore[no-any-return]  # JSON-Datei des Skripts


def test_a_clear_eight_percent_shift_over_ten_pairs_is_a_gain(tmp_path: Path, env: dict[str, str]) -> None:
    repo = _repo(tmp_path, _BASIS, [wert * 1.08 for wert in _BASIS])

    result = _messen(repo, env)

    assert result.returncode == 0, result.stderr
    ergebnis = _ergebnis(repo)
    assert ergebnis["urteil"] == "Gewinn"
    assert ergebnis["paare"] == 10
    assert ergebnis["erweitert"] is False
    assert ergebnis["werte_a"] == _BASIS[:10]
    assert ergebnis["werte_b"] == [wert * 1.08 for wert in _BASIS[:10]]
    assert isinstance(ergebnis["p"], float)
    assert ergebnis["p"] < 0.05
    assert ergebnis["u"] == 0.0
    assert ergebnis["verschiebung"] == pytest.approx(80, abs=5)
    assert ergebnis["relativ"] == pytest.approx(0.08, abs=0.005)
    assert ergebnis["commit_a"] == _git(repo, "rev-parse", "a").strip()
    assert ergebnis["commit_b"] == _git(repo, "rev-parse", "b").strip()
    profile = ergebnis["profile"]
    assert isinstance(profile, dict)
    assert all(Path(profile[seite]).read_text(encoding="utf-8") == "profil\n" for seite in "ab")
    assert "Gewinn" in result.stdout


def test_ten_pairs_without_a_difference_extend_once_to_thirty_then_no_gain(tmp_path: Path, env: dict[str, str]) -> None:
    repo = _repo(tmp_path, _BASIS, _BASIS[::-1])

    result = _messen(repo, env)

    assert result.returncode == 0, result.stderr
    ergebnis = _ergebnis(repo)
    assert ergebnis["erweitert"] is True
    assert ergebnis["paare"] == 30
    assert ergebnis["werte_a"] == _BASIS
    assert ergebnis["urteil"] == "kein Gewinn"
    assert [seite for seite, _ in _log(env)] == ["a", "b"] * 31


def test_an_eight_percent_gain_without_profiles_is_not_proven(tmp_path: Path, env: dict[str, str]) -> None:
    repo = _repo(tmp_path, _BASIS, [wert * 1.08 for wert in _BASIS])
    env["PY_SPY_RC"] = "1"

    result = _messen(repo, env)

    assert result.returncode == 0, result.stderr
    ergebnis = _ergebnis(repo)
    assert ergebnis["urteil"] == "nicht belegt"
    assert ergebnis["profile"] is None
    assert "No python processes found" in result.stderr
    # Das Profil entsteht im Worktree der jeweiligen Seite, nicht im Repository.
    assert len(set(Path(env["PY_SPY_LOG"]).read_text(encoding="utf-8").splitlines()) - {str(repo)}) == 2


def _worktrees(repo: Path) -> list[str]:
    return [
        zeile for zeile in _git(repo, "worktree", "list", "--porcelain").splitlines() if zeile.startswith("worktree ")
    ]


def test_the_worktrees_are_removed_after_the_run(tmp_path: Path, env: dict[str, str]) -> None:
    repo = _repo(tmp_path, _BASIS, [wert * 1.08 for wert in _BASIS])

    result = _messen(repo, env)

    assert result.returncode == 0, result.stderr
    assert _worktrees(repo) == [f"worktree {repo}"]


def test_a_failing_command_aborts_with_a_message_and_removes_the_worktrees(tmp_path: Path, env: dict[str, str]) -> None:
    repo = _repo(tmp_path, _BASIS, _BASIS)

    result = _messen(repo, env, "--befehl", "echo kaputt >&2; exit 3")

    assert result.returncode == 1
    assert "kaputt" in result.stderr
    assert "Traceback" not in result.stderr
    assert not (repo.parent / "ergebnis.json").exists()
    assert _worktrees(repo) == [f"worktree {repo}"]


def test_a_relative_result_path_keeps_the_profiles_next_to_it(tmp_path: Path, env: dict[str, str]) -> None:
    # Der Messbefehl und py-spy laufen im Worktree; relative Pfade gelten trotzdem ab dem Aufrufverzeichnis.
    repo = _repo(tmp_path, _BASIS, [wert * 1.08 for wert in _BASIS])

    result = _messen(repo, env, "--ergebnis", "ab.json")

    assert result.returncode == 0, result.stderr
    ergebnis = json.loads((repo / "ab.json").read_text(encoding="utf-8"))
    assert ergebnis["urteil"] == "Gewinn"
    assert (repo / "ab-profil-a.svg").exists()
    assert (repo / "ab-profil-b.svg").exists()


# Ein venv wie das, das ``uv run`` für ``just ab`` setzt: ``VIRTUAL_ENV`` und dessen ``bin`` vorn im ``PATH``.
# Ein bloßes ``python`` im Messbefehl träfe sonst den Interpreter (und das editierbare swarmq) des
# Haupt-Checkouts statt des Worktrees.
def _aeusseres_venv(tmp_path: Path, env: dict[str, str]) -> Path:
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    python = venv / "bin" / "python"
    python.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    python.chmod(0o755)
    env["VIRTUAL_ENV"] = str(venv)
    env["PATH"] = f"{venv / 'bin'}{os.pathsep}{env['PATH']}"
    env["UMGEBUNG_LOG"] = str(tmp_path / "umgebung.log")
    return venv


def test_the_command_does_not_see_the_outer_venv(tmp_path: Path, env: dict[str, str]) -> None:
    repo = _repo(tmp_path, _BASIS, [wert * 1.08 for wert in _BASIS])
    venv = _aeusseres_venv(tmp_path, env)
    befehl = 'echo "${VIRTUAL_ENV:-leer} $(command -v python || echo keins)" >> "$UMGEBUNG_LOG"; bash mess.sh'

    result = _messen(repo, env, "--befehl", befehl)

    assert result.returncode == 0, result.stderr
    zeilen = Path(env["UMGEBUNG_LOG"]).read_text(encoding="utf-8").splitlines()
    assert len(zeilen) == 22  # zwei Vorläufe, zehn Paare
    assert all(zeile.split(" ")[0] == "leer" for zeile in zeilen)
    assert all(not zeile.split(" ")[1].startswith(str(venv)) for zeile in zeilen)


# py-spy-Stub im äußeren venv (Gruppe ``messung``): protokolliert die Umgebung, die das profilierte Kind erbt.
_PY_SPY_IM_VENV = """#!/usr/bin/env bash
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then out="$2"; fi
  shift
done
echo "${VIRTUAL_ENV:-leer} $(command -v python || echo keins)" >> "$UMGEBUNG_LOG"
echo profil > "$out"
"""


def test_py_spy_from_the_outer_venv_is_found_and_its_child_does_not_see_the_venv(
    tmp_path: Path, env: dict[str, str]
) -> None:
    repo = _repo(tmp_path, _BASIS, [wert * 1.08 for wert in _BASIS])
    venv = _aeusseres_venv(tmp_path, env)
    stub = venv / "bin" / "py-spy"
    stub.write_text(_PY_SPY_IM_VENV, encoding="utf-8")
    stub.chmod(0o755)
    env["PATH"] = env["PATH"].replace(f"{tmp_path / 'bin'}{os.pathsep}", "")

    result = _messen(repo, env)

    assert result.returncode == 0, result.stderr
    assert _ergebnis(repo)["urteil"] == "Gewinn"
    zeilen = Path(env["UMGEBUNG_LOG"]).read_text(encoding="utf-8").splitlines()
    assert len(zeilen) == 2
    assert all(zeile.split(" ")[0] == "leer" for zeile in zeilen)
    assert all(not zeile.split(" ")[1].startswith(str(venv)) for zeile in zeilen)
