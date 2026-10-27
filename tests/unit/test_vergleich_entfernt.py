"""Vergleich auf einem entfernten Docker-Host (``scripts/vergleich-entfernt.sh``).

Der Messhost (celsius) startet nur Container: kein Repo, keine Umgebung,
keine Ergebnisse bleiben dort. Deshalb darf kein Compose-Stack des
Vergleichs eine Datei des Hosts einbinden; der Daemon suchte sie auf dem
entfernten Host.

Test-Liste:
- [x] kein Compose-Stack des Vergleichs bindet eine Datei oder ein Verzeichnis des Hosts ein
- [x] die Toxiproxy-Konfiguration in ``compose.yml`` passt zu ``stack.PROXYS`` und ``stack.PORTS``
- [x] ``starten`` baut das Image im Kontext aus einem sauberen Klon des Commits und startet den Lauf im Hintergrund
- [x] ``starten`` verweigert einen Arbeitsbaum mit Änderungen und einen zweiten Lauf auf demselben Host
- [x] ``starten`` hält einen laufenden GitLab-Runner des Hosts vor dem Lauf an und vermerkt das am Lauf-Container
- [x] ``starten`` lässt einen angehaltenen oder fehlenden Runner in Ruhe
- [x] ``abholen`` startet den Runner wieder, den ``starten`` angehalten hat, und nur diesen
- [x] ``status`` zeigt den Zustand des Laufs und das Ende seines Logs
- [x] ``abholen`` holt Ergebnisse und Log und entfernt Container und Image; solange der Lauf läuft, verweigert es
- [x] im Container startet ``performance`` den Dienste-Stack, führt den Runner aus, räumt den Stack auch nach einem Fehler ab und legt Ergebnisse und Ausgang ab
- [x] im Container schreibt ``garantie`` seine Ergebnisse direkt nach /ergebnisse; den Stack verwaltet der Garantie-Lauf selbst
- [x] im Container misst ``ab`` mit dem Dienste-Stack aus dem Repo heraus und schreibt das Ergebnis nach /ergebnisse
- [x] das Lauf-Image enthält das Git-Repo; ``ab`` legt daraus die Worktrees der beiden Refs an
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import yaml

VERGLEICH_DIR = Path(__file__).resolve().parents[2] / "benchmarks" / "comparison"


@pytest.mark.parametrize("compose", ["docker-compose.yml", "stoerungen/compose.yml"])
def test_kein_compose_stack_bindet_eine_datei_des_hosts_ein(compose: str) -> None:
    dienste = yaml.safe_load((VERGLEICH_DIR / compose).read_text())["services"]

    gebunden = {
        name: volume
        for name, dienst in dienste.items()
        for volume in dienst.get("volumes", [])
        if str(volume).startswith((".", "/", "~"))
    }

    assert gebunden == {}


@pytest.fixture
def stack(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    """``stoerungen.stack`` wie beim Lauf importiert; danach wieder entladen."""
    monkeypatch.syspath_prepend(str(VERGLEICH_DIR))
    yield importlib.import_module("stoerungen.stack")
    for name in list(sys.modules):
        if name.split(".")[0] in {"stoerungen", "garantieprofile"}:
            del sys.modules[name]


def test_toxiproxy_konfiguration_passt_zu_den_proxys_und_ports_des_stacks(stack: ModuleType) -> None:
    compose = yaml.safe_load((VERGLEICH_DIR / "stoerungen" / "compose.yml").read_text())
    proxys = json.loads(compose["configs"]["toxiproxy"]["content"])
    veroeffentlicht = {
        int(ziel): int(host) for host, ziel in (p.split(":") for p in compose["services"]["toxiproxy"]["ports"])
    }

    assert [p["name"] for p in proxys] == stack.PROXYS
    assert {p["name"]: veroeffentlicht[int(p["listen"].rsplit(":", 1)[1])] for p in proxys} == stack.PORTS


SKRIPT = Path(__file__).resolve().parents[2] / "scripts" / "vergleich-entfernt.sh"

# Schein-docker: protokolliert jeden Aufruf; ``ps`` und ``inspect`` antworten aus Umgebungsvariablen.
_SCHEIN_DOCKER = """#!/usr/bin/env bash
echo "$*" >> "$DOCKER_PROTOKOLL"
case " $* " in
  *" ps "*) printf '%s' "${SCHEIN_PS:-}" ;;
  *" inspect -f {{.State.Running}} gitlab-runner "*) echo "${SCHEIN_RUNNER_LAEUFT:-false}" ;;
  *" inspect -f {{.State.Running}} "*) echo "${SCHEIN_LAEUFT:-false}" ;;
  *" inspect -f {{.Config.Image}} "*) echo swarmq-vergleich:abc1234 ;;
  *"Config.Labels"*) echo "${SCHEIN_VERMERK:-}" ;;
esac
"""


def _repo(pfad: Path) -> str:
    """Ein Git-Repo mit einem Commit; gibt dessen Kurz-Hash zurück."""
    pfad.mkdir()
    for befehl in (["init", "-q"], ["add", "-A"], ["commit", "-q", "--allow-empty", "-m", "stand"]):
        subprocess.run(["git", "-C", str(pfad), "-c", "user.name=t", "-c", "user.email=t@t", *befehl], check=True)
    return subprocess.run(
        ["git", "-C", str(pfad), "rev-parse", "--short", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


def _aufruf(tmp_path: Path, *argumente: str, **umgebung: str) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    docker = bin_dir / "docker"
    docker.write_text(_SCHEIN_DOCKER)
    docker.chmod(0o755)
    protokoll = tmp_path / "docker.log"
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "DOCKER_PROTOKOLL": str(protokoll),
        **umgebung,
    }
    lauf = subprocess.run([str(SKRIPT), *argumente], env=env, capture_output=True, text=True, check=False)
    return lauf, protokoll.read_text().splitlines() if protokoll.exists() else []


def test_starten_baut_aus_sauberem_klon_und_startet_im_hintergrund(tmp_path: Path) -> None:
    commit = _repo(tmp_path / "repo")

    lauf, aufrufe = _aufruf(
        tmp_path, "starten", "celsius", "garantie", "--runs", "3", VERGLEICH_REPO=str(tmp_path / "repo")
    )

    assert lauf.returncode == 0, lauf.stderr
    bauen = next(a for a in aufrufe if " build " in f" {a} ")
    starten = next(a for a in aufrufe if " run " in f" {a} ")
    assert bauen.startswith("--context celsius build")
    assert f"-t swarmq-vergleich:{commit}" in bauen
    assert starten.startswith("--context celsius run -d --name swarmq-vergleich --network host")
    assert "-v /var/run/docker.sock:/var/run/docker.sock" in starten
    assert starten.endswith(f"swarmq-vergleich:{commit} garantie --runs 3")


def test_starten_verweigert_einen_arbeitsbaum_mit_aenderungen(tmp_path: Path) -> None:
    """Gemessen wird ein Commit; eine nicht committete Änderung wäre im Ergebnis nicht nachstellbar."""
    _repo(tmp_path / "repo")
    (tmp_path / "repo" / "neu.txt").write_text("x")

    lauf, aufrufe = _aufruf(tmp_path, "starten", "celsius", "garantie", VERGLEICH_REPO=str(tmp_path / "repo"))

    assert lauf.returncode == 2
    assert "Arbeitsbaum" in lauf.stderr
    assert aufrufe == []


def test_starten_verweigert_einen_zweiten_lauf_auf_demselben_host(tmp_path: Path) -> None:
    """Zwei Läufe gleichzeitig verfälschen beide Messungen; der erste wird erst abgeholt."""
    _repo(tmp_path / "repo")

    lauf, aufrufe = _aufruf(
        tmp_path, "starten", "celsius", "garantie", VERGLEICH_REPO=str(tmp_path / "repo"), SCHEIN_PS="0123abcd"
    )

    assert lauf.returncode == 2
    assert "abholen" in lauf.stderr
    assert not [a for a in aufrufe if " build " in f" {a} " or " run " in f" {a} "]


def test_starten_haelt_den_laufenden_runner_vor_dem_lauf_an(tmp_path: Path) -> None:
    """CI-Jobs auf dem Messhost verfälschen die Messung; ``abholen`` braucht den Vermerk."""
    _repo(tmp_path / "repo")

    lauf, aufrufe = _aufruf(
        tmp_path, "starten", "celsius", "garantie", VERGLEICH_REPO=str(tmp_path / "repo"), SCHEIN_RUNNER_LAEUFT="true"
    )

    assert lauf.returncode == 0, lauf.stderr
    anhalten = aufrufe.index("--context celsius stop gitlab-runner")
    starten = next(i for i, a in enumerate(aufrufe) if " run " in f" {a} ")
    assert anhalten < starten
    assert "--label swarmq.runner=gitlab-runner" in aufrufe[starten]


def test_starten_laesst_einen_angehaltenen_oder_fehlenden_runner_in_ruhe(tmp_path: Path) -> None:
    """Ohne Vermerk startet ``abholen`` keinen Runner, den jemand absichtlich angehalten hat."""
    _repo(tmp_path / "repo")

    lauf, aufrufe = _aufruf(tmp_path, "starten", "celsius", "garantie", VERGLEICH_REPO=str(tmp_path / "repo"))

    assert lauf.returncode == 0, lauf.stderr
    assert not [a for a in aufrufe if " stop " in f" {a} "]
    assert "swarmq.runner" not in next(a for a in aufrufe if " run " in f" {a} ")


def test_abholen_startet_den_angehaltenen_runner_wieder(tmp_path: Path) -> None:
    lauf, aufrufe = _aufruf(
        tmp_path, "abholen", "celsius", str(tmp_path / "ergebnisse"), SCHEIN_VERMERK="gitlab-runner"
    )

    assert lauf.returncode == 0, lauf.stderr
    assert aufrufe[-1] == "--context celsius start gitlab-runner"


def test_abholen_holt_ergebnisse_und_log_und_raeumt_container_und_image_ab(tmp_path: Path) -> None:
    ziel = tmp_path / "ergebnisse"

    lauf, aufrufe = _aufruf(tmp_path, "abholen", "celsius", str(ziel))

    assert lauf.returncode == 0, lauf.stderr
    assert f"--context celsius cp swarmq-vergleich:/ergebnisse/. {ziel}/" in aufrufe
    assert "--context celsius logs swarmq-vergleich" in aufrufe
    assert aufrufe[-2:] == ["--context celsius rm swarmq-vergleich", "--context celsius rmi swarmq-vergleich:abc1234"]
    assert (ziel / "lauf.log").exists()


def test_abholen_verweigert_solange_der_lauf_laeuft(tmp_path: Path) -> None:
    lauf, aufrufe = _aufruf(tmp_path, "abholen", "celsius", str(tmp_path / "ergebnisse"), SCHEIN_LAEUFT="true")

    assert lauf.returncode == 2
    assert "läuft noch" in lauf.stderr
    assert not [a for a in aufrufe if " rm " in f" {a} " or " rmi " in f" {a} "]


def test_status_zeigt_zustand_und_ende_des_logs(tmp_path: Path) -> None:
    lauf, aufrufe = _aufruf(tmp_path, "status", "celsius", SCHEIN_PS="Up 2 hours")

    assert lauf.returncode == 0, lauf.stderr
    assert "Up 2 hours" in lauf.stdout
    assert aufrufe[-1] == "--context celsius logs --tail 20 swarmq-vergleich"


EINSTIEG = VERGLEICH_DIR / "im-container.sh"


def _container(tmp_path: Path, *argumente: str, python_ausgang: int = 0) -> tuple[int, list[str], Path]:
    """Führt den Einstieg mit Schein-docker und Schein-Python der swarmq-Umgebung aus."""
    vergleich = tmp_path / "comparison"
    (vergleich / ".venvs" / "swarmq" / "bin").mkdir(parents=True)
    (vergleich / "results").mkdir()
    (vergleich / "results" / "noop.json").write_text("{}")
    (vergleich / "im-container.sh").write_text(EINSTIEG.read_text())
    (vergleich / "im-container.sh").chmod(0o755)
    python = vergleich / ".venvs" / "swarmq" / "bin" / "python"
    python.write_text(f'#!/usr/bin/env bash\necho "python $*" >> "$DOCKER_PROTOKOLL"\nexit {python_ausgang}\n')
    python.chmod(0o755)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(_SCHEIN_DOCKER)
    (bin_dir / "docker").chmod(0o755)
    (bin_dir / "uv").write_text('#!/usr/bin/env bash\necho "uv $* (in $(basename "$PWD"))" >> "$DOCKER_PROTOKOLL"\n')
    (bin_dir / "uv").chmod(0o755)
    protokoll = tmp_path / "docker.log"
    ergebnisse = tmp_path / "ergebnisse"
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "DOCKER_PROTOKOLL": str(protokoll),
        "VERGLEICH_ERGEBNISSE": str(ergebnisse),
    }
    lauf = subprocess.run([str(vergleich / "im-container.sh"), *argumente], env=env, check=False)
    return lauf.returncode, protokoll.read_text().splitlines(), ergebnisse


def test_performance_im_container_startet_den_stack_und_raeumt_ihn_auch_nach_einem_fehler_ab(tmp_path: Path) -> None:
    ausgang, aufrufe, ergebnisse = _container(tmp_path, "performance", "--runs", "3", python_ausgang=1)

    assert ausgang == 1
    assert aufrufe == [
        "compose -p swarmq-vergleich -f docker-compose.yml up -d --wait",
        "python runner.py --runs 3",
        "compose -p swarmq-vergleich -f docker-compose.yml down -v",
    ]
    assert (ergebnisse / "results" / "noop.json").exists()
    assert (ergebnisse / "ausgang").read_text().strip() == "1"


def test_garantie_im_container_schreibt_nach_ergebnisse(tmp_path: Path) -> None:
    ausgang, aufrufe, ergebnisse = _container(tmp_path, "garantie", "--runs", "3")

    assert ausgang == 0
    assert aufrufe == [f"python -m stoerungen.garantie --ergebnisse {ergebnisse}/garantie --runs 3"]
    assert (ergebnisse / "ausgang").read_text().strip() == "0"


def test_ab_im_container_misst_mit_dem_stack_aus_dem_repo(tmp_path: Path) -> None:
    """Der Einstieg liegt unter ``benchmarks/comparison``; ``ab_messung.py`` läuft zwei Ebenen höher."""
    ausgang, aufrufe, ergebnisse = _container(tmp_path, "ab", "--a", "v1", "--b", "v2")

    assert ausgang == 0
    messung = (
        "uv run --no-default-groups --group messung python scripts/ab_messung.py"
        f" --ergebnis {ergebnisse}/ab/ergebnis.json --a v1 --b v2 (in {tmp_path.parent.name})"
    )
    assert aufrufe == [
        "compose -p swarmq-vergleich -f docker-compose.yml up -d --wait",
        messung,
        "compose -p swarmq-vergleich -f docker-compose.yml down -v",
    ]


def _ausgeschlossen(dockerignore: Path) -> set[str]:
    return {z.strip() for z in dockerignore.read_text().splitlines() if z.strip() and not z.startswith("#")}


def test_das_lauf_image_enthaelt_das_git_repo_fuer_die_worktrees_von_ab() -> None:
    """BuildKit nimmt ``<Dockerfile>.dockerignore`` vor der ``.dockerignore`` im Kontext."""
    eigene = VERGLEICH_DIR / "Dockerfile.dockerignore"
    dockerignore = eigene if eigene.exists() else VERGLEICH_DIR.parents[1] / ".dockerignore"

    assert ".git" not in _ausgeschlossen(dockerignore)
