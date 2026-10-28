"""Runner-Supervisor für celsius (``ops/github-runner/github_runner.py``).

Plan 2026-10-28-1308, U6 (KTD4, KTD6). Abweichend vom Plan läuft der Runner
als Host-Prozess unter ``ghrunner`` aus einer frischen Kopie der gepinnten
Distribution: Bei ``container:``-Jobs bindet der Runner seine externals über
den Docker-Daemon ein, im Container-Betrieb stimmten die Pfade nicht.

Gefälscht werden nur die Grenzen: HTTP, Prozessstart und Zeit. Schalter,
Sperre und Zustandsdateien sind echte Dateien unter ``tmp_path``.

Test-Liste:
Slot-Schleife (Zustandsdiagramm „Ein Runner-Slot auf celsius“):
- [x] mit gesetztem Schalter holt ein Slot keine Konfiguration und meldet ``pausiert``
- [x] ohne Schalter holt ein Slot eine JIT-Konfiguration und startet genau einen Runner
- [x] die JIT-Anfrage nennt eindeutigen Namen, Labels und Arbeitsverzeichnis
- [x] während des Holens meldet der Slot ``holt``, während der Runner läuft ``laeuft``
- [x] nach dem Ende des Runners sind Kopie und Arbeitsverzeichnis gelöscht
- [x] Schalter zwischen Holen und Start: Konfiguration verworfen (DELETE), nichts gestartet
- [x] Nachprüfen und Start laufen unter der Sperre
- [x] Pause bei wartendem Runner: erst DELETE, nach 204 Runner stoppen
- [x] 422 (beschäftigt): Runner läuft bis zum Ende, kein zweiter Löschversuch
- [x] API-Fehler beim Holen: wachsende, begrenzte Wartezeit, kein Absturz; Erfolg setzt zurück
- [x] API-Fehler beim Löschen (wartend und beim Verwerfen): kein Absturz
- [x] Runner endet ungestoppt mit Fehlercode: Zustand ``fehler``, dieselbe wachsende Wartezeit; ein sauberes Ende setzt zurück (Review #5)
Starter (Host-Prozess statt Container):
- [x] genau ein Prozess als ghrunner mit ``DOCKER_HOST`` des rootless Docker, aus einer Kopie der Distribution
- [x] keine Zusatzgruppen des root-Supervisors
- [x] Reste eines abgebrochenen Laufs blockieren die Kopie nicht
- [x] die Kopie gehört ghrunner
- [x] ghrunner legt die Kopie selbst an; root ändert nur den Besitz des frischen Slot-Verzeichnisses, nie etwas im Baum (Review #1, Symlinks)
- [x] ``stop()`` beendet die Prozessgruppe, nach der Frist hart
- [x] das PAT erscheint in keiner Ausgabe, keinem Prozessargument und keiner Umgebung des Runners
GitHub-API und HTTP:
- [x] JIT per POST mit Gruppe 1; Fehlerstatus wird ApiError ohne PAT
- [x] DELETE: 204 gelöscht, 422 beschäftigt, sonst ApiError
- [x] neuestes Runner-Release und PAT-Ablauf aus einer Antwort
- [x] urllib: Status auch bei 4xx, Netzfehler als ApiError
Befehle:
- [x] ``pause`` setzt den Schalter unter der Sperre und wartet, bis jeder Slot ``pausiert`` meldet
- [x] ``pause`` mit Timeout gibt auf und nennt die beschäftigten Slots
- [x] ``pause`` bei gesetztem Schalter ändert nichts und meldet „schon pausiert“
- [x] ``resume`` entfernt den Schalter; ohne Schalter No-op
- [x] ``status``: Schalter und Slots, Warnung bei veraltetem Release, PAT < 14 Tage oder ohne Ablauf, API-Fehler
- [x] Konfiguration aus der Umgebung, leere Werte gelten als nicht gesetzt
- [x] ``main`` nennt fehlende Einstellungen statt abzustürzen
"""

from __future__ import annotations

import fcntl
import http.client
import importlib.util
import io
import json
import shutil
import signal
import subprocess
import sys
import urllib.error
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from types import ModuleType
from typing import Any, Self, override

import pytest

_PFAD = Path(__file__).resolve().parents[3] / "ops" / "github-runner" / "github_runner.py"


def _lade() -> ModuleType:
    spec = importlib.util.spec_from_file_location("github_runner", _PFAD)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclasses schlägt das Modul in sys.modules nach.
    sys.modules["github_runner"] = module
    spec.loader.exec_module(module)
    return module


gr = _lade()

TOKEN = "github_pat_GEHEIM0123456789"


class FalscheUhr:
    """Zeit ohne Schlafen; ``bei_schlaf`` erlaubt Wettläufe an genau dieser Stelle."""

    def __init__(self) -> None:
        self.geschlafen: list[float] = []
        self.jetzt = 1_000_000.0
        self.bei_schlaf: list[Any] = []

    def sleep(self, sekunden: float) -> None:
        self.geschlafen.append(sekunden)
        self.jetzt += sekunden
        if self.bei_schlaf:
            self.bei_schlaf.pop(0)()

    def monotonic(self) -> float:
        return self.jetzt


class FalschesGitHub:
    def __init__(self) -> None:
        self.jit_anfragen: list[dict[str, Any]] = []
        self.geloescht: list[int] = []
        self.loeschen_antwort = True
        self.jit_fehler: list[Exception] = []
        self.loesch_fehler: list[Exception] = []
        self.bei_jit: Any = None
        self.release: Any = None

    def jit_config(self, repo: str, name: str, labels: list[str], work_folder: str) -> Any:
        if self.jit_fehler:
            raise self.jit_fehler.pop(0)
        self.jit_anfragen.append({"repo": repo, "name": name, "labels": labels, "work_folder": work_folder})
        if self.bei_jit is not None:
            self.bei_jit()
        return gr.JitConfig(runner_id=42, encoded="JIT-KONFIG")

    def latest_runner_release(self) -> Any:
        if isinstance(self.release, Exception):
            raise self.release
        return self.release

    def delete_runner(self, repo: str, runner_id: int) -> bool:
        if self.loesch_fehler:
            raise self.loesch_fehler.pop(0)
        self.geloescht.append(runner_id)
        return self.loeschen_antwort


class FalscherRunner:
    def __init__(self, laeuft_polls: int, ausgang: int = 0) -> None:
        self.laeuft_polls = laeuft_polls
        self.ausgang = ausgang
        self.gestoppt = False

    def poll(self) -> int | None:
        if self.gestoppt:
            return 0
        if self.laeuft_polls <= 0:
            return self.ausgang
        self.laeuft_polls -= 1
        return None

    def stop(self) -> None:
        self.gestoppt = True


class FalscherStarter:
    def __init__(self, laeuft_polls: int = 0) -> None:
        self.starts: list[tuple[Path, str]] = []
        self.laeuft_polls = laeuft_polls
        self.ausgaenge: list[int] = []
        self.runner: FalscherRunner | None = None

    def start(self, slot_dir: Path, jitconfig: str) -> FalscherRunner:
        self.starts.append((slot_dir, jitconfig))
        self.runner = FalscherRunner(self.laeuft_polls, self.ausgaenge.pop(0) if self.ausgaenge else 0)
        return self.runner


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "state"


def _slot(state_dir: Path, github: FalschesGitHub, starter: FalscherStarter, uhr: FalscheUhr) -> Any:
    return gr.Slot(
        repo="swarmq",
        number=1,
        switch=gr.Switch(state_dir),
        github=github,
        launcher=starter,
        clock=uhr,
    )


def test_mit_gesetztem_schalter_holt_der_slot_keine_konfiguration(state_dir: Path) -> None:
    switch = gr.Switch(state_dir)
    switch.ensure()
    (state_dir / "pause").touch()
    github, starter, uhr = FalschesGitHub(), FalscherStarter(), FalscheUhr()

    _slot(state_dir, github, starter, uhr).run_once()

    assert github.jit_anfragen == []
    assert starter.starts == []
    assert (state_dir / "slots" / "swarmq-1.state").read_text().strip() == "pausiert"
    assert uhr.geschlafen != []


def test_ohne_schalter_holt_der_slot_eine_konfiguration_und_startet_genau_einen_runner(state_dir: Path) -> None:
    github, starter, uhr = FalschesGitHub(), FalscherStarter(), FalscheUhr()

    _slot(state_dir, github, starter, uhr).run_once()

    assert len(github.jit_anfragen) == 1
    assert starter.starts == [(state_dir / "slots" / "swarmq-1", "JIT-KONFIG")]


def test_die_jit_anfrage_nennt_eindeutigen_namen_labels_und_arbeitsverzeichnis(state_dir: Path) -> None:
    github, starter, uhr = FalschesGitHub(), FalscherStarter(), FalscheUhr()
    slot = _slot(state_dir, github, starter, uhr)

    slot.run_once()
    slot.run_once()

    erste, zweite = github.jit_anfragen
    assert erste["repo"] == "swarmq"
    assert erste["labels"] == ["self-hosted", "celsius-swarmq"]
    assert erste["work_folder"] == "_work"
    assert erste["name"].startswith("celsius-swarmq-1-")
    # Ein Name je Registrierung: GitHub lehnt einen schon vergebenen ab.
    assert erste["name"] != zweite["name"]


class KopierenderStarter(FalscherStarter):
    """Legt wie der echte Starter Kopie und Arbeitsverzeichnis im Slot-Verzeichnis an."""

    @override
    def start(self, slot_dir: Path, jitconfig: str) -> FalscherRunner:
        (slot_dir / "runner" / "_work" / "repo").mkdir(parents=True)
        (slot_dir / "runner" / "run.sh").write_text("#!/bin/sh\n")
        return super().start(slot_dir, jitconfig)


def test_der_slot_wartet_auf_das_ende_und_loescht_kopie_und_arbeitsverzeichnis(state_dir: Path) -> None:
    github, starter, uhr = FalschesGitHub(), KopierenderStarter(laeuft_polls=3), FalscheUhr()
    zustand = state_dir / "slots" / "swarmq-1.state"
    waehrend: list[str] = []
    uhr.bei_schlaf = [lambda: waehrend.append(zustand.read_text().strip())] * 3

    _slot(state_dir, github, starter, uhr).run_once()

    assert waehrend == ["laeuft"] * 3
    assert starter.runner is not None
    assert starter.runner.laeuft_polls == 0
    assert not (state_dir / "slots" / "swarmq-1").exists()


def test_schalter_zwischen_holen_und_start_verwirft_die_konfiguration(state_dir: Path) -> None:
    github, starter, uhr = FalschesGitHub(), FalscherStarter(), FalscheUhr()
    github.bei_jit = (state_dir / "pause").touch

    _slot(state_dir, github, starter, uhr).run_once()

    assert starter.starts == []
    assert github.geloescht == [42]
    assert (state_dir / "slots" / "swarmq-1.state").read_text().strip() == "pausiert"


def _sperre_frei(state_dir: Path) -> bool:
    with (state_dir / "lock").open("a") as datei:
        try:
            fcntl.flock(datei, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(datei, fcntl.LOCK_UN)
        return True


def test_nachpruefen_und_start_laufen_unter_der_sperre(state_dir: Path) -> None:
    github, uhr = FalschesGitHub(), FalscheUhr()
    beim_start: list[bool] = []

    class PruefenderStarter(FalscherStarter):
        @override
        def start(self, slot_dir: Path, jitconfig: str) -> FalscherRunner:
            beim_start.append(_sperre_frei(state_dir))
            return super().start(slot_dir, jitconfig)

    _slot(state_dir, github, PruefenderStarter(), uhr).run_once()

    assert beim_start == [False]
    assert _sperre_frei(state_dir)


def test_pause_loescht_erst_die_registrierung_und_stoppt_dann_den_wartenden_runner(state_dir: Path) -> None:
    github, uhr = FalschesGitHub(), FalscheUhr()
    reihenfolge: list[str] = []

    class MerkenderRunner(FalscherRunner):
        @override
        def stop(self) -> None:
            reihenfolge.append(f"stop nach delete {github.geloescht}")
            super().stop()

    class MerkenderStarter(FalscherStarter):
        @override
        def start(self, slot_dir: Path, jitconfig: str) -> FalscherRunner:
            self.runner = MerkenderRunner(laeuft_polls=100)
            return self.runner

    starter = MerkenderStarter()
    uhr.bei_schlaf = [(state_dir / "pause").touch]

    _slot(state_dir, github, starter, uhr).run_once()

    assert reihenfolge == ["stop nach delete [42]"]
    assert not (state_dir / "slots" / "swarmq-1").exists()


def test_lehnt_die_api_das_loeschen_ab_laeuft_der_beschaeftigte_runner_zu_ende(state_dir: Path) -> None:
    github, starter, uhr = FalschesGitHub(), FalscherStarter(laeuft_polls=5), FalscheUhr()
    github.loeschen_antwort = False
    uhr.bei_schlaf = [(state_dir / "pause").touch]

    _slot(state_dir, github, starter, uhr).run_once()

    assert starter.runner is not None
    assert not starter.runner.gestoppt
    assert starter.runner.laeuft_polls == 0
    # Ein ephemerer Runner wird nach seinem Job nicht wieder frei: ein Versuch genügt.
    assert github.geloescht == [42]


def test_api_fehler_beim_holen_fuehrt_zu_wachsender_begrenzter_wartezeit(state_dir: Path) -> None:
    github, starter, uhr = FalschesGitHub(), FalscherStarter(), FalscheUhr()
    github.jit_fehler = [gr.ApiError("HTTP 503")] * 8
    slot = _slot(state_dir, github, starter, uhr)

    for _ in range(8):
        slot.run_once()

    assert uhr.geschlafen == [5, 10, 20, 40, 80, 160, 300, 300]
    assert (state_dir / "slots" / "swarmq-1.state").read_text().strip() == "fehler"
    assert starter.starts == []

    slot.run_once()
    github.jit_fehler = [gr.ApiError("HTTP 503")]
    slot.run_once()

    assert len(starter.starts) == 1
    assert uhr.geschlafen[-1] == 5


def test_ein_runner_der_ungestoppt_mit_fehler_endet_bremst_den_slot(state_dir: Path) -> None:
    """Endet der Runner sofort (kaputte Kopie, abgelehnte Konfiguration), holte der Slot sonst ohne
    Pause eine Registrierung nach der anderen und verbrauchte das API-Limit des PAT."""
    github, starter, uhr = FalschesGitHub(), FalscherStarter(), FalscheUhr()
    starter.ausgaenge = [1, 1, 1, 0, 1]
    slot = _slot(state_dir, github, starter, uhr)

    for _ in range(3):
        slot.run_once()

    assert uhr.geschlafen == [5, 10, 20]
    assert (state_dir / "slots" / "swarmq-1.state").read_text().strip() == "fehler"

    slot.run_once()
    slot.run_once()

    assert uhr.geschlafen == [5, 10, 20, 5]


def test_waehrend_des_holens_meldet_der_slot_holt(state_dir: Path) -> None:
    github, starter, uhr = FalschesGitHub(), FalscherStarter(), FalscheUhr()
    zustand = state_dir / "slots" / "swarmq-1.state"
    beim_holen: list[str] = []
    github.bei_jit = lambda: beim_holen.append(zustand.read_text().strip() if zustand.exists() else "")

    _slot(state_dir, github, starter, uhr).run_once()

    assert beim_holen == ["holt"]


def test_api_fehler_beim_loeschen_eines_wartenden_runners_wird_beim_naechsten_abfragen_wiederholt(
    state_dir: Path,
) -> None:
    github, starter, uhr = FalschesGitHub(), FalscherStarter(laeuft_polls=100), FalscheUhr()
    github.loesch_fehler = [gr.ApiError("HTTP 502")]
    uhr.bei_schlaf = [(state_dir / "pause").touch]

    _slot(state_dir, github, starter, uhr).run_once()

    assert github.geloescht == [42]
    assert starter.runner is not None
    assert starter.runner.gestoppt


def test_api_fehler_beim_verwerfen_der_konfiguration_startet_trotzdem_nichts(state_dir: Path) -> None:
    github, starter, uhr = FalschesGitHub(), FalscherStarter(), FalscheUhr()
    github.bei_jit = (state_dir / "pause").touch
    github.loesch_fehler = [gr.ApiError("HTTP 502")]

    _slot(state_dir, github, starter, uhr).run_once()

    assert starter.starts == []
    assert (state_dir / "slots" / "swarmq-1.state").read_text().strip() == "pausiert"


class FalschesPopen:
    """Prozessgrenze: protokolliert jeden Start statt ihn auszuführen."""

    def __init__(self) -> None:
        self.aufrufe: list[tuple[list[str], dict[str, Any]]] = []
        self.ergebnis: int | None = None
        self.wait_ergebnisse: list[Any] = []

    def __call__(self, args: list[str], **kwargs: Any) -> FalschesPopen:
        self.aufrufe.append((list(args), kwargs))
        return self

    pid = 4711

    def poll(self) -> int | None:
        return self.ergebnis

    def wait(self, timeout: float | None = None) -> int:
        if self.wait_ergebnisse:
            ergebnis = self.wait_ergebnisse.pop(0)
            if isinstance(ergebnis, BaseException):
                raise ergebnis
        return 0


@pytest.fixture
def distribution(tmp_path: Path) -> Path:
    dist = tmp_path / "dist" / "2.329.0"
    (dist / "bin").mkdir(parents=True)
    (dist / "externals" / "node20").mkdir(parents=True)
    (dist / "run.sh").write_text("#!/bin/sh\n")
    (dist / "bin" / "Runner.Listener").write_text("")
    return dist


class FalschesKopieren:
    """Prozessgrenze für das Kopieren: kopiert wirklich, protokolliert den Aufruf."""

    def __init__(self) -> None:
        self.aufrufe: list[tuple[list[str], dict[str, Any]]] = []

    def __call__(self, args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.aufrufe.append((args, kwargs))
        quelle, ziel = args[-2], args[-1]
        shutil.copytree(quelle.removesuffix("/."), ziel, symlinks=True)
        return subprocess.CompletedProcess(args, 0)


def _starter(
    distribution: Path,
    popen: FalschesPopen,
    gechownt: list[Path],
    signale: list[Any] | None = None,
    kopieren: FalschesKopieren | None = None,
) -> Any:
    return gr.HostLauncher(
        dist_dir=distribution,
        user="ghrunner",
        home=Path("/home/ghrunner"),
        docker_host="unix:///run/user/1500/docker.sock",
        popen=popen,
        run=kopieren if kopieren is not None else FalschesKopieren(),
        chown=gechownt.append,
        killpg=lambda pid, sig: (signale if signale is not None else []).append((pid, sig)),
    )


def test_ghrunner_legt_die_kopie_selbst_an_und_root_aendert_im_baum_keinen_besitz(
    tmp_path: Path, distribution: Path
) -> None:
    """Root folgte beim rekursiven chown Symlinks, die ein überdauernder ghrunner-Prozess in die schon
    ghrunner-eigene Kopie legen konnte (Rechteausweitung). Jetzt kopiert ghrunner, root fasst nur das
    frische Slot-Verzeichnis an, das es selbst in seinem eigenen Verzeichnis angelegt hat."""
    kopieren, gechownt = FalschesKopieren(), list[Path]()
    slot_dir = tmp_path / "state" / "slots" / "swarmq-1"

    _starter(distribution, FalschesPopen(), gechownt, kopieren=kopieren).start(slot_dir, "JIT")

    assert gechownt == [slot_dir]
    [(args, kwargs)] = kopieren.aufrufe
    assert args == ["cp", "-a", "--no-preserve=ownership", f"{distribution}/.", str(slot_dir / "runner")]
    assert (kwargs["user"], kwargs["group"], kwargs["extra_groups"]) == ("ghrunner", "ghrunner", [])
    assert kwargs["check"] is True


def test_der_starter_startet_genau_einen_runner_als_ghrunner_auf_dem_rootless_docker(
    tmp_path: Path, distribution: Path
) -> None:
    popen, gechownt = FalschesPopen(), list[Path]()
    slot_dir = tmp_path / "state" / "slots" / "swarmq-1"

    _starter(distribution, popen, gechownt).start(slot_dir, "JIT-KONFIG")

    kopie = slot_dir / "runner"
    assert (kopie / "run.sh").read_text() == "#!/bin/sh\n"
    assert (kopie / "externals" / "node20").is_dir()
    assert gechownt == [slot_dir]
    [(args, kwargs)] = popen.aufrufe
    assert args == [str(kopie / "run.sh")]
    # Die JIT-Konfiguration ist ein Zugangsnachweis: In der Kommandozeile könnte
    # jeder Benutzer auf celsius sie über /proc/<pid>/cmdline lesen, die Umgebung
    # sehen nur ghrunner und root. Der Runner löscht die Variable nach dem Lesen.
    assert kwargs["env"]["ACTIONS_RUNNER_INPUT_JITCONFIG"] == "JIT-KONFIG"
    assert kwargs["user"] == "ghrunner"
    assert kwargs["group"] == "ghrunner"
    assert kwargs["cwd"] == kopie
    assert kwargs["env"]["DOCKER_HOST"] == "unix:///run/user/1500/docker.sock"
    assert kwargs["env"]["HOME"] == "/home/ghrunner"
    # Eigene Prozessgruppe: stop() erreicht auch den Listener unter run.sh.
    assert kwargs["start_new_session"] is True


def test_der_runner_erbt_keine_zusatzgruppen_des_root_supervisors(tmp_path: Path, distribution: Path) -> None:
    popen = FalschesPopen()

    _starter(distribution, popen, []).start(tmp_path / "slot", "JIT")

    # Ohne extra_groups=[] ruft Popen kein setgroups(): Gruppe 0 von root bliebe.
    [(_, kwargs)] = popen.aufrufe
    assert kwargs["extra_groups"] == []


def test_stop_beendet_die_prozessgruppe_und_greift_nach_der_frist_hart_durch(
    tmp_path: Path, distribution: Path
) -> None:
    popen, signale = FalschesPopen(), list[Any]()
    popen.wait_ergebnisse = [subprocess.TimeoutExpired("run.sh", 30)]
    runner = _starter(distribution, popen, [], signale).start(tmp_path / "slot", "JIT")

    assert runner.poll() is None
    runner.stop()

    assert signale == [(4711, signal.SIGTERM), (4711, signal.SIGKILL)]


def test_reste_eines_abgebrochenen_laufs_werden_vor_der_kopie_entfernt(tmp_path: Path, distribution: Path) -> None:
    slot_dir = tmp_path / "slot"
    (slot_dir / "runner" / "_work").mkdir(parents=True)
    (slot_dir / "runner" / "_work" / "alt").write_text("Rest des letzten Jobs")

    _starter(distribution, FalschesPopen(), []).start(slot_dir, "JIT")

    assert not (slot_dir / "runner" / "_work" / "alt").exists()
    assert (slot_dir / "runner" / "run.sh").exists()


def test_ohne_eigenes_chown_gibt_root_nur_das_slot_verzeichnis_an_ghrunner(
    tmp_path: Path, distribution: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    besitz: dict[Path, tuple[str, str]] = {}
    monkeypatch.setattr(gr.shutil, "chown", lambda pfad, user, group: besitz.__setitem__(Path(pfad), (user, group)))
    starter = gr.HostLauncher(
        dist_dir=distribution,
        user="ghrunner",
        home=Path("/home/ghrunner"),
        docker_host="unix:///run/user/1500/docker.sock",
        popen=FalschesPopen(),
        run=FalschesKopieren(),
    )

    starter.start(tmp_path / "slot", "JIT")

    assert besitz == {tmp_path / "slot": ("ghrunner", "ghrunner")}


class FalschesHttp:
    """HTTP-Grenze: Antworten der Reihe nach, jede Anfrage protokolliert."""

    def __init__(self, *antworten: Any) -> None:
        self.antworten = list(antworten)
        self.anfragen: list[dict[str, Any]] = []

    def __call__(self, method: str, url: str, headers: dict[str, str], body: bytes | None) -> Any:
        self.anfragen.append({"method": method, "url": url, "headers": headers, "body": body})
        return self.antworten.pop(0)


def _api(http: FalschesHttp) -> Any:
    return gr.GitHubApi(http=http, token=TOKEN, owner="Schorschofwill")


def test_die_jit_konfiguration_kommt_per_post_mit_gruppe_1(state_dir: Path) -> None:
    antwort = json.dumps({"runner": {"id": 23, "name": "x"}, "encoded_jit_config": "ENC"}).encode()
    http = FalschesHttp(gr.Response(status=201, headers={}, body=antwort))

    jit = _api(http).jit_config("swarmq", "celsius-swarmq-1-ab", ["self-hosted", "celsius-swarmq"], "_work")

    assert jit == gr.JitConfig(runner_id=23, encoded="ENC")
    [anfrage] = http.anfragen
    assert anfrage["method"] == "POST"
    assert anfrage["url"] == "https://api.github.com/repos/Schorschofwill/swarmq/actions/runners/generate-jitconfig"
    assert anfrage["headers"]["Authorization"] == f"Bearer {TOKEN}"
    assert json.loads(anfrage["body"]) == {
        "name": "celsius-swarmq-1-ab",
        "runner_group_id": 1,
        "labels": ["self-hosted", "celsius-swarmq"],
        "work_folder": "_work",
    }


def test_ein_fehlerstatus_der_api_wird_zum_api_fehler_ohne_pat() -> None:
    http = FalschesHttp(gr.Response(status=403, headers={}, body=b'{"message": "Resource not accessible"}'))

    with pytest.raises(gr.ApiError) as fehler:
        _api(http).jit_config("swarmq", "n", [], "_work")

    assert "403" in str(fehler.value)
    assert "Resource not accessible" in str(fehler.value)
    assert TOKEN not in str(fehler.value)


@pytest.mark.parametrize(("status", "erwartet"), [(204, True), (422, False)])
def test_delete_runner_meldet_geloescht_oder_beschaeftigt(status: int, erwartet: bool) -> None:
    http = FalschesHttp(gr.Response(status=status, headers={}, body=b""))

    assert _api(http).delete_runner("swarmv", 23) is erwartet
    [anfrage] = http.anfragen
    assert anfrage["method"] == "DELETE"
    assert anfrage["url"] == "https://api.github.com/repos/Schorschofwill/swarmv/actions/runners/23"


def test_delete_runner_mit_anderem_status_ist_ein_api_fehler() -> None:
    http = FalschesHttp(gr.Response(status=500, headers={}, body=b""))

    with pytest.raises(gr.ApiError):
        _api(http).delete_runner("swarmv", 23)


class FalscheUrlAntwort:
    def __init__(self, status: int, headers: dict[str, str], body: bytes) -> None:
        self.status = status
        self.headers = headers
        self._body = body

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_urllib_http_liefert_status_header_und_rumpf() -> None:
    gesendet: list[Any] = []

    def opener(request: Any, timeout: float) -> FalscheUrlAntwort:
        gesendet.append(request)
        return FalscheUrlAntwort(201, {"github-authentication-token-expiration": "2026-12-01 10:00:00 UTC"}, b"{}")

    antwort = gr.UrllibHttp(opener=opener)("POST", "https://api.github.com/x", {"A": "b"}, b"{}")

    assert antwort.status == 201
    assert antwort.body == b"{}"
    assert antwort.headers["github-authentication-token-expiration"] == "2026-12-01 10:00:00 UTC"
    [anfrage] = gesendet
    assert anfrage.get_method() == "POST"
    assert anfrage.data == b"{}"


def test_urllib_http_reicht_einen_4xx_status_als_antwort_durch() -> None:
    kopf = Message()
    kopf["Content-Type"] = "application/json"

    def opener(request: Any, timeout: float) -> Any:
        raise urllib.error.HTTPError(request.full_url, 422, "Unprocessable", kopf, io.BytesIO(b'{"message":"busy"}'))

    antwort = gr.UrllibHttp(opener=opener)("DELETE", "https://api.github.com/x", {}, None)

    assert antwort.status == 422
    assert antwort.body == b'{"message":"busy"}'
    assert antwort.headers["content-type"] == "application/json"


@pytest.mark.parametrize(
    "fehler",
    [
        urllib.error.URLError(OSError("Network is unreachable")),
        TimeoutError("timed out"),
        # Abgerissene Antwort: http.client meldet sie nicht als OSError.
        http.client.IncompleteRead(b"{", 10),
    ],
)
def test_urllib_http_macht_aus_netzfehlern_einen_api_fehler(fehler: Exception) -> None:
    def opener(request: Any, timeout: float) -> Any:
        raise fehler

    with pytest.raises(gr.ApiError):
        gr.UrllibHttp(opener=opener)("GET", "https://api.github.com/x", {}, None)


def _zustaende(state_dir: Path, **slots: str) -> None:
    (state_dir / "slots").mkdir(parents=True, exist_ok=True)
    for slot, wort in slots.items():
        (state_dir / "slots" / f"{slot.replace('_', '-')}.state").write_text(f"{wort}\n")


def test_pause_kehrt_erst_zurueck_wenn_jeder_slot_pausiert_meldet(
    state_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _zustaende(state_dir, swarmq_1="laeuft", swarmq_2="pausiert", swarmv_1="holt")
    uhr = FalscheUhr()
    uhr.bei_schlaf = [
        lambda: _zustaende(state_dir, swarmv_1="pausiert"),
        lambda: None,
        lambda: _zustaende(state_dir, swarmq_1="pausiert"),
    ]

    assert gr.pause(gr.Switch(state_dir), uhr) == 0

    assert (state_dir / "pause").exists()
    assert len(uhr.geschlafen) == 3
    assert "pausiert" in capsys.readouterr().out


def test_pause_setzt_den_schalter_unter_der_sperre(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    echt = fcntl.flock
    beobachtet: list[tuple[int, bool]] = []

    def flock(datei: Any, op: int) -> None:
        beobachtet.append((op, (state_dir / "pause").exists()))
        echt(datei, op)

    monkeypatch.setattr(gr.fcntl, "flock", flock)

    gr.pause(gr.Switch(state_dir), FalscheUhr())

    assert beobachtet == [(fcntl.LOCK_EX, False), (fcntl.LOCK_UN, True)]


def test_pause_bei_gesetztem_schalter_aendert_nichts_und_meldet_schon_pausiert(
    state_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _zustaende(state_dir, swarmq_1="pausiert")
    (state_dir / "pause").write_text("von U7")

    assert gr.pause(gr.Switch(state_dir), FalscheUhr()) == 0

    assert (state_dir / "pause").read_text() == "von U7"
    assert "schon pausiert" in capsys.readouterr().out


def test_pause_mit_timeout_gibt_auf_und_nennt_den_beschaeftigten_slot(
    state_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _zustaende(state_dir, swarmq_1="laeuft", swarmq_2="pausiert")
    uhr = FalscheUhr()

    assert gr.pause(gr.Switch(state_dir), uhr, timeout=10.0) == 1

    assert sum(uhr.geschlafen) <= 10.0
    assert (state_dir / "pause").exists()
    ausgabe = capsys.readouterr().out
    assert "swarmq-1" in ausgabe
    assert "swarmq-2" not in ausgabe


def test_resume_entfernt_den_schalter_und_ist_ohne_schalter_ein_no_op(
    state_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    switch = gr.Switch(state_dir)
    switch.ensure()
    (state_dir / "pause").touch()

    assert gr.resume(switch) == 0
    assert not (state_dir / "pause").exists()

    assert gr.resume(switch) == 0
    assert not (state_dir / "pause").exists()
    assert "nicht pausiert" in capsys.readouterr().out


HEUTE = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)


def _status(state_dir: Path, release: Any, version: str = "2.329.0") -> int:
    github = FalschesGitHub()
    github.release = release
    return int(gr.status(gr.Switch(state_dir), github, pinned_version=version, now=HEUTE))


def test_status_zeigt_schalter_und_slots(state_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _zustaende(state_dir, swarmq_1="laeuft", swarmv_1="pausiert")
    (state_dir / "pause").touch()

    assert _status(state_dir, gr.Release(version="2.329.0", token_expiration="2027-01-01 00:00:00 UTC")) == 0

    ausgabe = capsys.readouterr().out
    assert "Schalter: gesetzt" in ausgabe
    assert "swarmq-1: laeuft" in ausgabe
    assert "swarmv-1: pausiert" in ausgabe
    assert "WARNUNG" not in ausgabe


@pytest.mark.parametrize(
    ("gepinnt", "warnt"), [("2.329.0", True), ("2.99.0", True), ("2.330.0", False), ("2.331.0", False)]
)
def test_status_warnt_bei_veraltetem_runner_release(
    state_dir: Path, capsys: pytest.CaptureFixture[str], gepinnt: str, warnt: bool
) -> None:
    _status(state_dir, gr.Release(version="2.330.0", token_expiration="2027-01-01 00:00:00 UTC"), version=gepinnt)

    ausgabe = capsys.readouterr().out
    assert f"Runner: {gepinnt}" in ausgabe
    assert ("WARNUNG: neueres Runner-Release 2.330.0" in ausgabe) is warnt


@pytest.mark.parametrize(
    ("ablauf", "warnt"),
    [
        ("2026-11-11 12:00:00 UTC", False),  # genau 14 Tage
        ("2026-11-11 11:59:59 UTC", True),
        ("2026-10-30 00:00:00 UTC", True),
        ("2027-10-01 00:00:00 UTC", False),
    ],
)
def test_status_warnt_bei_einem_pat_das_in_weniger_als_14_tagen_ablaeuft(
    state_dir: Path, capsys: pytest.CaptureFixture[str], ablauf: str, warnt: bool
) -> None:
    _status(state_dir, gr.Release(version="2.329.0", token_expiration=ablauf))

    ausgabe = capsys.readouterr().out
    assert f"PAT läuft ab: {ablauf}" in ausgabe
    assert ("WARNUNG: PAT läuft in weniger als 14 Tagen ab" in ausgabe) is warnt


def test_status_warnt_wenn_die_api_kein_ablaufdatum_meldet(state_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _status(state_dir, gr.Release(version="2.329.0", token_expiration=None))

    assert "WARNUNG: kein Ablaufdatum des PAT gemeldet" in capsys.readouterr().out


def test_status_meldet_einen_api_fehler_statt_abzustuerzen(state_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _zustaende(state_dir, swarmq_1="holt")

    assert _status(state_dir, gr.ApiError("HTTP 401: Bad credentials")) == 1

    ausgabe = capsys.readouterr().out
    assert "swarmq-1: holt" in ausgabe
    assert "WARNUNG: GitHub nicht erreichbar (HTTP 401: Bad credentials)" in ausgabe


def test_latest_runner_release_liest_version_und_pat_ablauf_aus_einer_antwort() -> None:
    http = FalschesHttp(
        gr.Response(
            status=200,
            headers={"github-authentication-token-expiration": "2026-12-01 10:00:00 UTC"},
            body=b'{"tag_name": "v2.330.0"}',
        )
    )

    release = _api(http).latest_runner_release()

    assert release == gr.Release(version="2.330.0", token_expiration="2026-12-01 10:00:00 UTC")
    [anfrage] = http.anfragen
    assert anfrage["method"] == "GET"
    assert anfrage["url"] == "https://api.github.com/repos/actions/runner/releases/latest"


def test_die_konfiguration_kommt_aus_der_umgebung_mit_vorgaben(tmp_path: Path) -> None:
    config = gr.Config.from_env(
        {
            "GH_RUNNER_DIST": "/opt/github-runner/dist/2.329.0",
            "GH_RUNNER_DOCKER_HOST": "unix:///run/user/1500/docker.sock",
        }
    )

    assert config.state_dir == Path("/var/lib/github-runner")
    assert config.token_file == Path("/etc/github-runner/token")
    assert config.owner == "Schorschofwill"
    assert config.user == "ghrunner"
    assert config.home == Path("/home/ghrunner")
    assert config.dist_dir == Path("/opt/github-runner/dist/2.329.0")
    # Die Version der gepinnten Distribution ist der Name ihres Verzeichnisses (README).
    assert config.runner_version == "2.329.0"
    assert config.docker_host == "unix:///run/user/1500/docker.sock"

    eigene = gr.Config.from_env(
        {
            "GH_RUNNER_STATE_DIR": str(tmp_path),
            "GH_RUNNER_TOKEN_FILE": "/root/pat",
            "GH_RUNNER_OWNER": "jemand",
            "GH_RUNNER_USER": "ci",
            "GH_RUNNER_HOME": "/srv/ci",
        }
    )
    assert (eigene.state_dir, eigene.token_file, eigene.owner, eigene.user, eigene.home) == (
        tmp_path,
        Path("/root/pat"),
        "jemand",
        "ci",
        Path("/srv/ci"),
    )


def test_main_pausiert_und_setzt_fort_im_zustandsverzeichnis_aus_der_umgebung(state_dir: Path) -> None:
    env = {"GH_RUNNER_STATE_DIR": str(state_dir)}
    # Alle Slots ruhen schon: pause kehrt ohne Schlafen zurück.
    _zustaende(state_dir, swarmq_1="pausiert")

    assert gr.main(["pause", "--timeout", "5"], env) == 0
    assert (state_dir / "pause").exists()

    assert gr.main(["resume"], env) == 0
    assert not (state_dir / "pause").exists()


@pytest.mark.parametrize(
    ("argv", "fehlt"),
    [
        (["status"], "GH_RUNNER_DIST"),
        (["slot", "swarmq", "1"], "GH_RUNNER_DIST"),
        (["slot", "swarmq", "1"], "GH_RUNNER_DOCKER_HOST"),
        (["status"], "PAT-Datei"),
    ],
)
def test_main_nennt_fehlende_einstellungen_statt_abzustuerzen(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], argv: list[str], fehlt: str
) -> None:
    env = {
        "GH_RUNNER_STATE_DIR": str(tmp_path / "state"),
        "GH_RUNNER_TOKEN_FILE": str(tmp_path / "fehlt"),
        "GH_RUNNER_DIST": str(tmp_path / "2.329.0"),
        "GH_RUNNER_DOCKER_HOST": "unix:///run/user/1500/docker.sock",
    }
    if fehlt.startswith("GH_"):
        del env[fehlt]

    assert gr.main(argv, env) == 2

    assert fehlt in capsys.readouterr().err


def test_das_pat_erreicht_weder_ausgabe_noch_argumente_noch_umgebung_des_runners(
    tmp_path: Path,
    state_dir: Path,
    distribution: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pat_datei = tmp_path / "token"
    pat_datei.write_text(TOKEN + "\n")
    # So sähe die Umgebung des Supervisors unter systemd aus.
    monkeypatch.setenv("GH_RUNNER_TOKEN_FILE", str(pat_datei))
    monkeypatch.setenv("GITHUB_TOKEN", TOKEN)
    jit = json.dumps({"runner": {"id": 7}, "encoded_jit_config": "ENC"}).encode()
    http = FalschesHttp(
        gr.Response(status=401, headers={}, body=b'{"message": "Bad credentials"}'),
        gr.Response(status=201, headers={}, body=jit),
    )
    popen = FalschesPopen()
    popen.ergebnis = 0
    slot = gr.Slot(
        repo="swarmq",
        number=1,
        switch=gr.Switch(state_dir),
        github=gr.GitHubApi(http=http, token=pat_datei.read_text().strip(), owner="Schorschofwill"),
        launcher=_starter(distribution, popen, []),
        clock=FalscheUhr(),
    )

    slot.run_once()
    slot.run_once()

    [(args, kwargs)] = popen.aufrufe
    assert kwargs["env"]["ACTIONS_RUNNER_INPUT_JITCONFIG"] == "ENC"
    sichtbar = " ".join([*args, *kwargs["env"].keys(), *kwargs["env"].values(), *capsys.readouterr()])
    assert TOKEN not in sichtbar
    assert str(pat_datei) not in sichtbar
    assert set(kwargs["env"]) == {"DOCKER_HOST", "HOME", "USER", "PATH", "ACTIONS_RUNNER_INPUT_JITCONFIG"}


def test_leere_werte_aus_der_umgebungsdatei_gelten_als_nicht_gesetzt() -> None:
    namen = ["STATE_DIR", "TOKEN_FILE", "OWNER", "USER", "HOME", "DIST", "DOCKER_HOST"]

    config = gr.Config.from_env({f"GH_RUNNER_{name}": "" for name in namen})

    assert config == gr.Config.from_env({})
    assert config.state_dir == Path("/var/lib/github-runner")
    assert config.docker_host is None
