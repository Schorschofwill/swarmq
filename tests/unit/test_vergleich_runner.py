"""Runner und Adapter des Vergleichsaufbaus (Plan 2026-10-24-1118, KTD13, R19-R22).

Die Module unter ``benchmarks/comparison/`` importieren sich gegenseitig über
das Verzeichnis selbst (``from scenarios.base import ...``), wie beim Lauf
von ``runner.py``. Die Fixture stellt das Verzeichnis für einen Test vorn in
``sys.path`` und räumt die geladenen Module danach wieder ab. Keine Dienste,
keine der verglichenen Bibliotheken: Die Adapter laden sie erst in
``setup()``, und die PID-Tests starten statt der CLI einen schlafenden
Python-Prozess.

Test-Liste:
- [x] die Reihenfolge ist reihum über die Systeme, nicht blockweise
- [x] eine Ergebnisdatei ohne Versionen, Images, Host, Profil oder Parallelität wird abgelehnt
- [x] die Metadaten eines Laufs nennen Versionen, Image-Tags, Host, Profil und Parallelität
- [x] ``benchmarks/docker-compose.yml`` und der Vergleichsaufbau nutzen keinen Testport
- [x] die Images des Vergleichsaufbaus sind exakt gepinnt
- [x] jede Abhängigkeit der Vergleichsumgebungen ist mit ``==`` gepinnt
- [x] ein Worker-Prozess läuft mit eigener PID und endet mit ``stoppen()``
- [x] jeder Adapter liefert nach dem Start die PID eines laufenden Worker-Prozesses, die nicht die des Runners ist
- [x] jeder Adapter startet den Worker über die CLI seines Systems
- [x] der Hänger-Wächter beendet eine Kombination nach dem Zeitlimit und meldet sie
- [x] der Bericht trennt die Profile eines Systems
- [x] der Bericht lässt Ergebnisse ohne Metadaten weg
- [x] ``save_result`` schreibt die Metadaten mit und verweigert ein Ergebnis ohne sie
- [x] ``beende_alle`` tötet jeden laufenden Worker (für den SIGTERM-Handler des Kombinationsprozesses)
- [x] mit Parallelität ``gleich`` führt jeder Adapter 10 Tasks gleichzeitig aus, in einem Prozess, wo das System es erlaubt
- [x] mit Parallelität ``ab_werk`` (Standard) setzt kein Adapter etwas daran
- [x] die Kombinationen laufen in jeder gewählten Parallelität, und der Kindprozess erfährt sie
- [x] Metadaten und Ergebnisdatei nennen die Parallelität; ohne sie wird ein Ergebnis abgelehnt
- [x] der Bericht trennt die Parallelitäten eines Systems
- [x] die Entscheidungsregeln des Berichts vergleichen bei gleicher Parallelität
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import json
import os
import re
import signal
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType

import pytest

BENCHMARKS_DIR = Path(__file__).resolve().parents[2] / "benchmarks"
VERGLEICH_DIR = BENCHMARKS_DIR / "comparison"

# Steht statt der CLI eines Systems: ein Python-Prozess, der nur wartet.
SCHLAFENDER_WORKER = [sys.executable, "-c", "import time; time.sleep(30)"]

# Standardports, Testports der Suiten und Chaos-Ports (AGENTS.md, tests/chaos/).
BELEGTE_PORTS = frozenset({5672, 15672, 6379, 5699, 15699, 6399, 5698, 6398})

# Oberste Modulnamen unter ``benchmarks/comparison``; nach jedem Test entladen,
# damit kein anderer Test sie über ``sys.modules`` sieht.
_EIGENE_MODULE = frozenset(
    {"garantieprofile", "worker_prozess", "runner", "report", "scenarios", "systems", "workers"},
)


def _entladen() -> None:
    for name in list(sys.modules):
        if name.split(".")[0] in _EIGENE_MODULE:
            del sys.modules[name]


@pytest.fixture
def vergleich(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str], ModuleType]]:
    """Importiert Module aus ``benchmarks/comparison`` wie ``runner.py`` beim Lauf."""
    _entladen()
    monkeypatch.syspath_prepend(str(VERGLEICH_DIR))
    yield importlib.import_module
    _entladen()


def test_reihenfolge_ist_reihum_ueber_die_systeme(vergleich: Callable[[str], ModuleType]) -> None:
    runner = vergleich("runner")

    plan = runner.reihenfolge(systeme=["a", "b", "c"], szenarien=["noop"], profile=["stark"], runs=2)

    assert [k.system for k in plan] == ["a", "b", "c", "a", "b", "c"]


def test_jede_kombination_laeuft_in_jeder_gewaehlten_parallelitaet_und_das_kind_erfaehrt_sie(
    vergleich: Callable[[str], ModuleType],
    tmp_path: Path,
) -> None:
    runner = vergleich("runner")

    plan = runner.reihenfolge(
        systeme=["a", "b"], szenarien=["noop"], profile=["stark"], runs=1, parallelitaeten=["gleich", "ab_werk"]
    )

    assert [(k.parallelitaet, k.system) for k in plan] == [
        ("gleich", "a"),
        ("gleich", "b"),
        ("ab_werk", "a"),
        ("ab_werk", "b"),
    ]
    kommando = runner.kind_kommando(plan[0], tmp_path)
    assert kommando[kommando.index("--parallelitaet") + 1] == "gleich"


def _vollstaendiges_ergebnis() -> dict[str, object]:
    return {
        "system_name": "celery",
        "scenario_name": "noop",
        "params": {"n": 10},
        "throughput": 100.0,
        "profil": "stark",
        "parallelitaet": "gleich",
        "versionen": {"celery": "5.6.3", "kombu": "5.6.2"},
        "images": {"rabbitmq": "rabbitmq:4.3.6-management"},
        "host": {"name": "celsius"},
    }


@pytest.mark.parametrize("fehlt", ["versionen", "images", "host", "profil", "parallelitaet"])
def test_ergebnis_ohne_metadaten_wird_abgelehnt(
    vergleich: Callable[[str], ModuleType],
    tmp_path: Path,
    fehlt: str,
) -> None:
    base = vergleich("scenarios.base")
    ergebnis = _vollstaendiges_ergebnis()
    del ergebnis[fehlt]
    datei = tmp_path / "noop_celery.json"
    datei.write_text(json.dumps(ergebnis))

    with pytest.raises(base.UngueltigesErgebnis, match=fehlt):
        base.lade_ergebnis(datei)


def test_vollstaendiges_ergebnis_wird_geladen(vergleich: Callable[[str], ModuleType], tmp_path: Path) -> None:
    base = vergleich("scenarios.base")
    datei = tmp_path / "noop_celery.json"
    datei.write_text(json.dumps(_vollstaendiges_ergebnis()))

    assert base.lade_ergebnis(datei) == _vollstaendiges_ergebnis()


def test_metadaten_nennen_versionen_images_host_profil_und_parallelitaet(
    vergleich: Callable[[str], ModuleType],
) -> None:
    """swarmq ist in der Testumgebung installiert; die anderen Systeme nur in ihren Vergleichsumgebungen."""
    runner = vergleich("runner")

    meta = runner.metadaten("swarmq", "stark", "gleich")

    assert meta["profil"] == "stark"
    assert meta["parallelitaet"] == "gleich"
    assert meta["versionen"]["swarmq"] == importlib.metadata.version("swarmq")
    assert meta["versionen"]["aio-pika"] == importlib.metadata.version("aio-pika")
    assert meta["images"] == {"rabbitmq": "rabbitmq:4.3.6-management", "valkey": "valkey/valkey:8.1.10-alpine"}
    assert meta["host"]["name"]
    assert meta["host"]["python"] == sys.version.split()[0]


@pytest.mark.parametrize("compose", ["docker-compose.yml", "comparison/docker-compose.yml"])
def test_benchmark_compose_nutzt_keinen_test_oder_standardport(compose: str) -> None:
    """Gelesen aus der Datei: Ein Benchmark neben einer Testsitzung darf deren Dienste nicht treffen."""
    text = (BENCHMARKS_DIR / compose).read_text()
    host_ports = {int(port) for port in re.findall(r'^\s*-\s*"(\d+):\d+"', text, flags=re.MULTILINE)}

    assert host_ports
    assert not host_ports & BELEGTE_PORTS


def test_images_des_vergleichs_sind_exakt_gepinnt() -> None:
    """Ein Tag wie ``4-management-alpine`` wandert mit; nachstellbar ist nur Major.Minor.Patch (R22)."""
    images = re.findall(r"^\s*image:\s*(\S+)", (VERGLEICH_DIR / "docker-compose.yml").read_text(), flags=re.MULTILINE)

    assert images
    for image in images:
        assert re.search(r":\d+\.\d+\.\d+(-[a-z]+)*$", image), image


@pytest.mark.parametrize("system", ["swarmq", "celery", "dramatiq", "arq", "taskiq"])
def test_jede_abhaengigkeit_der_vergleichsumgebung_ist_exakt_gepinnt(system: str) -> None:
    """Je System eine eigene Umgebung (arq verlangt redis<6, taskiq-redis redis>=8).

    Erlaubt sind nur ``-r`` auf die gemeinsame Datei, ``==``-Pins und für
    swarmq der Stand dieses Repos (``-e ../..``), der ja gemessen wird.
    """

    def zeilen_aus(datei: str) -> list[str]:
        text = (VERGLEICH_DIR / datei).read_text()
        return [zeile.strip() for zeile in text.splitlines() if zeile.strip() and not zeile.lstrip().startswith("#")]

    zeilen = zeilen_aus(f"requirements-{system}.txt")
    pins = [zeile for zeile in zeilen + zeilen_aus("requirements.txt") if not zeile.startswith(("-r ", "-e "))]

    assert "-r requirements.txt" in zeilen
    assert all(zeile == "-e ../.." for zeile in zeilen if zeile.startswith("-e ")), zeilen
    assert any(pin.split("==")[0].split("[")[0] == system for pin in pins) or system == "swarmq"
    for pin in pins:
        assert re.fullmatch(r"[A-Za-z0-9_.-]+(\[[a-z,]+\])?==\d+(\.\d+)*(\.post\d+)?", pin), pin


def _laeuft(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.timeout(5)
async def test_worker_prozess_laeuft_mit_eigener_pid_und_endet_mit_stoppen(
    vergleich: Callable[[str], ModuleType],
) -> None:
    worker_prozess = vergleich("worker_prozess")
    worker = worker_prozess.WorkerProzess(SCHLAFENDER_WORKER, umgebung={}, verzeichnis=VERGLEICH_DIR)

    pid = await worker.starten()

    assert pid == worker.pid
    assert pid != os.getpid()
    assert _laeuft(pid)
    await worker.stoppen()
    assert not _laeuft(pid)


SYSTEME = ("swarmq", "celery", "dramatiq", "arq", "taskiq")


@pytest.mark.timeout(5)
@pytest.mark.parametrize("system", SYSTEME)
async def test_adapter_liefert_die_pid_seines_laufenden_workers(
    vergleich: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    system: str,
) -> None:
    """Der Worker ist ein eigener Prozess: Ein SIGKILL an ihn trifft den Runner nicht (KTD13)."""
    adapter_cls = vergleich("runner").get_systems()[system]
    adapter = adapter_cls(profil="stark")
    monkeypatch.setattr(adapter, "worker_kommando", lambda: SCHLAFENDER_WORKER)

    await adapter.start_worker()
    pid = adapter.worker_pid
    try:
        assert pid != os.getpid()
        assert _laeuft(pid)
    finally:
        await adapter.stop_worker()
    assert not _laeuft(pid)


@pytest.mark.parametrize(
    ("system", "erwartet"),
    [
        ("swarmq", ["swarmq", "worker", "--module", "workers.swarmq_cli"]),
        ("celery", ["celery", "-A", "workers.celery_worker", "worker"]),
        ("dramatiq", ["dramatiq", "workers.dramatiq_worker"]),
        ("arq", ["arq", "workers.arq_worker.WorkerSettings"]),
        ("taskiq", ["taskiq", "worker", "workers.taskiq_rmq_worker:broker", "--ack-type", "when_saved"]),
    ],
)
def test_adapter_startet_den_worker_ueber_die_cli_seines_systems(
    vergleich: Callable[[str], ModuleType],
    system: str,
    erwartet: list[str],
) -> None:
    """Die CLI liegt neben dem Python der Vergleichsumgebung, in der der Runner läuft."""
    adapter = vergleich("runner").get_systems()[system](profil="stark")

    kommando = adapter.worker_kommando()

    assert Path(kommando[0]) == Path(sys.executable).with_name(erwartet[0])
    assert kommando[1 : len(erwartet)] == erwartet[1:]


@pytest.mark.parametrize(
    ("system", "argumente", "umgebung"),
    [
        ("swarmq", ["--processes", "1", "--concurrency", "10"], {}),
        ("celery", ["--concurrency", "10"], {}),
        ("dramatiq", ["--processes", "1", "--threads", "10"], {}),
        ("arq", [], {"VERGLEICH_MAX_JOBS": "10"}),
        ("taskiq", ["--workers", "1", "--max-async-tasks", "10"], {}),
    ],
)
def test_adapter_mit_gleicher_parallelitaet_fuehrt_zehn_tasks_gleichzeitig_aus(
    vergleich: Callable[[str], ModuleType],
    system: str,
    argumente: list[str],
    umgebung: dict[str, str],
) -> None:
    """Ab Werk reichen die Systeme von 10 (swarmq, arq) bis 200 (taskiq) gleichzeitigen Tasks.

    Celery kennt im Prefork-Pool keinen Prozess mit mehreren Tasks; dort sind es zehn Kindprozesse.
    arq liest ``max_jobs`` aus ``WorkerSettings``, das der Worker aus der Umgebung setzt.
    """
    adapter = vergleich("runner").get_systems()[system](profil="stark", parallelitaet="gleich")
    ab_werk = vergleich("runner").get_systems()[system](profil="stark")
    ab_werk.queue = adapter.queue

    assert adapter.worker_kommando() == ab_werk.worker_kommando() + argumente
    assert adapter.worker_umgebung() == {**ab_werk.worker_umgebung(), **umgebung}


PARALLELITAETS_SCHALTER = frozenset({"--concurrency", "-c", "--processes", "-p", "--threads", "-t", "--workers", "-w"})


@pytest.mark.parametrize("system", SYSTEME)
def test_adapter_ab_werk_laesst_die_parallelitaet_des_systems_stehen(
    vergleich: Callable[[str], ModuleType],
    system: str,
) -> None:
    adapter = vergleich("runner").get_systems()[system](profil="stark")

    assert not PARALLELITAETS_SCHALTER & set(adapter.worker_kommando())
    assert not any(schalter.startswith("--max-async-tasks") for schalter in adapter.worker_kommando())
    assert "VERGLEICH_MAX_JOBS" not in adapter.worker_umgebung()


@pytest.mark.timeout(5)
async def test_haenger_waechter_beendet_die_kombination_nach_dem_zeitlimit(
    vergleich: Callable[[str], ModuleType],
) -> None:
    """Jede Kombination läuft in einem frischen Prozess; hängt er, geht der Vergleich weiter (KTD13)."""
    runner = vergleich("runner")
    kombination = runner.Kombination(run=1, szenario="noop", profil="stark", system="celery")

    ausgang = await runner.fuehre_aus(kombination, SCHLAFENDER_WORKER, zeitlimit=0.05, frist=1.0)

    assert ausgang.kombination == kombination
    assert ausgang.status == "haenger"
    assert not _laeuft(ausgang.pid)


@pytest.mark.timeout(5)
async def test_beende_alle_toetet_jeden_laufenden_worker(vergleich: Callable[[str], ModuleType]) -> None:
    """Der SIGTERM-Handler des Kombinationsprozesses ruft das auf, bevor er endet."""
    worker_prozess = vergleich("worker_prozess")
    worker = worker_prozess.WorkerProzess(SCHLAFENDER_WORKER, umgebung={}, verzeichnis=VERGLEICH_DIR)
    pid = await worker.starten()

    worker_prozess.beende_alle()

    assert await asyncio.wait_for(worker.warten(), 1.0) == -signal.SIGKILL
    assert not _laeuft(pid)


def test_bericht_trennt_die_profile_eines_systems(vergleich: Callable[[str], ModuleType]) -> None:
    """Sonst mittelt der Bericht ``ab_werk`` und ``stark`` zu einer Zahl."""
    report = vergleich("report")
    ab_werk = {**_vollstaendiges_ergebnis(), "profil": "ab_werk"}
    stark = _vollstaendiges_ergebnis()

    gruppiert = report.group_results([ab_werk, stark])

    assert gruppiert["noop"] == {"celery/ab_werk/gleich": [ab_werk], "celery/stark/gleich": [stark]}


def test_bericht_trennt_die_parallelitaeten_eines_systems(vergleich: Callable[[str], ModuleType]) -> None:
    report = vergleich("report")
    gleich = _vollstaendiges_ergebnis()
    ab_werk = {**_vollstaendiges_ergebnis(), "parallelitaet": "ab_werk"}

    gruppiert = report.group_results([gleich, ab_werk])

    assert gruppiert["noop"] == {"celery/stark/gleich": [gleich], "celery/stark/ab_werk": [ab_werk]}


def test_entscheidungsregeln_vergleichen_bei_gleicher_parallelitaet(
    vergleich: Callable[[str], ModuleType],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ab Werk verarbeitet taskiq 20-mal so viele Tasks gleichzeitig wie swarmq; das Verhältnis sagte dann wenig."""
    report = vergleich("report")
    swarmq = {**_vollstaendiges_ergebnis(), "system_name": "swarmq", "throughput": 50.0}
    taskiq = {**_vollstaendiges_ergebnis(), "system_name": "taskiq", "throughput": 200.0}
    taskiq_ab_werk = {**taskiq, "parallelitaet": "ab_werk", "throughput": 50.0}

    report.check_decision_rules(report.group_results([swarmq, taskiq, taskiq_ab_werk]))

    assert "Throughput ratio (SwarmQ/TaskIQ): 0.25" in capsys.readouterr().out


def test_bericht_laesst_ergebnisse_ohne_metadaten_weg(
    vergleich: Callable[[str], ModuleType],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = vergleich("report")
    gueltig = _vollstaendiges_ergebnis()
    ohne_versionen = _vollstaendiges_ergebnis()
    del ohne_versionen["versionen"]
    (tmp_path / "a.json").write_text(json.dumps(gueltig))
    (tmp_path / "b.json").write_text(json.dumps(ohne_versionen))

    assert report.load_results(tmp_path) == [gueltig]
    assert "b.json" in capsys.readouterr().err


def test_gespeichertes_ergebnis_traegt_die_metadaten(vergleich: Callable[[str], ModuleType], tmp_path: Path) -> None:
    base = vergleich("scenarios.base")
    meta = {key: _vollstaendiges_ergebnis()[key] for key in ("versionen", "images", "host", "profil", "parallelitaet")}
    result = base.make_result("celery", "noop", {"n": 1}, [0.01], 0.01)
    result.meta = meta

    gespeichert = base.lade_ergebnis(base.save_result(result, tmp_path))

    assert {key: gespeichert[key] for key in meta} == meta


def test_ergebnis_ohne_metadaten_wird_nicht_gespeichert(vergleich: Callable[[str], ModuleType], tmp_path: Path) -> None:
    base = vergleich("scenarios.base")
    result = base.make_result("celery", "noop", {"n": 1}, [0.01], 0.01)

    with pytest.raises(base.UngueltigesErgebnis):
        base.save_result(result, tmp_path)
    assert not list(tmp_path.iterdir())
