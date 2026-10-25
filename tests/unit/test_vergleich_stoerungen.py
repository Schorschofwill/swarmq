"""Ablauf des Garantie-Vergleichs unter Störungen (Plan 2026-10-24-1118, KTD13, KTD14, R21).

Geprüft wird die Logik ohne Dienste: Ruhephase, Ausgang eines Publishs,
Ziel jeder Störung, der eigene Compose-Stack und die Adressen, über die die
Adapter den Stack erreichen. Die Störungen selbst (SIGKILL, ``docker
restart``, Toxiproxy) prüft der lokale Lauf ``uv run just vergleich-garantie``.

Test-Liste:
- [x] die Ruhephase endet, wenn seit ``ruhe`` Sekunden keine Ausführung dazukam und kein bestätigter Publish offen ist
- [x] solange ein bestätigter Publish offen ist, läuft die Ruhephase weiter, bis zur Obergrenze
- [x] ein Fehler beim Publish ist abgelehnt
- [x] ``PublishIndeterminate`` von swarmq ist unbestimmt (E-042)
- [x] eine Zeitüberschreitung des Erzeugers ist unbestimmt
- [x] der Broker-Neustart trifft bei arq Valkey, sonst RabbitMQ
- [x] der Valkey-Neustart ohne Persistenz trifft das flüchtige Valkey, und die Systeme nutzen es in dieser Störung
- [x] die Adapter erreichen RabbitMQ und Valkey über ``VERGLEICH_RABBITMQ_URL`` und ``VERGLEICH_VALKEY_HOST``
- [x] ohne die Variablen bleiben die Adressen des Performance-Vergleichs
- [x] der Stack nutzt keinen Port der Testsuiten, der Chaos-Tests oder des Performance-Vergleichs
- [x] der Stack pinnt Toxiproxy auf die Version aus ``tests/chaos/compose.yml`` und alle Images exakt
- [x] das persistente Valkey schreibt ein AOF mit fsync je Befehl, das flüchtige speichert nichts
- [x] ein Worker-Prozess meldet, ob er noch läuft
- [x] ein Adapter meldet, ob sein Worker läuft
- [x] die Ergebnistabelle zeigt je Kombination alle Spalten der Bilanz, einen leeren oder gescheiterten Lauf ohne Zahlen
- [x] ein gescheiterter Lauf mit Bilanz erscheint in der Tabelle ohne Zahlen
- [x] ein gescheiterter Lauf behält seinen Status, auch wenn das Hauptbuch leer ist; nur ein Lauf ohne Fehler wird ``leer``
"""

from __future__ import annotations

import importlib
import os
import re
import signal
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType

import pytest

VERGLEICH_DIR = Path(__file__).resolve().parents[2] / "benchmarks" / "comparison"

_EIGENE_MODULE = frozenset(
    {"garantieprofile", "worker_prozess", "runner", "report", "scenarios", "systems", "workers", "stoerungen"},
)


def _entladen() -> None:
    for name in list(sys.modules):
        if name.split(".")[0] in _EIGENE_MODULE:
            del sys.modules[name]


@pytest.fixture
def vergleich(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str], ModuleType]]:
    """Importiert Module aus ``benchmarks/comparison`` wie beim Lauf."""
    _entladen()
    monkeypatch.syspath_prepend(str(VERGLEICH_DIR))
    yield importlib.import_module
    _entladen()


def test_the_quiet_phase_ends_after_ruhe_seconds_without_execution_when_nothing_is_open(
    vergleich: Callable[[str], ModuleType],
) -> None:
    lauf = vergleich("stoerungen.lauf")

    assert lauf.ruhe_beendet(still_s=20.0, offen=0, vergangen_s=25.0, ruhe_s=20.0, obergrenze_s=360.0)
    assert not lauf.ruhe_beendet(still_s=19.0, offen=0, vergangen_s=25.0, ruhe_s=20.0, obergrenze_s=360.0)


def test_the_quiet_phase_goes_on_while_a_confirmed_publish_is_open_up_to_the_limit(
    vergleich: Callable[[str], ModuleType],
) -> None:
    lauf = vergleich("stoerungen.lauf")

    assert not lauf.ruhe_beendet(still_s=60.0, offen=1, vergangen_s=60.0, ruhe_s=20.0, obergrenze_s=360.0)
    assert lauf.ruhe_beendet(still_s=60.0, offen=1, vergangen_s=360.0, ruhe_s=20.0, obergrenze_s=360.0)


def test_an_error_while_publishing_is_rejected(vergleich: Callable[[str], ModuleType]) -> None:
    lauf = vergleich("stoerungen.lauf")

    assert lauf.ausgang_von("celery", ConnectionRefusedError()) == "abgelehnt"


@pytest.mark.entscheidung("E-042")
def test_publish_indeterminate_from_swarmq_is_indeterminate(vergleich: Callable[[str], ModuleType]) -> None:
    from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate

    lauf = vergleich("stoerungen.lauf")

    assert lauf.ausgang_von("swarmq", PublishIndeterminate("confirm lost")) == "unbestimmt"
    assert lauf.ausgang_von("swarmq", BrokerConnectionError("not sent")) == "abgelehnt"


def test_a_producer_timeout_is_indeterminate(vergleich: Callable[[str], ModuleType]) -> None:
    lauf = vergleich("stoerungen.lauf")

    assert lauf.ausgang_von("dramatiq", lauf.PublishFrist()) == "unbestimmt"


def test_the_broker_restart_hits_valkey_for_arq_and_rabbitmq_otherwise(
    vergleich: Callable[[str], ModuleType],
) -> None:
    stack = vergleich("stoerungen.stack")

    assert stack.dienst("broker_neustart", "arq") == "valkey"
    assert {stack.dienst("broker_neustart", s) for s in ("swarmq", "celery", "dramatiq", "taskiq")} == {"rabbitmq"}


def test_the_valkey_restart_without_persistence_hits_the_volatile_valkey_that_the_systems_use(
    vergleich: Callable[[str], ModuleType],
) -> None:
    stack = vergleich("stoerungen.stack")

    fluechtig = stack.umgebung("valkey_neustart_fluechtig")["VERGLEICH_VALKEY_HOST"]
    persistent = stack.umgebung("valkey_neustart_persistent")["VERGLEICH_VALKEY_HOST"]

    assert stack.dienst("valkey_neustart_fluechtig", "celery") == "valkey-fluechtig"
    assert stack.dienst("valkey_neustart_persistent", "celery") == "valkey"
    assert fluechtig == f"localhost:{stack.PORTS['valkey-fluechtig']}"
    assert persistent == f"localhost:{stack.PORTS['valkey']}"
    assert stack.umgebung("sigkill")["VERGLEICH_VALKEY_HOST"] == persistent
    assert (
        stack.umgebung("sigkill")["VERGLEICH_RABBITMQ_URL"]
        == f"amqp://guest:guest@localhost:{stack.PORTS['rabbitmq']}/"
    )


def test_the_adapters_reach_the_stack_through_the_vergleich_variables(
    vergleich: Callable[[str], ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERGLEICH_RABBITMQ_URL", "amqp://guest:guest@localhost:5686/")
    monkeypatch.setenv("VERGLEICH_VALKEY_HOST", "localhost:6387")

    base = vergleich("systems.base")
    swarmq = vergleich("systems.swarmq_sys").SwarmQSystem(profil="stark")

    assert base.RABBITMQ_URL == "amqp://guest:guest@localhost:5686/"
    assert swarmq.backend_url == "valkey://localhost:6387/0"
    assert swarmq.worker_umgebung()["SWARMQ_BROKER_URL"] == "amqp://guest:guest@localhost:5686/"


def test_without_the_variables_the_addresses_of_the_performance_comparison_stay(
    vergleich: Callable[[str], ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("VERGLEICH_RABBITMQ_URL", raising=False)
    monkeypatch.delenv("VERGLEICH_VALKEY_HOST", raising=False)

    base = vergleich("systems.base")

    assert base.RABBITMQ_URL == "amqp://guest:guest@localhost:5682/"
    assert base.VALKEY_HOST == "localhost:6389"


# Standard-, Test-, Chaos- und Vergleichsports samt Management (AGENTS.md, tests/chaos/).
BELEGTE_PORTS = frozenset({5672, 15672, 6379, 5699, 15699, 6399, 5698, 6398, 5682, 15682, 6389, 8474})


def _compose() -> str:
    return (VERGLEICH_DIR / "stoerungen" / "compose.yml").read_text(encoding="utf-8")


def test_the_stack_uses_no_port_of_the_test_suites_chaos_tests_or_performance_comparison(
    vergleich: Callable[[str], ModuleType],
) -> None:
    stack = vergleich("stoerungen.stack")
    host_ports = {int(port) for port in re.findall(r'^\s*-\s*"(\d+):\d+"', _compose(), flags=re.MULTILINE)}

    assert set(stack.PORTS.values()) | {stack.ADMIN_PORT} == host_ports
    assert not host_ports & BELEGTE_PORTS


def test_the_stack_pins_toxiproxy_like_the_chaos_tests_and_every_image_exactly() -> None:
    chaos = (VERGLEICH_DIR.parents[1] / "tests" / "chaos" / "compose.yml").read_text(encoding="utf-8")
    [toxiproxy_chaos] = re.findall(r"image:\s*(\S*toxiproxy\S*)", chaos)
    images = re.findall(r"^\s*image:\s*(\S+)", _compose(), flags=re.MULTILINE)

    assert toxiproxy_chaos in images
    for image in images:
        assert re.search(r":\d+\.\d+\.\d+(-[a-z]+)*$", image), image


def test_the_persistent_valkey_writes_an_aof_synced_per_command_and_the_volatile_one_keeps_nothing() -> None:
    befehle = dict(re.findall(r"^  ([a-z-]+):\n(?:    .*\n)*?    command: (\[.*\])$", _compose(), flags=re.MULTILINE))

    assert '"--appendonly", "yes", "--appendfsync", "always"' in befehle["valkey"]
    assert '"--appendonly", "no", "--save", ""' in befehle["valkey-fluechtig"]


@pytest.mark.timeout(5)
async def test_a_worker_process_reports_whether_it_still_runs(vergleich: Callable[[str], ModuleType]) -> None:
    worker_prozess = vergleich("worker_prozess")
    worker = worker_prozess.WorkerProzess(
        [sys.executable, "-c", "import time; time.sleep(30)"], umgebung={}, verzeichnis=VERGLEICH_DIR
    )
    assert not worker.laeuft

    pid = await worker.starten()
    assert worker.laeuft

    os.killpg(pid, signal.SIGKILL)
    await worker.warten()
    assert not worker.laeuft
    await worker.stoppen()


@pytest.mark.timeout(5)
async def test_an_adapter_reports_whether_its_worker_runs(
    vergleich: Callable[[str], ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = vergleich("systems.celery_sys").CelerySystem(profil="stark")
    monkeypatch.setattr(adapter, "worker_kommando", lambda: [sys.executable, "-c", "import time; time.sleep(30)"])
    assert not adapter.worker_laeuft

    await adapter.start_worker()
    try:
        assert adapter.worker_laeuft
    finally:
        await adapter.stop_worker()


def test_the_result_table_shows_every_balance_column_and_no_numbers_for_an_empty_or_failed_run(
    vergleich: Callable[[str], ModuleType],
) -> None:
    garantie = vergleich("stoerungen.garantie")
    zahlen = {
        "bestaetigt": 90,
        "unbestimmt": 1,
        "abgelehnt": 2,
        "ausgefuehrt": 60,
        "verluste": 30,
        "duplikate": 3,
        "duplikate_e077": 4,
    }
    eintraege = [
        {
            "run": 1,
            "system": "celery",
            "profil": "ab_werk",
            "stoerung": "sigkill",
            "status": "ok",
            "bilanz": zahlen,
            "worker_neustart": False,
        },
        {
            "run": 1,
            "system": "arq",
            "profil": "stark",
            "stoerung": "netztrennung",
            "status": "leer",
            "bilanz": None,
            "worker_neustart": True,
        },
    ]

    zeilen = garantie.tabelle(eintraege).splitlines()

    assert "| celery | ab_werk | sigkill | ok | 90 | 60 | 30 | 3 | 1 | 2 | 4 | nein |" in zeilen
    assert "| arq | stark | netztrennung | leer | - | - | - | - | - | - | - | ja |" in zeilen


def test_a_failed_run_with_a_balance_shows_no_numbers_in_the_result_table(
    vergleich: Callable[[str], ModuleType],
) -> None:
    garantie = vergleich("stoerungen.garantie")
    zahlen = dict.fromkeys(
        ("bestaetigt", "unbestimmt", "abgelehnt", "ausgefuehrt", "verluste", "duplikate", "duplikate_e077"), 5
    )
    eintrag = {
        "run": 1,
        "system": "swarmq",
        "profil": "stark",
        "stoerung": "sigkill",
        "status": "fehler",
        "bilanz": zahlen,
        "worker_neustart": False,
    }

    zeilen = garantie.tabelle([eintrag]).splitlines()

    assert "| swarmq | stark | sigkill | fehler | - | - | - | - | - | - | - | nein |" in zeilen


def test_a_failed_run_keeps_its_status_and_only_a_run_without_error_becomes_empty(
    vergleich: Callable[[str], ModuleType],
) -> None:
    garantie = vergleich("stoerungen.garantie")

    assert garantie.lauf_status("fehler", leer=True) == "fehler"
    assert garantie.lauf_status("haenger", leer=True) == "haenger"
    assert garantie.lauf_status("ok", leer=True) == "leer"
    assert garantie.lauf_status("ok", leer=False) == "ok"
