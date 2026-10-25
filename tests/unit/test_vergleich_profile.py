"""Profile des Vergleichsaufbaus (Plan 2026-10-24-1118, KTD13, R19/R20).

``benchmarks/comparison/garantieprofile.py`` hält je System die Profile
``ab_werk`` und ``stark`` als Daten. Tests und Methodik lesen dieselbe Quelle.
Die Datei liegt außerhalb von ``src/`` und wird per Pfad geladen; sie braucht
keine der verglichenen Bibliotheken.

Test-Liste:
- [x] jedes der fünf Systeme hat beide Profile
- [x] ``stark`` für Celery enthält ``task_acks_late``, ``task_reject_on_worker_lost``, ``confirm_publish``, Quorum-Queues und Prefetch 1
- [x] ``stark`` für Dramatiq enthält ``confirm_delivery`` und Prefetch 1
- [x] ``stark`` für taskiq enthält ``ack_type=when_saved`` und Quorum-Queues
- [x] arq ist nur mit Valkey kombinierbar, alle anderen nur mit RabbitMQ
- [x] arq und swarmq ändern in ``stark`` nichts, und die Profile sagen das
- [x] jedes Profil nennt einen Beleg
- [x] Celery deklariert in beiden Profilen exklusive Kontroll- und Event-Queues, weil RabbitMQ 4.3 die
      Werkseinstellung (nicht dauerhaft, nicht exklusiv) ablehnt, und ``ab_werk`` nennt die Abweichung mit Beleg
- [x] die arq-Profile nennen die Abfragepause des Workers ab Werk (``poll_delay`` 0,5 s)
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from tests.support.skripte import lade_skript

PROFILE_PATH = Path(__file__).resolve().parents[2] / "benchmarks" / "comparison" / "garantieprofile.py"

SYSTEME = ("swarmq", "celery", "dramatiq", "arq", "taskiq")


@pytest.fixture(scope="module")
def profile() -> ModuleType:
    """Lädt die Profildatei per Pfad, ohne ``benchmarks/comparison`` in ``sys.path``."""
    return lade_skript("vergleich_garantieprofile", PROFILE_PATH)


@pytest.mark.parametrize("system", SYSTEME)
def test_jedes_system_hat_beide_profile(profile: ModuleType, system: str) -> None:
    assert set(profile.PROFILE[system]) == {"ab_werk", "stark"}
    assert profile.profil(system, "ab_werk").name == "ab_werk"
    assert profile.profil(system, "stark").name == "stark"


def test_celery_stark_setzt_spaete_bestaetigung_confirms_quorum_und_prefetch_eins(profile: ModuleType) -> None:
    """Celery-Konfigurationsschlüssel der gepinnten Version, wie ``app.conf`` sie liest."""
    einstellungen = profile.profil("celery", "stark").einstellungen

    assert einstellungen["task_acks_late"] is True
    assert einstellungen["task_reject_on_worker_lost"] is True
    assert einstellungen["broker_transport_options"] == {"confirm_publish": True}
    assert einstellungen["task_default_queue_type"] == "quorum"
    assert einstellungen["worker_prefetch_multiplier"] == 1
    assert profile.profil("celery", "ab_werk").einstellungen == {
        "control_queue_exclusive": True,
        "event_queue_exclusive": True,
    }


@pytest.mark.parametrize("name", ["ab_werk", "stark"])
def test_celery_deklariert_exklusive_kontroll_und_event_queues_fuer_rabbitmq_4(
    profile: ModuleType,
    name: str,
) -> None:
    """RabbitMQ 4.3 lehnt nicht dauerhafte, nicht exklusive Queues ab (``transient_nonexcl_queues``).

    Celery 5.6.3 deklariert ab Werk so die Antwort-Queues der Fernsteuerung
    (Mingle beim Start) und die Event-Queue von Gossip; der Worker startet
    dann nicht. Die Abweichung gilt in beiden Profilen und ändert die
    Task-Queue nicht.
    """
    celery = profile.profil("celery", name)

    assert celery.einstellungen["control_queue_exclusive"] is True
    assert celery.einstellungen["event_queue_exclusive"] is True
    assert "transient_nonexcl_queues" in celery.hinweis
    assert celery.beleg.strip()


def test_dramatiq_stark_setzt_confirm_delivery_und_prefetch_eins(profile: ModuleType) -> None:
    """``confirm_delivery`` ist ein Argument von ``RabbitmqBroker``, der Prefetch die Umgebungsvariable des Workers."""
    einstellungen = profile.profil("dramatiq", "stark").einstellungen

    assert einstellungen == {"confirm_delivery": True, "dramatiq_queue_prefetch": 1}
    assert profile.profil("dramatiq", "ab_werk").einstellungen == {}


def test_taskiq_stark_bestaetigt_nach_dem_speichern_auf_quorum_queues(profile: ModuleType) -> None:
    """``--ack-type`` des taskiq-Workers und ``QueueType`` von taskiq-aio-pika."""
    einstellungen = profile.profil("taskiq", "stark").einstellungen

    assert einstellungen == {"ack_type": "when_saved", "queue_type": "quorum"}
    assert profile.profil("taskiq", "ab_werk").einstellungen == {}


def test_arq_ist_nur_mit_valkey_kombinierbar(profile: ModuleType) -> None:
    assert profile.kombinierbar("arq", "valkey") is True
    assert profile.kombinierbar("arq", "rabbitmq") is False


@pytest.mark.parametrize("system", ["swarmq", "celery", "dramatiq", "taskiq"])
def test_die_anderen_systeme_laufen_auf_rabbitmq(profile: ModuleType, system: str) -> None:
    assert profile.kombinierbar(system, "rabbitmq") is True
    assert profile.kombinierbar(system, "valkey") is False


@pytest.mark.parametrize(("system", "benennung"), [("arq", "Werkseinstellung"), ("swarmq", "feste Garantien")])
def test_arq_und_swarmq_aendern_in_stark_nichts_und_sagen_das(
    profile: ModuleType,
    system: str,
    benennung: str,
) -> None:
    stark = profile.profil(system, "stark")

    assert stark.einstellungen == {}
    assert benennung in stark.hinweis


@pytest.mark.parametrize("system", SYSTEME)
def test_jedes_starke_profil_nennt_seinen_beleg(profile: ModuleType, system: str) -> None:
    """Doku-Adresse oder Quelltextstelle der gepinnten Version, für die Methodik."""
    assert profile.profil(system, "stark").beleg.strip()


@pytest.mark.parametrize("name", ["ab_werk", "stark"])
def test_arq_profile_nennen_die_abfragepause_des_workers(profile: ModuleType, name: str) -> None:
    """arq 0.28.0 fragt die Queue ab Werk alle 0,5 s ab (``Worker(poll_delay=0.5)``).

    Ein Task, der nach dem Ergebnis des vorigen eingereiht wird, wartet bis
    zur nächsten Abfrage; nacheinander laufende Szenarien messen dann rund
    zwei Tasks je Sekunde. Das ist Werksverhalten und bleibt im Vergleich,
    der Bericht muss es aber nennen.
    """
    arq = profile.profil("arq", name)

    assert arq.einstellungen == {}
    assert "poll_delay" in arq.hinweis
