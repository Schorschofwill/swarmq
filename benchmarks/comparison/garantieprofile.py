"""Profile ``ab_werk`` und ``stark`` je System (Plan 2026-10-24-1118, KTD13, R20).

Eine Quelle für Adapter, Tests und Methodik: ``ab_werk`` lässt jede
Einstellung auf dem Wert der gepinnten Version, ``stark`` setzt die stärkste
Zustellgarantie, die sich mit swarmq vergleichen lässt. swarmq selbst hat
feste Garantien, seine beiden Profile sind gleich.

Der Dateiname ist nicht ``profile.py``, weil das Verzeichnis beim Lauf vorn in
``sys.path`` steht und das Modul ``profile`` der Standardbibliothek verdecken
würde (``cProfile`` importiert es).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

AB_WERK = "ab_werk"
STARK = "stark"
PROFILNAMEN = (AB_WERK, STARK)


@dataclass(frozen=True)
class Profil:
    """Einstellungen eines Systems in einem Profil."""

    system: str
    name: str
    einstellungen: dict[str, Any] = field(default_factory=dict)
    #: Was das Profil bedeutet, für Bericht und Methodik.
    hinweis: str = "Werkseinstellungen der gepinnten Version."
    #: Doku-Adresse oder Quelltextstelle der gepinnten Version.
    beleg: str = ""


def _profile(
    system: str,
    stark: dict[str, Any],
    hinweis: str,
    beleg: str,
    *,
    ab_werk: Profil | None = None,
) -> dict[str, Profil]:
    """Beide Profile; Einstellungen aus ``ab_werk`` gelten auch in ``stark``."""
    werk = ab_werk or Profil(system, AB_WERK)
    return {
        AB_WERK: werk,
        STARK: Profil(system, STARK, {**werk.einstellungen, **stark}, hinweis, beleg),
    }


_ARQ_POLL_DELAY = (
    "Der Worker fragt die Queue ab Werk alle 0,5 s ab (poll_delay); nacheinander eingereihte Tasks warten "
    "je bis zur nächsten Abfrage, Szenarien mit einzelnen Tasks messen deshalb rund 2 Tasks/s."
)
_ARQ_BELEG = "https://arq-docs.helpmanual.io/; arq 0.28.0: arq/worker.py (Worker.__init__, poll_delay, Worker.main)"

#: Celery 5.6.3 deklariert die Antwort-Queues der Fernsteuerung (Mingle beim
#: Start) und die Event-Queue von Gossip ab Werk nicht dauerhaft und nicht
#: exklusiv. RabbitMQ 4.3 lehnt das ab (``transient_nonexcl_queues``), der
#: Worker startet nicht. Exklusiv bleiben sie flüchtig wie ab Werk; die
#: Task-Queue berühren die Schlüssel nicht.
_CELERY_RABBITMQ_4 = {"control_queue_exclusive": True, "event_queue_exclusive": True}


PROFILE: dict[str, dict[str, Profil]] = {
    "swarmq": _profile(
        "swarmq",
        {},
        "swarmq hat feste Garantien und kein schwächeres Profil; es läuft auf frischen Quorum-Queues.",
        "Entscheidungsregister compound-packs/swarmq-entscheidungen/ (u. a. e017-dead-letter-strategie-auto.md)",
    ),
    # Schlüssel von ``app.conf`` in Celery 5.6.3 (celery/app/defaults.py).
    "celery": _profile(
        "celery",
        {
            "task_acks_late": True,
            "task_reject_on_worker_lost": True,
            # Quorum-Queues verlangen Publisher-Confirms (Celery-Doku zu Quorum-Queues).
            "task_default_queue_type": "quorum",
            "broker_transport_options": {"confirm_publish": True},
            # Prefetch je Pool-Prozess; ab Werk 4.
            "worker_prefetch_multiplier": 1,
        },
        "Späte Bestätigung, Rückgabe bei verlorenem Worker, Quorum-Queues mit Publisher-Confirms, Prefetch 1; "
        "exklusive Kontroll- und Event-Queues wie in ab_werk (RabbitMQ 4.3, transient_nonexcl_queues).",
        "https://docs.celeryq.dev/en/v5.6.3/userguide/configuration.html, "
        "https://docs.celeryq.dev/en/v5.6.3/getting-started/backends-and-brokers/rabbitmq.html#using-quorum-queues; "
        "celery/app/defaults.py, celery/app/amqp.py (x-queue-type), kombu/transport/pyamqp.py (transport_options)",
        ab_werk=Profil(
            "celery",
            AB_WERK,
            _CELERY_RABBITMQ_4,
            "Werkseinstellungen bis auf exklusive Kontroll- und Event-Queues: RabbitMQ 4.3 lehnt Celerys "
            "nicht dauerhafte, nicht exklusive Queues ab (transient_nonexcl_queues), ohne die Anpassung "
            "startet der Worker nicht. Die Task-Queue bleibt ab Werk.",
            "https://www.rabbitmq.com/docs/deprecated-features (transient_nonexcl_queues); celery 5.6.3: "
            "celery/app/defaults.py (control/event queue_exclusive), celery/app/control.py (Mailbox), "
            "celery/worker/consumer/mingle.py, celery/worker/consumer/gossip.py, celery/events/receiver.py",
        ),
    ),
    # Dramatiq 2.2.1: ``RabbitmqBroker(confirm_delivery=...)``
    # (dramatiq/brokers/rabbitmq.py) und die Umgebungsvariable
    # ``dramatiq_queue_prefetch`` des Workers (dramatiq/worker.py), ab Werk
    # zwei je Thread. Quorum-Queues kennt Dramatiq nicht: Es deklariert
    # klassische, dauerhafte Queues und sendet persistent.
    "dramatiq": _profile(
        "dramatiq",
        {"confirm_delivery": True, "dramatiq_queue_prefetch": 1},
        "Publisher-Confirms und Prefetch 1; klassische Queues, weil Dramatiq keine Quorum-Queues deklariert.",
        "https://dramatiq.io/reference.html (RabbitmqBroker); dramatiq 2.2.1: dramatiq/brokers/rabbitmq.py "
        "(confirm_delivery, _build_queue_arguments), dramatiq/worker.py (dramatiq_queue_prefetch)",
    ),
    # Der Worker fragt die Queue ab Werk alle 0,5 s ab (``poll_delay``); ein
    # Task, der erst nach dem Ergebnis des vorigen kommt, wartet bis zur
    # nächsten Abfrage. Das bleibt so und steht im Hinweis beider Profile.
    "arq": _profile(
        "arq",
        {},
        "Werkseinstellung: arq hat keine stärkere Einstellung und läuft nur auf Valkey, als eigener Vergleich. "
        + _ARQ_POLL_DELAY,
        _ARQ_BELEG,
        ab_werk=Profil(
            "arq", AB_WERK, hinweis=f"Werkseinstellungen der gepinnten Version. {_ARQ_POLL_DELAY}", beleg=_ARQ_BELEG
        ),
    ),
    # taskiq 0.13.0 / taskiq-aio-pika 0.6.1: ``--ack-type`` des Workers
    # (taskiq/cli/worker/args.py) und ``Queue(type=QueueType.QUORUM)``
    # (taskiq_aio_pika/queue.py). Beides ist dort schon der Werkswert;
    # ``stark`` setzt es ausdrücklich, damit eine neue Version es nicht
    # unbemerkt ändert. Publisher-Confirms kommen von aio-pika
    # (``channel(publisher_confirms=True)`` ab Werk).
    "taskiq": _profile(
        "taskiq",
        {"ack_type": "when_saved", "queue_type": "quorum"},
        "Bestätigung erst nach dem Speichern des Ergebnisses, Quorum-Queues; entspricht den Werkswerten.",
        "taskiq 0.13.0: taskiq/acks.py, taskiq/cli/worker/args.py (--ack-type); taskiq-aio-pika 0.6.1: "
        "taskiq_aio_pika/queue.py (QueueType), taskiq_aio_pika/broker.py (DeliveryMode.PERSISTENT)",
    ),
}


def profil(system: str, name: str) -> Profil:
    """Profil ``name`` des Systems ``system``."""
    return PROFILE[system][name]


#: Broker je System. arq kennt nur Redis-kompatible Server und bekommt einen
#: eigenen Vergleich auf Valkey (KTD13).
BROKER: dict[str, str] = {
    "swarmq": "rabbitmq",
    "celery": "rabbitmq",
    "dramatiq": "rabbitmq",
    "arq": "valkey",
    "taskiq": "rabbitmq",
}


def kombinierbar(system: str, broker: str) -> bool:
    """Ob ``system`` mit ``broker`` läuft."""
    return BROKER[system] == broker


#: Verteilungen, deren Versionen jedes Ergebnis speichert: das System und die
#: Client-Bibliotheken, über die es mit dem Broker spricht.
PAKETE: dict[str, tuple[str, ...]] = {
    "swarmq": ("swarmq", "aio-pika", "aiormq", "valkey"),
    "celery": ("celery", "kombu", "amqp", "redis"),
    "dramatiq": ("dramatiq", "pika", "redis"),
    "arq": ("arq", "redis"),
    "taskiq": ("taskiq", "taskiq-aio-pika", "taskiq-redis", "aio-pika", "aiormq", "redis"),
}
