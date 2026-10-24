"""Dramatiq-Broker und Aktoren des Vergleichs, für ``dramatiq workers.dramatiq_worker`` und den Erzeuger.

Broker RabbitMQ, Ergebnisse über die ``Results``-Middleware in Valkey.
``confirm_delivery`` kommt aus dem Profil; den Prefetch liest Dramatiq selbst
aus der Umgebungsvariable ``dramatiq_queue_prefetch``, die der Adapter nur
im Profil ``stark`` setzt.
"""

from __future__ import annotations

import hashlib
import json
import os
import time

import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results import Results
from dramatiq.results.backends.redis import RedisBackend

from stoerungen import hauptbuch

QUEUE = os.environ["VERGLEICH_QUEUE"]
EINSTELLUNGEN = json.loads(os.environ["VERGLEICH_EINSTELLUNGEN"])

broker = RabbitmqBroker(
    url=os.environ["VERGLEICH_BROKER_URL"],
    confirm_delivery=EINSTELLUNGEN.get("confirm_delivery", False),
)
result_backend = RedisBackend(url=os.environ["VERGLEICH_BACKEND_URL"])
broker.add_middleware(Results(backend=result_backend))
dramatiq.set_broker(broker)


@dramatiq.actor(actor_name="noop", queue_name=QUEUE, store_results=True)
def noop() -> bool:
    return True


@dramatiq.actor(actor_name="io_task", queue_name=QUEUE, store_results=True)
def io_task(sleep_ms: int = 10) -> bool:
    # Dramatiq-Aktoren laufen in Threads; ``time.sleep`` ist hier die I/O-Wartezeit.
    time.sleep(sleep_ms / 1000)
    return True


@dramatiq.actor(actor_name="cpu_task", queue_name=QUEUE, store_results=True)
def cpu_task(iterations: int = 10_000) -> bool:
    data = b"benchmark"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()
    return True


@dramatiq.actor(actor_name="garantie", queue_name=QUEUE, store_results=True)
def garantie(kennung: str, dauer_ms: int = 1000) -> None:
    hauptbuch.garantie(kennung, dauer_ms)


@dramatiq.actor(actor_name="failing_task", queue_name=QUEUE, store_results=True)
def failing_task(fail_count: int = 0) -> bool:
    if fail_count > 0:
        raise RuntimeError(f"Intentional failure (fail_count={fail_count})")
    return True


AKTOREN = {
    "noop": noop,
    "io_task": io_task,
    "cpu_task": cpu_task,
    "failing_task": failing_task,
    "garantie": garantie,
}
