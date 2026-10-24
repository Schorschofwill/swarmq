"""Celery-App des Vergleichs, für ``celery -A workers.celery_worker worker`` und den Erzeuger.

Broker RabbitMQ, Ergebnisse in Valkey. Queue und Profileinstellungen kommen
aus den ``VERGLEICH_*``-Variablen, die der Adapter setzt: ``ab_werk`` lässt
``app.conf`` unverändert, ``stark`` übernimmt die Schlüssel aus
``garantieprofile.py`` wörtlich.
"""

from __future__ import annotations

import hashlib
import json
import os
import time

from celery import Celery

from stoerungen import hauptbuch

app = Celery(
    "vergleich",
    broker=os.environ["VERGLEICH_BROKER_URL"],
    backend=os.environ["VERGLEICH_BACKEND_URL"],
)
# Die Default-Queue ist die frische Queue des Laufs; ``task_default_queue_type``
# aus ``stark`` macht sie zur Quorum-Queue (celery/app/amqp.py).
app.conf.task_default_queue = os.environ["VERGLEICH_QUEUE"]
app.conf.update(json.loads(os.environ["VERGLEICH_EINSTELLUNGEN"]))


@app.task(name="noop")
def noop() -> bool:
    return True


@app.task(name="io_task")
def io_task(sleep_ms: int = 10) -> bool:
    # Celery-Tasks laufen im Prefork-Pool synchron; ``time.sleep`` ist hier die I/O-Wartezeit.
    time.sleep(sleep_ms / 1000)
    return True


@app.task(name="cpu_task")
def cpu_task(iterations: int = 10_000) -> bool:
    data = b"benchmark"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()
    return True


@app.task(name="garantie")
def garantie(kennung: str, dauer_ms: int = 1000) -> None:
    hauptbuch.garantie(kennung, dauer_ms)


@app.task(name="failing_task")
def failing_task(fail_count: int = 0) -> bool:
    if fail_count > 0:
        raise RuntimeError(f"Intentional failure (fail_count={fail_count})")
    return True
