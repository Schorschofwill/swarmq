"""Worker-Paket (seit Plan U6).

``swarmq.worker.Worker`` und ``swarmq.worker.WorkerState`` bleiben die
Importpfade. Die Fassade re-exportiert bewusst keine Namen, die Tests heute
patchen (``logger``, ``decode_json``, ``asyncio``, ``os``) — Patches zielen
auf ``swarmq.worker.pipeline.*`` (KTD6/KTD14).

``pipeline.py`` trägt bis U19 den unveränderten Inhalt des früheren
``worker.py``; ``outcome.py`` das typisierte Zustellungs-Ergebnis (KTD5).
"""

from __future__ import annotations

from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal
from swarmq.worker.pipeline import Worker, WorkerState

__all__ = ["Continue", "DeliveryOutcome", "Redelivered", "Terminal", "Worker", "WorkerState"]
