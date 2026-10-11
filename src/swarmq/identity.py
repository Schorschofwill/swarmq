"""Ausfuehrungsidentitaet (Plan 0.12.0, MR 12, BZ-08/BZ-12).

Neutral und rein (Q16): ``worker``, ``broker`` und ``cli`` nutzen es, ohne
einander zu importieren.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from typing import Any

from swarmq.headers import HEADER_DLQ_GENERATION, HEADER_REPLAY_ID, HEADER_XDEATH_BASE
from swarmq.queue_names import is_delay_queue

#: Broker-Header mit der DLX-Historie (RabbitMQ).
HEADER_X_DEATH = "x-death"

#: Deckel fuer n (12 Stellen): ein gefaelschter Header macht den Valkey-Key
#: nicht beliebig lang.
_MAX_XDEATH = 999_999_999_999


def _count_of(entry: Any) -> int:
    """``count`` eines ``x-death``-Eintrags ausserhalb der Delay-Queues; ein
    kaputter Eintrag zaehlt wie keiner."""
    if not isinstance(entry, Mapping):
        return 0
    queue, count = entry.get("queue"), entry.get("count")
    if not isinstance(queue, str) or isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        return 0
    return 0 if is_delay_queue(queue) else count


def _base_of(headers: Mapping[str, Any]) -> int:
    base = headers.get(HEADER_XDEATH_BASE, 0)
    if isinstance(base, bool) or not isinstance(base, int) or base <= 0:
        return 0
    return base


def xdeath_count(headers: Mapping[str, Any] | None) -> int:
    """n der Identitaet: ``x-swarmq-xdeath-base`` plus die Summe der ``count``
    aller ``x-death``-Eintraege, deren Queue keine Delay-Queue ist."""
    headers = headers or {}
    deaths = headers.get(HEADER_X_DEATH)
    counted = sum(_count_of(entry) for entry in deaths) if isinstance(deaths, list) else 0
    return min(_base_of(headers) + counted, _MAX_XDEATH)


# Laenger ist kein von swarmq erzeugter Wert (uuid4().hex, Generations-ID);
# ein laengerer oder kaputter Header zaehlt wie keiner und wird nicht
# ungedeckelt zum Valkey-Key.
_MAX_REPLAY_ID_LEN = 64


def replay_id_of(headers: Mapping[str, Any] | None) -> str | None:
    """H der Identitaet: ``x-swarmq-replay-id``, falls gueltig."""
    replay = (headers or {}).get(HEADER_REPLAY_ID)
    if isinstance(replay, str) and 0 < len(replay) <= _MAX_REPLAY_ID_LEN:
        return replay
    return None


def identity_suffix(headers: Mapping[str, Any] | None) -> str:
    """Der Teil der Identitaet hinter der Task-ID: ``""``, ``|H``, ``#n``
    oder ``|H#n``."""
    replay = replay_id_of(headers)
    count = xdeath_count(headers)
    suffix = "" if replay is None else f"|{replay}"
    return suffix if count == 0 else f"{suffix}#{count}"


def execution_identity(headers: Mapping[str, Any] | None, task_id: str) -> str:
    """``task_id`` | H | #n: bei n = 0 wie 0.11 (``task_id`` bzw.
    ``task_id|H``), bei n > 0 kommt ``#n`` dazu."""
    return f"{task_id}{identity_suffix(headers)}"


def copy_headers(headers: Mapping[str, Any] | None) -> dict[str, Any]:
    """Header einer Kopie, die swarmq selbst publiziert (Retry, Zurueck-
    stellung, DLQ-Stempel, ``dlq retry``): ``x-death`` wird entfernt und sein
    Stand in ``x-swarmq-xdeath-base`` festgehalten -- der Broker baut
    ``x-death`` beim naechsten Dead-Lettern neu auf (Spike S5), die Identitaet
    bleibt so stabil. Bei n = 0 traegt die Kopie keine Base (wie 0.11)."""
    count = xdeath_count(headers)
    copied = {k: v for k, v in (headers or {}).items() if k not in (HEADER_X_DEATH, HEADER_XDEATH_BASE)}
    if count > 0:
        copied[HEADER_XDEATH_BASE] = count
    return copied


def generation_id(identity: str) -> str:
    """Replay-ID der naechsten Ausfuehrungsgeneration beim Dead-Lettern durch
    den Worker: deterministisch aus der aktuellen Identitaet, damit zwei
    DLQ-Kopien derselben Ausfuehrung (``PublishIndeterminate``) dieselbe ID
    tragen und ein wiederholter ``dlq retry`` dieselbe Identitaet ergibt."""
    return "g" + hashlib.sha1(identity.encode(), usedforsecurity=False).hexdigest()[:39]


#: Namensraum der Replay-IDs, die ``dlq retry`` ohne frische Generation
#: vergibt (uuid5): uuid-foermig wie bisher, getrennt von den
#: Generations-IDs des Workers (``g`` + sha1).
_RETRY_NAMESPACE = uuid.UUID("5f0c1d2e-8a4b-5c6d-9e7f-0a1b2c3d4e5f")


def retry_replay_id(headers: Mapping[str, Any] | None, body: bytes) -> str:
    """Replay-ID fuer ``dlq retry`` einer Nachricht ohne frische Generation
    (Review MR 12, P2-2): deterministisch aus der Identitaet der
    XQ-Nachricht -- ein wiederholter Retry desselben Eintrags (Broker-DLX,
    0.11-Worker, Altbestand) ergibt dieselbe Identitaet und laeuft nur
    einmal; jede neue Generation hat eine andere. Ohne Task-ID im Body gibt
    es keine Identitaet: neue ``uuid4``."""
    task_id = task_id_of(body)
    if task_id is None:
        return uuid.uuid4().hex
    return uuid.uuid5(_RETRY_NAMESPACE, execution_identity(headers, task_id)).hex


def task_id_of(body: bytes) -> str | None:
    """Task-ID aus dem Nachrichten-Body; None, wenn er nicht die Form einer
    swarmq-Nachricht hat."""
    try:
        payload = json.loads(body)
    except ValueError, TypeError:
        return None
    task = payload.get("task") if isinstance(payload, dict) else None
    task_id = task.get("id") if isinstance(task, dict) else None
    return task_id if isinstance(task_id, str) else None


def is_replay(headers: Mapping[str, Any] | None) -> bool:
    """Replay-Identitaet (H gesetzt oder n > 0): ihre Done-Marker leben
    mindestens so lange wie die DLQ-Frist."""
    return identity_suffix(headers) != ""


def dead_letter_generation(headers: Mapping[str, Any] | None, body: bytes, at_ms: int) -> dict[str, Any]:
    """Header der DLQ-Kopie, die der Worker publiziert (MR 12): ``x-death``
    als Base, ``x-swarmq-replay-id`` = Generations-ID der aktuellen Identitaet
    und ``x-swarmq-dlq-gen`` = *at_ms* (der Wert von
    ``x-swarmq-dead-lettered-at`` desselben Stempels). Ohne Task-ID im Body
    gibt es keine Identitaet und damit keine Generation."""
    stamped = copy_headers(headers)
    task_id = task_id_of(body)
    if task_id is None:
        return stamped
    stamped[HEADER_REPLAY_ID] = generation_id(execution_identity(headers, task_id))
    stamped[HEADER_DLQ_GENERATION] = at_ms
    return stamped
