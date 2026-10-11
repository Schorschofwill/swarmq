"""Nachricht lesen: Header pruefen, Groesse deckeln, Body dekodieren (U6).

Die Funktionen hier entscheiden, ob aus einer Zustellung ueberhaupt ein
Task-Aufruf werden kann. Sie sind aus ``pipeline.py`` gezogen, weil sie eine
eigene Frage beantworten -- "ist diese Nachricht brauchbar?" -- und weil das
Modul sonst das Zeilenlimit der Architektur-Guards reisst. Die Ablehnung
laeuft ueber ``route_via_dlx``, das seit U6 ``TASK_REJECTED`` ausloest.
"""

from __future__ import annotations

from typing import Any

from swarmq.headers import HEADER_RETRY_COUNT, HEADER_UNIQUE_HASH, HEADER_WORKFLOW_ID
from swarmq.logging import _error_fields, get_logger
from swarmq.serialization import decode_json
from swarmq.worker.gates import GateDeps, route_via_dlx

logger = get_logger(__name__)

#: Laenge des Unique-Hashes (SHA-256, hex).
_UNIQUE_HASH_LEN = 64
#: Laenge einer Workflow-ID (UUID in kanonischer Schreibweise).
_WORKFLOW_ID_LEN = 36


def valid_unique_hash(message: Any) -> str | None:
    """Read and validate the unique-hash header from an incoming message.

    Producer sites emit a sha256 hex digest (64 lowercase hex chars).
    Any message whose header deviates — missing, wrong type, wrong
    length, non-hex — is treated as "no flag": the worker skips
    release rather than forwarding unvalidated bytes to Valkey as a
    key. Defends against adversarial producers that could otherwise
    amplify a malformed header into a large per-message Valkey key
    (plan Risks / R3.5).
    """
    unique_hash = (message.headers or {}).get(HEADER_UNIQUE_HASH)
    if not isinstance(unique_hash, str) or len(unique_hash) != _UNIQUE_HASH_LEN:
        return None
    # Cheap manual hex check — re.match on a 64-char string is
    # faster than compiling a regex but still O(n).
    if not all(c in "0123456789abcdef" for c in unique_hash):
        return None
    return unique_hash


def valid_workflow_id(message: Any) -> str | None:
    """Read and validate the workflow-id header from an incoming message.

    Workflow ids are minted as ``str(uuid.uuid7())`` (client/worker side)
    — a canonical UUID string: 36 chars, lowercase hex with dashes at the
    8-4-4-4-12 positions. Any header that deviates — missing, wrong type,
    wrong length, non-hex, misplaced dashes — is treated as "no workflow":
    the caller skips advancement rather than forwarding unvalidated bytes
    to Valkey as a workflow-state key. Mirrors ``valid_unique_hash``:
    never mark a workflow failed on a malformed id.
    """
    wf_id = (message.headers or {}).get(HEADER_WORKFLOW_ID)
    if not isinstance(wf_id, str) or len(wf_id) != _WORKFLOW_ID_LEN:
        return None
    for i, c in enumerate(wf_id):
        if i in (8, 13, 18, 23):
            if c != "-":
                return None
        elif c not in "0123456789abcdef":
            return None
    return wf_id


def attempt_number(message: Any) -> int | None:
    """Laufender Versuch fuer den Logkontext (U10): ``retry_count + 1``.

    Der Retry-Header kommt vom Netz; ein Wert, der keine ganze Zahl ist,
    ergibt ``None`` (das Feld fehlt dann im Kontext), statt die Zustellung
    scheitern zu lassen.
    """
    raw = (message.headers or {}).get(HEADER_RETRY_COUNT, 0)
    if not isinstance(raw, int):
        return None
    return raw + 1


async def decode_and_validate(deps: GateDeps, message: Any) -> tuple[str, str, str, list[Any], dict[str, Any]] | None:
    """Size-Cap + Deserialize + Shape-Validierung; ``None`` = behandelt
    (Oversize/Malformed → DLX-Route ohne Redelivery)."""
    # Size cap BEFORE the decode. msgspec has a built-in recursion-depth
    # guard but no size guard — a compromised/misconfigured producer
    # could otherwise make the worker decode an arbitrarily large body
    # fully into memory before any validation runs.
    if len(message.body) > deps.config.max_message_body_bytes:
        logger.error(
            "message_body_too_large",
            body_bytes=len(message.body),
            cap_bytes=deps.config.max_message_body_bytes,
            hint="raise SWARMQ_MAX_MESSAGE_BODY_BYTES if legitimate",
        )
        await route_via_dlx(
            deps,
            message,
            reason="body_too_large",
            error=(f"body {len(message.body)} bytes over cap {deps.config.max_message_body_bytes}"),
        )
        return None
    try:
        payload = decode_json(message.body)
        task_meta = payload["task"]
        task_id = task_meta["id"]
        queue = task_meta["queue"]
        task_name = task_meta["name"]
        args = payload["args"]
        kwargs = payload["kwargs"]
        # Validate shapes before run(*args, **kwargs). A wrong-typed
        # payload (e.g. args="hello") would otherwise unpack a string
        # character-by-character into positional args and run the task
        # with garbage. Treat as malformed: route to DLQ, don't redeliver.
        if not isinstance(args, list):
            raise TypeError(f"task 'args' must be a list, got {type(args).__name__}")  # noqa: TRY301 - Formfehler laufen bewusst in den Dekodierfehler-Pfad
        if not isinstance(kwargs, dict):
            raise TypeError(f"task 'kwargs' must be a dict, got {type(kwargs).__name__}")  # noqa: TRY301 - Formfehler laufen bewusst in den Dekodierfehler-Pfad
    except Exception as exc:  # noqa: BLE001 - jeder Dekodierfehler routet in die DLQ
        logger.error("deserialize_failed", **_error_fields(exc))
        # Plan Unit 11 Risks: deserialize-fail cannot release (task_id
        # unknown). Accepted limitation — flag self-heals via TTL.
        await route_via_dlx(
            deps,
            message,
            reason="undecodable",
            error=str(exc),
        )
        return None
    return task_id, queue, task_name, args, kwargs
