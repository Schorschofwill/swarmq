"""RabbitMQ ``BrokerAdmin`` (Plan U5, KTD3): Dead-Letter-Operationen,
Queue-Tiefen und ``route_via_dlx``.

Die heutigen Methodennamen (``list_dead_letter_queue``, ``get_queue_depth``,
…) sind seit U15 entfernt; es gelten die Vertragsnamen (KTD10 abgeschlossen).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aio_pika
from aio_pika.exceptions import ChannelNotFoundEntity

from swarmq.broker.rabbitmq.connection import RabbitMQConnection, close_channel_quietly
from swarmq.broker.rabbitmq.delivery import RabbitMQDelivery, dead_letter_ttl_header, message_of, settle
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError
from swarmq.headers import (
    HEADER_DEAD_LETTER_TTL_MS,
    HEADER_DEAD_LETTERED_AT,
    HEADER_DLQ_GENERATION,
    HEADER_ENQUEUED_AT,
    HEADER_LOCK_CONFLICT_COUNT_LEGACY,
    HEADER_REPLAY_ID,
    HEADER_RETRY_COUNT,
    LOOP_STATE_HEADERS,
)
from swarmq.identity import HEADER_X_DEATH, copy_headers, replay_id_of, retry_replay_id
from swarmq.queue_names import delay_tiers


def _dead_lettered_at_ms(headers: dict[str, Any]) -> int | None:
    """``x-swarmq-dead-lettered-at`` als Epoch-Millisekunden; None, wenn er
    fehlt oder unlesbar ist (dann zaehlt er wie fehlend)."""
    value = headers.get(HEADER_DEAD_LETTERED_AT)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _fresh_generation(headers: dict[str, Any]) -> bool:
    """True, wenn ``x-swarmq-dlq-gen`` dem Zeitstempel
    ``x-swarmq-dead-lettered-at`` gleicht: Die Replay-ID ist die
    Generations-ID des Stempels eines 0.12-Workers (MR 12). Hat ein
    0.11-Worker die Nachricht erneut dead-gelettert, stempelt er den
    Zeitstempel neu, und die Gleichheit bricht."""
    generation = headers.get(HEADER_DLQ_GENERATION)
    if isinstance(generation, bool) or not isinstance(generation, int):
        return False
    # Review MR 12: eine Replay-ID, die der Worker nicht liest (fremder
    # Producer, > 64 Zeichen), liefe unter der Identitaet des Originals --
    # dessen Done-Marker verwuerfe das Replay.
    if replay_id_of(headers) is None:
        return False
    # Der Stempel entfernt ``x-death``. Steht es wieder da, hat der Broker die
    # Nachricht nach dem Stempel erneut dead-gelettert (UI-Move, Lauf,
    # ``route_via_dlx``): die Generation ist verbraucht (Review MR 12, P2-3).
    if headers.get(HEADER_X_DEATH):
        return False
    return generation == _dead_lettered_at_ms(headers)


_UNLIMITED = ("none", b"none")


def _message_ttl_s(headers: dict[str, Any]) -> tuple[bool, float | None]:
    """Frist aus ``x-swarmq-dead-letter-ttl-ms``: ``(True, Sekunden)``,
    ``(True, None)`` fuer ``"none"`` (unbegrenzt), ``(False, None)``, wenn
    der Header fehlt oder unlesbar ist."""
    value = headers.get(HEADER_DEAD_LETTER_TTL_MS)
    if value in _UNLIMITED:
        return True, None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return False, None
    return True, value / 1000


async def _close_quietly(channel: aio_pika.abc.AbstractChannel) -> None:
    if not channel.is_closed:
        await close_channel_quietly(channel)


class RabbitMQAdmin(RabbitMQDelivery):
    # ------------------------------------------------------------------
    # DLX-Fallback
    # ------------------------------------------------------------------

    async def route_via_dlx(self, delivery: Delivery) -> None:
        """``nack(requeue=False)``: RabbitMQ leitet über die Queue-Argumente
        nach ``{queue}.XQ`` und ergänzt ``x-death`` — ohne ``x-swarmq-*``-
        Diagnose-Header (Entwurf §7)."""
        message = message_of(delivery)
        if not await self._may_settle(message, "route_via_dlx"):
            # Anders als ein Ack ist das ein terminales Verb ohne Kopie: die
            # Nachricht ist NICHT im DLX. Der Aufrufer (Ausfuehrungsschutz)
            # darf sie nicht als abgeschlossen fuehren -- die Neuzustellung
            # des Brokers muss laufen.
            raise BrokerConnectionError(
                "route_via_dlx withheld: consumer was cancelled by the broker; the broker redelivers the message"
            )
        await settle(message.nack(requeue=False), "route_via_dlx")

    # ------------------------------------------------------------------
    # R3.10 DLQ inspection / recovery primitives
    # ------------------------------------------------------------------

    async def _dlq_queue(self, queue_name: str) -> aio_pika.abc.AbstractQueue:
        """Get a handle on ``{queue_name}.XQ``."""
        pub = await self._ensure_publish_channel()
        return await pub.get_queue(f"{queue_name}.XQ")

    @staticmethod
    def _parse_task_fields(body: bytes) -> tuple[str | None, str | None, dict[str, Any] | None]:
        """Extract (task_id, task_name, payload_dict) from a SwarmQ message body.

        Single-pass replacement for the earlier double-parse. All
        three values are None when the body isn't valid
        SwarmQ-shaped JSON. Catches both ``ValueError`` (bad JSON) and
        ``TypeError`` (body isn't bytes/str) so malformed inputs never
        crash the drain loop.
        """
        try:
            payload = json.loads(body)
        except (ValueError, TypeError):
            return None, None, None
        if not isinstance(payload, dict):
            return None, None, None
        task = payload.get("task")
        if not isinstance(task, dict):
            return None, None, payload
        task_id = task.get("id")
        task_name = task.get("name")
        return (
            task_id if isinstance(task_id, str) else None,
            task_name if isinstance(task_name, str) else None,
            payload,
        )

    @staticmethod
    def _normalise_header_keys(headers: dict[Any, Any] | None) -> dict[str, Any]:
        """Decode bytes-valued header keys to str.

        aio_pika occasionally surfaces AMQP header keys as ``bytes``
        instead of ``str`` (depends on wire encoding path). Callers that
        do ``headers.pop("x-swarmq-retry-count")`` miss a bytes-keyed
        entry silently (R-3). Normalise at the broker boundary so
        downstream code can use plain string keys.
        """
        out: dict[str, Any] = {}
        for k, v in (headers or {}).items():
            key = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else k
            out[key] = v
        return out

    # Back-compat alias for any in-module caller still using the old name.
    @staticmethod
    def _extract_task_id(body: bytes) -> str | None:
        task_id, _name, _payload = RabbitMQAdmin._parse_task_fields(body)
        return task_id

    async def _drain_dlq_with_filter(  # verbatim aus rabbitmq.py:606 übernommen; Follow-Up 0.2.x
        self,
        queue_name: str,
        limit: int,
        match_task_id: str | None = None,
        move_to_main: bool = False,
        break_after_first_match: bool = False,
        skip_malformed_on_move: bool = False,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Drain up to *limit* messages from the DLQ destructively, then
        republish each one to either the DLQ (default) or the main queue
        (when *move_to_main* is set and either no filter is in effect or
        the task_id matches).

        Four call-site modes encoded by the flag combinations:
          * SCAN-ONLY (match=None, move=False): list_dead_letters —
            drain + republish every scanned message back to DLQ.
          * INSPECT (match=task_id, move=False): inspect_dead_letter —
            drain + republish every scanned message back to DLQ, with
            break_after_first_match to minimise the crash-window for a
            targeted lookup.
          * RETRY-ONE (match=task_id, move=True, break=True): republishes
            ONE match to main, puts the rest back. docstring-accurate.
          * RETRY-ALL (match=None, move=True, skip_malformed=True):
            drain everything, forward valid messages to main, but route
            unparseable bodies back to the DLQ instead of re-publishing
            garbage into main that the worker would just dead-letter
            again.

        Why drain+republish and not ``nack(requeue=True)``:
        ``basic.nack(requeue=True)`` re-inserts messages at the HEAD of
        the queue, so a plain get+nack loop iterates forever on the same
        message. Destructive get + in-finally republish is the only
        portable-RabbitMQ pattern that traverses the queue and preserves
        content.

        Returns ``(all_entries, matched_entries)`` — each entry has
        ``task_id``, ``task_name``, ``queue``, ``headers``, ``body``
        (decoded dict or None), ``body_raw`` (utf-8-decoded string), and
        ``body_size``. Header keys normalised to ``str`` at the
        boundary so downstream code can use string keys safely (plan
        R-3). ``len(all_entries)`` is the number of messages actually
        scanned (not the *limit* cap) — callers use it for
        operator-facing count reporting.

        Crash safety: if the process dies mid-drain, any messages that
        have been read but not yet republished are lost. Per-message
        republish-error handling: an AMQP error on one republish does
        NOT abort the rest — the loop catches each
        publish individually, logs, and continues. After the loop runs
        to completion, the first collected error is re-raised so the
        caller observes the failure.
        """
        queue = await self._dlq_queue(queue_name)
        drained: list[tuple[bytes, dict[str, Any], bool, bool]] = []  # (body, headers, is_match, is_malformed)
        entries: list[dict[str, Any]] = []
        matched: list[dict[str, Any]] = []
        try:
            for _ in range(limit):
                msg = await queue.get(no_ack=True, fail=False)
                if msg is None:
                    break
                body = msg.body
                headers = self._normalise_header_keys(dict(msg.headers or {}))
                if self._stamp_or_expire(headers):
                    continue
                task_id, task_name, payload = self._parse_task_fields(body)
                entry: dict[str, Any] = {
                    "task_id": task_id,
                    "task_name": task_name,
                    "queue": queue_name,
                    "headers": headers,
                    "body": payload,
                    "body_raw": body.decode("utf-8", errors="replace"),
                    "body_size": len(body),
                }
                is_match = match_task_id is not None and task_id == match_task_id
                # Malformed = we could not extract a task_id; such
                # messages cannot be tracked for dedup or retried
                # meaningfully. retry_all skips them to avoid an
                # infinite cycle.
                is_malformed = task_id is None
                entries.append(entry)
                if is_match:
                    matched.append(entry)
                drained.append((body, headers, is_match, is_malformed))
                if break_after_first_match and is_match:
                    break
        finally:
            # Per-item try/except so one publish failure doesn't truncate
            # the rest of the drain. Failures are collected
            # and logged; the first is re-raised after the loop.
            errors: list[BaseException] = []
            for body, headers, is_match, is_malformed in drained:
                try:
                    # Route to main when:
                    #  * move_to_main is set, AND
                    #  * (no filter OR this entry matched the filter), AND
                    #  * NOT (skip_malformed_on_move set AND this is malformed)
                    to_main = move_to_main and (match_task_id is None or is_match)
                    if to_main and skip_malformed_on_move and is_malformed:
                        to_main = False

                    # Route through _publish_with_retry: transient
                    # channel errors during
                    # reconnect windows must not silently drop drained
                    # messages. Same contract as every other publish
                    # site.
                    if to_main:
                        msg_out = aio_pika.Message(
                            body=body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            headers=self._forward_headers(headers, body),
                        )
                        await self._publish_with_retry(queue_name, msg_out)
                    else:
                        # Back into the DLQ. Headers preserved verbatim,
                        # mit der Rest-TTL (KTD6): Lesen verlaengert nichts.
                        msg_out = aio_pika.Message(
                            body=body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            expiration=self._remaining_dead_letter_ttl(headers),
                            headers=dict(headers),
                        )
                        await self._publish_with_retry(f"{queue_name}.XQ", msg_out)
                except asyncio.CancelledError:
                    # Honour task-level cancellation — do not collect
                    # and swallow. _publish_with_retry already handles
                    # the aio_pika internal drain_future CancelledError
                    # via current_task().cancelling() guard.
                    raise
                except Exception as exc:
                    errors.append(exc)
            if errors:
                # Wrap the first collected failure in a
                # BrokerConnectionError so the CLI surfaces a clean
                # error line rather than a raw aio_pika traceback. The
                # rest are lost-to-log; a single transient broker blip
                # usually affects every subsequent publish identically.
                raise BrokerConnectionError(
                    f"dlq republish failed on {len(errors)} of {len(drained)} message(s); first error: {errors[0]}"
                ) from errors[0]
        return entries, matched

    def _forward_headers(self, headers: dict[str, Any], body: bytes) -> dict[str, Any]:
        """Header einer Wiederholung aus der DLQ (``dlq retry``)."""
        # MR 12: ``x-death`` geht als Base mit (stabile Identitaet).
        forward_headers = copy_headers(headers)
        # A6 (MR 12): die Wiederholung beginnt mit frischem Alter, sonst
        # liefe sie mit ``max_age`` sofort wieder ab.
        forward_headers[HEADER_ENQUEUED_AT] = int(self.clock())
        # Reset retry-count so the operator's "try this again" intent isn't
        # blocked by a stale counter.
        forward_headers.pop(HEADER_RETRY_COUNT, None)
        # Keine DLQ-Nachricht mehr (KTD6): ohne Frist und Zeitstempel; ein
        # erneutes Dead-Lettering stempelt neu.
        forward_headers.pop(HEADER_DEAD_LETTERED_AT, None)
        forward_headers.pop(HEADER_DEAD_LETTER_TTL_MS, None)
        # Q6 (Plan 0.12.0, MR 11): die Gnadenfrist fuer einen unbekannten Task
        # beginnt mit der Wiederholung neu (eine DLX-Kopie per ``nack`` behielt
        # den Header).
        # MR 13 (BZ-05): ebenso die Lock-Wartezeit.
        for state_header in LOOP_STATE_HEADERS:
            forward_headers.pop(state_header, None)
        # Review MR 13: der Konfliktzaehler von 0.11 (Deckel 10) ebenso, sonst
        # schickte ein 0.11-Worker die Wiederholung beim ersten Konflikt zurueck.
        forward_headers.pop(HEADER_LOCK_CONFLICT_COUNT_LEGACY, None)
        # Ausfuehrungs-Identitaet der Wiederholung (KTD5; Plan 0.12.0, MR 12):
        # der Ausfuehrungsschutz liesse sie sonst als abgeschlossen fallen,
        # solange der Done-Marker des ersten Laufs steht. Die Generations-ID
        # eines 0.12-Workers bleibt (ein wiederholter Retry derselben
        # XQ-Nachricht laeuft nicht doppelt); sonst eine aus der Identitaet
        # der XQ-Nachricht abgeleitete ID (Review MR 12, P2-2: ebenso
        # idempotent), ohne Task-ID im Body eine neue uuid4.
        if not _fresh_generation(headers):
            forward_headers[HEADER_REPLAY_ID] = retry_replay_id(headers, body)
        forward_headers.pop(HEADER_DLQ_GENERATION, None)
        return forward_headers

    def _remaining_dead_letter_ttl(self, headers: dict[str, Any]) -> float | None:
        """Rest der Verfallsfrist in Sekunden ab dem Dead-Lettering (KTD6);
        None ohne Frist. Es gilt die Frist der Nachricht
        (``x-swarmq-dead-letter-ttl-ms``), sonst die eigene."""
        known, ttl_s = _message_ttl_s(headers)
        if not known:
            ttl_s = self.dead_letter_ttl_s
        if ttl_s is None:
            return None
        dead_lettered_at_ms: int = headers[HEADER_DEAD_LETTERED_AT]
        return ttl_s - (self.clock() * 1000 - dead_lettered_at_ms) / 1000

    def _stamp_or_expire(self, headers: dict[str, Any]) -> bool:
        """True, wenn die gelesene DLQ-Nachricht abgelaufen ist (KTD6):
        RabbitMQ verwirft sie erst am Kopf der Queue -- beim Lesen faellt sie
        weg, statt mit neuer Frist zurueckzukehren.

        Ohne lesbaren Zeitstempel oder ohne lesbare Frist (Altbestand,
        DLX-Wege) ist unbekannt, wie lange die Nachricht bleiben sollte: die
        eigene Frist zaehlt ab jetzt, beide Header werden in ``headers``
        festgehalten."""
        known, _ttl_s = _message_ttl_s(headers)
        if _dead_lettered_at_ms(headers) is None or not known:
            headers[HEADER_DEAD_LETTERED_AT] = int(self.clock() * 1000)
            headers[HEADER_DEAD_LETTER_TTL_MS] = dead_letter_ttl_header(self.dead_letter_ttl_s)
            return False
        remaining = self._remaining_dead_letter_ttl(headers)
        return remaining is not None and remaining <= 0

    async def list_dead_letters(self, queue: str, limit: int = 100) -> tuple[list[dict[str, Any]], int]:
        """Return up to *limit* messages from ``{queue}.XQ``.

        Returns ``(entries, scanned_count)`` where:
          * ``entries`` — list of summaries (task_id / task_name / queue
            / headers / body_size). Heavy ``body`` / ``body_raw`` fields
            are stripped for compact list output; use ``inspect`` for
            the full payload.
          * ``scanned_count`` — the number of messages actually read
            (may be ``len(entries)``, but exposed explicitly so callers
            can tell "DLQ had fewer than limit" from "stopped at limit").

        Non-destructive: every scanned message is republished to the DLQ
        in the drain's finally-block.
        """
        entries, _matched = await self._drain_dlq_with_filter(queue, limit=limit)
        summaries = [
            {
                "task_id": e["task_id"],
                "task_name": e["task_name"],
                "queue": e["queue"],
                "headers": e["headers"],
                "body_size": e["body_size"],
            }
            for e in entries
        ]
        return summaries, len(entries)

    async def inspect_dead_letter(
        self, queue: str, task_id: str, scan_limit: int = 1000
    ) -> tuple[dict[str, Any] | None, int]:
        """Find the first DLQ message whose payload's ``task.id`` equals
        *task_id* and return its full metadata.

        Returns ``(match_or_None, scanned_count)``. ``scanned_count``
        lets the caller distinguish "genuinely not in the DLQ" from
        "scan stopped at limit before the match" for operator-facing
        output. Non-destructive: scanned messages are republished.

        Uses ``break_after_first_match`` to minimise the number of
        messages pulled off the wire once the target is found — this
        shortens the crash-exposure window for targeted lookups.
        """
        entries, matched = await self._drain_dlq_with_filter(
            queue, limit=scan_limit, match_task_id=task_id, break_after_first_match=True
        )
        return (matched[0] if matched else None), len(entries)

    async def retry_dead_letter(self, queue: str, task_id: str, scan_limit: int = 1000) -> tuple[bool, int]:
        """Republish the first matching DLQ message into *queue*.

        Returns ``(found, scanned_count)``. Uses
        ``break_after_first_match`` so duplicate task_ids in the DLQ
        are NOT all forwarded by a single call.

        The ``x-swarmq-retry-count`` header is stripped on forward so
        the message gets the full retry budget again (operator intent:
        "try this again"). All other headers — including the original
        traceback and any R3.5 dedup headers — pass through unchanged.

        Messages scanned but not matched are republished back to the
        DLQ via ``_drain_dlq_with_filter``.
        """
        entries, matched = await self._drain_dlq_with_filter(
            queue, limit=scan_limit, match_task_id=task_id, move_to_main=True, break_after_first_match=True
        )
        return len(matched) > 0, len(entries)

    async def retry_all_dead_letters(self, queue: str, limit: int = 10_000) -> tuple[int, int]:
        """Republish every valid DLQ message into *queue*.

        Returns ``(moved_count, scanned_count)``. Malformed-body
        messages (invalid JSON, missing task.id) are routed back to the
        DLQ instead of forward to main — the worker would just fail to
        deserialize them and dead-letter them again, causing an
        operator-invisible infinite cycle. Operator
        clears these via ``purge``.

        ``scanned_count`` equals the total number of messages drained
        (including malformed). ``moved_count`` is strictly less-or-
        equal to ``scanned_count``; the gap is the malformed set.
        """
        entries, _matched = await self._drain_dlq_with_filter(
            queue, limit=limit, move_to_main=True, skip_malformed_on_move=True
        )
        moved = sum(1 for e in entries if e["task_id"] is not None)
        return moved, len(entries)

    async def purge_dead_letters(self, queue: str) -> int:
        """Drop every message in ``{queue}.XQ`` via the native
        AMQP purge. Returns the message count as reported by RabbitMQ.
        """
        try:
            amqp_queue = await self._dlq_queue(queue)
            result = await amqp_queue.purge()
            return int(getattr(result, "message_count", 0))
        except BrokerConnectionError:
            raise
        except Exception as exc:
            raise BrokerConnectionError(str(exc)) from exc

    async def queue_depth(self, queue: str) -> int:
        """Return the total pending-message count for ``queue``, its delay
        companion ``{queue}.DQ`` and the delay tiers ``{queue}.DQ.1s`` …
        ``.1h`` (R4.7 Autoscaling; tiers since 0.12, BZ-01).

        Uses AMQP ``declare(passive=True)`` which returns the current
        ``message_count`` without modifying the queue — safe to poll
        at a 10s cadence. A queue that does not exist (worker not
        started, a tier never used) counts as 0; the broker then
        **closes the channel** (AMQP semantics), so the next name gets a
        fresh one. Throwaway channels only — a missing queue never
        poisons the publish channel.

        DLQ depth is deliberately excluded: XQ backlog is not
        work-to-do, it's work-that-failed and should not drive
        scale-up.
        """
        return await self._passive_message_counts((queue, f"{queue}.DQ", *delay_tiers(queue)))

    async def ready_depth(self, queue: str) -> int:
        """Nachrichten, die in ``queue`` selbst zur Abholung bereitliegen, ohne
        Delay-Queues und DLQ (Plan 0.12.0, MR 10). Grundlage des Autoscalers:
        lange Rueckstellungen in den Delay-Stufen sind keine Arbeit fuer jetzt."""
        return await self._passive_message_counts((queue,))

    async def _passive_message_counts(self, names: tuple[str, ...]) -> int:
        """Summe der ``message_count`` von ``names`` ueber einen Wegwerf-Channel,
        der nach NOT_FOUND neu geoeffnet wird (Plan 0.12.0, MR 10)."""
        connection = self._admin_connection()
        if connection is None or connection.is_closed:
            raise BrokerConnectionError("broker is not connected")
        total = 0
        channel: aio_pika.abc.AbstractChannel | None = None
        try:
            for name in names:
                if channel is None or channel.is_closed:
                    channel = await connection.channel()
                try:
                    q = await channel.declare_queue(name, passive=True)
                except ChannelNotFoundEntity:
                    # Queue does not exist — counts as 0. The broker closed the
                    # channel, so ``is_closed`` is already True: close it here
                    # WITHOUT that guard, or a RobustChannel reopens itself and
                    # is never closed (Review MR 10, P0-1; like ``arg_conflict``).
                    await close_channel_quietly(channel)
                    channel = None
                    continue
                result = getattr(q, "declaration_result", None)
                if result is not None:
                    total += int(getattr(result, "message_count", 0))
        except Exception as exc:
            raise BrokerConnectionError(str(exc)) from exc
        finally:
            if channel is not None:
                await _close_quietly(channel)
        return total

    async def dead_letter_depth(self, queue: str) -> int:
        """Return the live message count of ``{queue}.XQ`` (the
        dead-letter queue). Unlike queue_depth (which excludes the
        DLQ), this is exactly the "work that failed" backlog operators
        alert on."""
        return await self._throwaway_channel_message_count(f"{queue}.XQ")

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------








    async def _passive_message_count(self, name: str) -> int:
        return await self._throwaway_channel_message_count(name)


# Damit ``RabbitMQConnection`` im Modul referenziert bleibt (MRO-Dokumentation).
__all__ = ["RabbitMQAdmin", "RabbitMQConnection"]
