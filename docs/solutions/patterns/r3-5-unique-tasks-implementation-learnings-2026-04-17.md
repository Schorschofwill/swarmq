---
title: "R3.5 Unique Tasks — implementation patterns and review learnings"
date: 2026-04-17
last_updated: 2026-04-17
category: patterns
module: swarmq
problem_type: reusable_pattern
component: client,worker,scheduler,backend
symptoms:
  - "Producer-side dedup via Valkey SETNX needs atomic check-and-claim semantics"
  - "Dedup flags can orphan when a post-SETNX step fails (publish, middleware, store insert)"
  - "Worker terminal-path release must not fire for non-terminal nack(requeue=True) branches"
  - "Deferred schedules need TTL = unique_for + fire_offset with ceiling arithmetic"
  - "msgspec.Struct schema extensions must append with defaults for backwards-compatible decode"
  - "AMQP header validation defends against adversarial amplification into Valkey keys"
root_cause: reusable_pattern
resolution_type: documented_pattern
severity: moderate
tags:
  - dedup
  - setnx
  - lua
  - scheduler
  - worker
  - msgspec
  - header-validation
  - r3-5
  - ce-review
---

# R3.5 Unique Tasks — implementation patterns and review learnings

Captured during R3.5 implementation (ce-work + ce-review cycle on
`feat/r3.5-unique-tasks`). These patterns apply to any future producer-
side dedup / at-most-once feature that coordinates state across Client,
Worker, and Scheduler.

## Problem

R3.5 introduced library-level deduplication via Valkey SETNX at four
producer sites (`Client.schedule`, `Client._schedule_deferred`,
`Client.schedule_many`, `Scheduler._fire_one`) with corresponding
worker-side release at pickup (`start`-mode) or terminal paths
(`completion`-mode). The feature surfaced several recurring patterns
that the existing worker-pipeline / ack-ordering / error-wrapping
solution docs did not cover explicitly. Left undocumented, future work
risks reinventing (and getting wrong) the same invariants.

## Patterns

### Pattern 1: atomic check-and-claim via `SET NX GET PX`

Valkey / Redis 7.0+ supports `SET key value NX GET PX ttl_ms` which
returns the existing value when NX fails, eliminating the TOCTOU gap
between a naive `SETNX` + `GET` two-step. R3.5 uses this for
`ValkeyBackend.setnx_unique`:

```python
result = await self._client.set(
    self._unique_key(dedup_hash),
    task_id,
    nx=True, get=True, px=ttl_ms,
)
# result: None → flag claimed (NX succeeded)
#         bytes → existing owner's task_id (NX rejected)
```

When in doubt: prefer this shape over any two-step claim pattern. It
composes cleanly with `asyncio.gather` (see Pattern 5).

### Pattern 2: rollback claims on post-SETNX failure

A dedup flag claimed before a subsequent fallible step (broker.publish,
pre_enqueue middleware, store.add_schedule) must be released if that
step raises. Otherwise the flag zombies until TTL while no backing
message exists — any concurrent same-args call gets the orphan
task_id as "existing" for the full window.

Shape used in R3.5:

```python
dedup_hash, existing = await self._claim_unique_flag(...)
if existing is not None:
    return existing
# ... headers[...] = dedup_hash
try:
    await chain.run_pre_enqueue(...)
    await self._broker.publish(...)
except BaseException:
    await self._rollback_unique_claim(dedup_hash, task_id)
    raise
```

The rollback helper uses the same owner-checked Lua script as the
worker release, so a racing reclaim (theoretical — cannot happen with
atomic SET NX GET) is safe by construction.

`schedule_many` extends this to a dict of per-element claims: any
Pass-1 abort releases everything already claimed; Pass-2 partial
failure releases only the failed positions.

### Pattern 3: `_unique_release_pending` state machine in Worker

`_handle_message` defaults `_unique_release_pending = True` (this
delivery is terminal until proven otherwise). Non-terminal branches —
anything that does `nack(requeue=True)` or `publish_to_delay_queue` —
clear the flag to `False` so the finally block does not release.

Positive-direction naming matters: forgetting to clear on a new
non-terminal branch causes an over-release, which the Lua owner-check
tolerates (redelivery's next terminal-path release simply returns
False). Under-release (the opposite direction) would leak the flag to
TTL.

Release sites that do NOT go through the finally (pre-start helpers,
inline DLQ sites inside `_try_apply_lock` and `_send_to_dlq`) release
inline AND the finally is guarded off via `_unique_release_pending`.

### Pattern 4: release AFTER ack, not before

The intuitive ordering — release the dedup flag before acking the
broker — breaks the dedup guarantee. If ack raises, the fallback nack
redelivers the message with the header intact but the flag already
gone. A concurrent second schedule during the redelivery window
succeeds and the task runs twice inside one dedup window.

Correct ordering at every terminal DLQ site:

```python
try:
    await broker.publish_to_dead_letter_queue(...)
    await broker.ack(message)  # ack FIRST
    await self._maybe_release_unique(...)  # release AFTER
except Exception:
    await broker.nack(message, requeue=True)
    # Do NOT release — redelivery's terminal path owns it.
```

Applies to `_send_to_dlq`, lock-render-fail DLQ, empty-render DLQ, and
lock-conflict-cap DLQ. The corresponding nack-requeue-true fallback
paths deliberately skip release.

### Pattern 5: `start`-mode release goes AFTER rate-limit + lock

The plan's initial placement — `start`-mode release at pickup, before
rate-limit / lock stages — broke dedup semantics: a rate-limited or
lock-conflicted message got requeued with the header intact but the
flag already released, opening a bypass window for concurrent
schedules.

Fix: move the start-mode release to the point where the task is
actually about to execute (after rate-limit + lock pass, before the
FIRE_AND_FORGET ack). This preserves ack-ordering discipline rule 1
("release before FAF-ack so Valkey-blip can't silent-drop") AND
eliminates the bypass window.

### Pattern 6: deferred SETNX TTL = `unique_for + ceil(fire_offset)`

Deferred schedules (`Client.schedule(delay=|eta=)`) hold the dedup
flag across the waiting window AND the task-execution window. The
TTL formula:

```python
fire_offset_ms = max(0, fire_ts_ms - now_ms)
fire_offset_s = (fire_offset_ms + 999) // 1000  # ceiling division
ttl_ms = (unique_for + fire_offset_s) * 1000
```

Floor division would drop sub-second deltas — a `delay=0.9,
unique_for=5` call would set TTL=5000ms, expire the flag before the
task finishes, and let a concurrent schedule bypass dedup. Ceiling
adds up to 999ms of padding per call; harmless upper bound.

### Pattern 7: msgspec.Struct append-only schema migration

R3.5 added two optional fields to `ScheduleDefinition` (`dedup_hash`
and `unique_until`). Both appended at the end of the struct with
defaults; pre-R3.5 entries in `sq:sched:defs` decode without error
because msgspec fills missing fields from the defaults.

Rules for future Struct changes:
1. New fields MUST have a default — old encoded data is missing them.
2. New fields MUST go at the end — msgspec decodes positionally.
3. Literal-typed fields CAN gain new variants (additive) but MUST NOT
   lose existing ones. (R3.5 extended `source` from two variants to
   three; old workers reading a `"cli_unique"` value from a new
   client would fail Literal validation, so deployment order matters:
   roll workers before activating new clients.)
4. Removing a field is a hard break — migrate via deprecation.

### Pattern 8: header validation at worker ingress

`x-swarmq-unique-hash` is untrusted input at the worker boundary. An
adversarial producer can send a 100KB value; without validation every
terminal path amplifies it into a large Valkey EVAL key.

`Worker._extract_valid_unique_hash` enforces the shape at ingress:

```python
if not isinstance(unique_hash, str) or len(unique_hash) != 64:
    return None
if not all(c in "0123456789abcdef" for c in unique_hash):
    return None
return unique_hash
```

Invalid headers are treated as "no flag" — safer than rejecting the
message, which would strand a legitimate but malformed delivery in
DLQ. Defence without liveness impact.

### Pattern 9: `_claim_unique_flag` as single-source-of-truth helper

Three producer sites (`schedule`, `_schedule_deferred`, `schedule_many`)
all run the same SETNX + `TASK_DUPLICATE_DETECTED` + structured-log
sequence. R3.5 centralises this in `Client._claim_unique_flag` so any
payload-shape change (adding a new field to the signal, changing the
log event string) requires exactly one edit instead of three.

`**extra_signal_payload` accepts site-specific keys (`batch_index` for
`schedule_many`); the helper defaults to None when not provided.

### Pattern 10: `_MissingType` sentinel singleton

R3.5 uses `_MISSING` to distinguish "caller didn't pass" from "caller
passed None" for optional dedup kwargs. Typing it as a dedicated
singleton class (rather than `Any`) lets mypy / pyright narrow the
union at call sites:

```python
class _MissingType:
    __slots__ = ()
    _instance: "_MissingType | None" = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

_MISSING = _MissingType()

def schedule(..., unique_for: int | None | _MissingType = _MISSING):
    if unique_for is _MISSING:
        ...
```

Identity check (`is _MISSING`) is the only correct comparison.
`__slots__ = ()` keeps the singleton tiny; `__new__` enforces
identity for type-checker narrowing.

## Related documents

- [worker-pipeline-stage-ack-ordering-2026-04-16](../logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md)
  — general ack-ordering rules. Patterns 3, 4, 5 here extend rule 1
  for dedup-specific release ordering.
- [nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16](../logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md)
  — DLX-log-wording discipline. R3.5 inherits it unchanged.
- [swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15](../logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md)
  — BackendConnectionError wrapping. `setnx_unique` raises,
  `release_unique` swallows — distinction documented in-module.
- [2026-04-17-004-feat-phase3-5-unique-tasks-plan.md](../../plans/2026-04-17-004-feat-phase3-5-unique-tasks-plan.md)
  — the R3.5 plan.

## Prevention checklist for future producer-side dedup features

- [ ] Use `SET NX GET PX` (Pattern 1) — never `SETNX` + `GET`.
- [ ] Wrap post-claim fallible steps in try/except with rollback (Pattern 2).
- [ ] Track terminal vs. non-terminal delivery state explicitly in
      the worker; default to "release pending" and require non-terminal
      branches to opt out (Pattern 3).
- [ ] Release AFTER ack at terminal DLQ sites; skip release in the
      nack-requeue-true fallback (Pattern 4).
- [ ] Place start-mode / early release AFTER requeue-capable stages,
      not before (Pattern 5).
- [ ] Use ceiling division for TTL formulas involving sub-second deltas
      (Pattern 6).
- [ ] Append msgspec.Struct fields with defaults; never reorder or
      remove (Pattern 7).
- [ ] Validate untrusted message headers at ingress before using them
      as Valkey keys (Pattern 8).
- [ ] Centralise repeated signal-dispatch shapes in a helper (Pattern 9).
- [ ] Type optional-arg sentinels as dedicated singleton classes,
      not `Any` (Pattern 10).
