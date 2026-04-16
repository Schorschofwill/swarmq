---
title: "Worker pipeline stages: ack-ordering and the FIRE_AND_FORGET data-loss trap"
date: 2026-04-16
last_updated: 2026-04-16
category: logic-errors
module: swarmq
problem_type: logic_error
component: worker
symptoms:
  - "FIRE_AND_FORGET task disappears silently when a new pre-execute stage (rate limit, lock, dedup, cancel check) raises"
  - "AT_LEAST_ONCE message is duplicated when a new pre-execute stage publishes to the delay queue and the ack between publish and the original delivery's ack-or-nack races"
  - "New requeue-with-delay stages do not match _evaluate_retry's try/except shape — broker faults leave half-published messages"
  - "x-swarmq-rate-limit-count (or any per-stage retry counter) bounces unbounded with no operator visibility"
root_cause: logic_error
resolution_type: code_fix
severity: high
tags:
  - worker
  - pipeline
  - fire-and-forget
  - at-least-once
  - data-loss
  - publish-to-delay-queue
  - ack-ordering
  - rate-limit
  - lock
  - dedup
  - cancel
  - phase-3b
---

# Worker pipeline stages: ack-ordering and the FIRE_AND_FORGET data-loss trap

## Problem

Phase 3b.1 added a rate-limit check between message dispatch and the middleware chain. The first implementation acked FIRE_AND_FORGET messages **before** the rate-limit check. A code review found that any failure in the limiter (Valkey down, NOSCRIPT recovery failed, network blip) escaped a message that had already been acked — silent data loss with no log, no DLQ, no redelivery.

Phase 3b.2 (lock / semaphore / unique tasks) and Phase 3b.3 (cancellation check) will all add the same shape of stage: a Valkey-backed decision that runs before the middleware chain and may cause the message to be requeued with a delay. The same trap applies to each. This document captures the pattern so the next stage doesn't reintroduce it.

## Symptoms

- A FIRE_AND_FORGET task vanishes under load without any trace in logs, DLQ, or broker queues. No exception bubbles up because `_on_message` spawns the handler as a background task whose errors are not logged at the call site.
- An AT_LEAST_ONCE task occasionally duplicates: the rate-limit handler republishes to the delay queue but the broker channel closes before the original delivery is acked, so the broker redelivers the original.
- A user-visible "task is bouncing in the rate limiter" symptom is unobservable because no per-stage retry counter survives the requeue.

## What didn't work

**Treating the new pre-execute stage like a normal middleware hook.** Middleware hooks run after the FIRE_AND_FORGET ack and inside the message-handler try/except. Stages that requeue messages (rate limit, lock conflict, cancel) cannot live there — they need to decide the ack disposition themselves, before the FIRE_AND_FORGET fast-path commits the message.

**Trusting the implicit "if it raises, the message is unacked" rule.** It only holds for AT_LEAST_ONCE. FIRE_AND_FORGET acks at line 5b of `_handle_message`, far before any pre-execute stage. The exception path was never the safety net the implementation thought it was.

**Wrapping `ch.close()` style guards.** Initially we considered a try/except around the limiter call, but that left the original message acked and the new delay-queue copy never published when broker errors hit. The fix is structural reordering, not exception handling.

## Solution

A worker pipeline stage that may requeue with delay must follow these four rules. Phase 3b.1 implements them in `Worker._try_apply_rate_limit`; the same shape carries forward to 3b.2 and 3b.3.

### Rule 1 — run the stage BEFORE the FIRE_AND_FORGET ack

The early-ack at "5b. FIRE_AND_FORGET: ack immediately" must not happen until every stage that can decide the message's fate has had its turn. Reorder the pipeline so the stage check sits between registry lookup and the FIRE_AND_FORGET ack.

```python
# WRONG — pre-3b.1 first cut
task_info = task_class.task_info
if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
    await self._broker.ack(message)            # commits before any stage runs

if task_info.rate_limit:
    if not await self._rate_limiter.check(...):
        await self._broker.publish_to_delay_queue(...)
        # FIRE_AND_FORGET case: original is gone, delay copy may never arrive
        return

# RIGHT — 3b.1 final
task_info = task_class.task_info

if await self._try_apply_rate_limit(...):       # may ack OR nack-requeue
    return                                       # no FIRE_AND_FORGET ack reached

if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
    await self._broker.ack(message)
```

### Rule 2 — wrap the whole decision-and-handoff in try/except, mirror `_evaluate_retry`

`_evaluate_retry` already has the right shape: the broker `publish_to_delay_queue` + `ack` pair lives inside a try, and a broker fault triggers `nack(requeue=True)` on the original message. The new stage handler does the same.

```python
async def _try_apply_rate_limit(self, message, task_info, queue, task_id, task_name) -> bool:
    if task_info.rate_limit is None or self._rate_limiter is None:
        return False                             # not our concern, pipeline continues

    try:
        allowed, remaining, retry_after_ms = await self._rate_limiter.check(...)
    except Exception as exc:
        # Valkey unreachable — we cannot decide safely. Hand back to broker.
        # CRITICAL: this runs BEFORE any FIRE_AND_FORGET ack, so the message
        # is still in flight and nack(requeue=True) lets another worker try.
        logger.error("rate_limit_check_failed", ..., error=str(exc))
        await self._broker.nack(message, requeue=True)
        return True                              # we handled it (by returning to broker)

    if allowed:
        return False

    # ... emit signal, log, increment per-stage counter ...

    try:
        await self._broker.publish_to_delay_queue(queue, message.body, ...)
        await self._broker.ack(message)          # original delivery closed only after copy is in
    except Exception as exc:
        logger.error("rate_limit_requeue_failed", ..., error=str(exc))
        await self._broker.nack(message, requeue=True)

    return True
```

**Refinement (added 2026-04-16 from Phase 3b.2 review)**: Rule 2's
`nack(requeue=True)` is correct ONLY for transient broker/backend faults where
redelivery to any worker can plausibly succeed. For **permanent** payload
errors — render failures, format-string templates that cannot resolve,
conflict-counter cap exceeded — use `nack(requeue=False)`. The main queue is
declared with `x-dead-letter-exchange=""` + `x-dead-letter-routing-key=
{queue}.XQ` (see `src/swarmq/broker/rabbitmq.py:29-36`), so `nack(requeue=False)`
is automatically routed by the broker to `{queue}.XQ` — the message is NOT
lost, just stripped of any application-managed diagnostic headers.
**Misusing `requeue=True` for a permanent error produces an infinite no-backoff
loop**: the same payload re-encounters the same failure on every redelivery.

The decision rule:
- **Transient** (Valkey blip, backend disconnect, broker channel close) →
  `requeue=True`
- **Permanent** (render fail, malformed payload, cap exceeded) →
  `requeue=False`

Logging discipline: every `nack(requeue=False)` log message must explicitly
state the message is **routed to `{queue}.XQ` via broker DLX without diagnostic
headers** — never "dropped", "discarded", or "lost". The DLX guarantees
delivery; the operationally relevant detail is the missing diagnostic headers,
not message disposition.

See `docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md`
for the full DLX-routing analysis and the cascade pattern that originally
violated this refinement (Phase 3b.2 / R3.4 Task Locking, ADV-002 + ADV-009).

### Rule 3 — track a per-stage requeue counter on the message header

Without it, a misconfigured stage (rate limit set too tight, lock that never releases, cancel flag that lingers) bounces the same message forever and the only operator-visible signal is unrelated queue-depth growth. Increment a stage-specific header on every requeue and surface it in both the structured log and the dispatched signal.

```python
original_headers = dict(message.headers) if message.headers else {}
rl_count = int(original_headers.get("x-swarmq-rate-limit-count", 0)) + 1
original_headers["x-swarmq-rate-limit-count"] = rl_count

logger.info("task_rate_limited", ..., rate_limit_count=rl_count)
await self._signal_dispatcher.dispatch(
    Signal.TASK_RATE_LIMITED, ..., rate_limit_count=rl_count,
)
```

The plan's "Non-Goal: auto-DLQ on counter cap" is fine for now — visibility first, policy later. For 3b.2 use `x-swarmq-lock-conflict-count`, for 3b.3 use `x-swarmq-cancel-deferred-count` (or equivalent — pick names that are searchable).

### Rule 4 — single helper method per stage, not inline in `_handle_message`

Inlining the 35-line stage block in `_handle_message` made `_handle_message` 318 lines and made the FIRE_AND_FORGET ordering bug impossible to spot. Extracting `_try_apply_rate_limit` returning `bool` (`True` = handled, stop the pipeline; `False` = continue) gave us:

- testable in isolation with mock limiter + mock broker
- easy to reorder relative to the FIRE_AND_FORGET ack — one line move
- one place to put the try/except symmetry guarantees

The same pattern applies for `_try_apply_lock`, `_try_apply_unique`, `_try_apply_cancel_check`. Each returns `bool`; `_handle_message` chains them with early-return:

```python
if await self._try_apply_rate_limit(...): return
if await self._try_apply_lock(...): return
if await self._try_apply_unique_check(...): return
if await self._try_apply_cancel_check(...): return

if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
    await self._broker.ack(message)
# ... continue with middleware + run ...
```

## Why this works

The FIRE_AND_FORGET ack is a commitment to the broker that we have taken responsibility for the message. Anything that can decide the message must not run can run after that commitment — by then redelivery is impossible. By placing every "pre-execute decision stage" before the commitment, we keep the broker's redelivery as a safety net for every Valkey or broker fault.

The try/except mirror to `_evaluate_retry` matters because broker faults during the publish-then-ack pair are the only window where a single delivery can become two (publish succeeded, ack failed → broker redelivers original) or zero (publish failed, ack would have succeeded, exception propagates → original may be nack'd-requeue depending on outer handler). `nack(requeue=True)` on any failure puts the message back in the broker's hands — possibly creating a duplicate that the next stage's idempotent check re-throttles — but never losing it.

## Prevention

1. **Regression suite per stage.** Phase 3b.1 ships these tests in `tests/unit/test_worker.py` and they are the template for 3b.2 and 3b.3:
   - `test_rate_limit_set_but_limiter_none_falls_through_to_execution` — defensive guard when stage init failed
   - `test_limiter_connection_error_nacks_with_requeue` — Valkey-fault path
   - `TestFireAndForgetRateLimit::test_fire_and_forget_limiter_failure_does_not_drop_message` — the headline regression: FIRE_AND_FORGET + stage failure must nack, not silently lose
   - `TestRateLimitRequeueCounter::test_*` — counter header tracking

   Each new stage gets the same four cases, parametrised on the stage name.

2. **Code review checklist for any new pre-execute stage.** Before merging a stage that may requeue with delay, verify:
   - The stage runs before the FIRE_AND_FORGET ack site in `_handle_message`
   - The stage method follows the `_try_apply_*` naming and returns `bool`
   - Both the decision call and the publish+ack pair are inside try/except
   - The exception path issues `nack(requeue=True)` for **transient** failures, `nack(requeue=False)` for **permanent** failures (see Rule 2 refinement above + DLX-routing solution doc)
   - Every `nack(requeue=False)` log message states routing to `{queue}.XQ via broker DLX`, never "dropped"
   - A per-stage requeue counter is incremented and surfaced in signal kwargs
   - The four-test template above exists in `tests/unit/test_worker.py`

3. **Plan-level acceptance criterion.** Future plans that add a worker pre-execute stage (3b.2, 3b.3, anything later) must include "Stage runs before FIRE_AND_FORGET ack" as an explicit requirement — not buried in implementation notes. Plan 005 missed this and the bug shipped to the first review iteration.

## References

- Implementation: `src/swarmq/worker.py` — `Worker._try_apply_rate_limit` (canonical example) and the reorder in `_handle_message`
- Tests: `tests/unit/test_worker.py` — `TestWorkerRateLimiting`, `TestFireAndForgetRateLimit`, `TestRateLimitRequeueCounter`
- Phase 3b.1 plan: `docs/plans/2026-04-16-005-feat-phase3b-rate-limiting-plan.md`
- Phase 2 retry pattern (the `_evaluate_retry` shape this mirrors): `src/swarmq/worker.py:_evaluate_retry`, `docs/plans/2026-04-15-003-feat-phase2-production-core-plan.md`
- Phase 3b.1 review artifacts: `.context/compound-engineering/ce-review/20260416-121928-2f0da051/` — the headline finding (correctness C1 / reliability rel-001 / adversarial adv-001) where three independent reviewers converged on the FIRE_AND_FORGET data-loss path
- Fix commit: `5e1e695` — "fix(rate-limit): address P1 data-loss + reliability + test gaps from review"
- **Rule 2 refinement (DLX semantics)**: `docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md` — Phase 3b.2 review-iteration findings on transient vs permanent error handling and DLX-honest log wording
- Phase 3b.2 fix commits applying the refinement: `95cc2d6`, `8b9ff67`
- Adjacent learning: `docs/solutions/patterns/measure-before-planning-variance-claims-2026-04-16.md` — same session, on review-finding confidence vs library-specific contracts
