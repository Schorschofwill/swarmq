---
title: "nack(requeue=False) routes via broker DLX — it does not drop the message"
date: 2026-04-16
category: logic-errors
module: swarmq/task_locking
problem_type: logic_error
component: background_job
symptoms:
  - "Log message says 'message dropped — honest data loss' when broker DLX silently routed the message to {queue}.XQ"
  - "nack(requeue=True) on a permanent error path causes infinite no-backoff redelivery loop"
  - "Format-string lock template renders to empty string and acquires the bare 'sq:lock:' key, silently colliding with every other empty-rendered lock"
root_cause: wrong_api
resolution_type: code_fix
severity: high
tags:
  - rabbitmq
  - dlx
  - dead-letter-exchange
  - nack
  - requeue
  - task-locking
  - message-routing
  - swarmq
  - pipeline-stage
  - phase-3b
---

# nack(requeue=False) routes via broker DLX — it does not drop the message

## Problem

Two failure paths in `Worker._try_apply_lock` (Phase 3b.2) misunderstood RabbitMQ
DLX routing semantics. The cap → DLQ broker-fault fallback logged
**"message dropped"** when the broker actually routes the message to `{queue}.XQ`
via the `x-dead-letter-exchange` config. The render-fail → DLQ broker-fault
fallback used `nack(requeue=True)` by analogy to the rate-limit stage — wrong,
because permanent render errors loop forever without counter or cap.

A third closely-related issue (ADV-010): `TaskInfo` rejected literal `lock=""`
at construction, but a format-string template `lock="{var}"` with `var=""`
slipped past, rendering to `""` at runtime and acquiring the bare prefix lock
`sq:lock:` — a silent global mutex shared by every empty-rendered lock in the
system.

## Symptoms

- **ADV-002 (cap → DLQ broker-fault)**: ERROR log `lock_conflict_dlq_failed`
  with `note: "message dropped — recover via valkey-cli DEL ..."`. Operator
  reads the log and concludes the message is gone, missing that it landed in
  `.XQ` without diagnostic headers.
- **ADV-009 (render-fail → DLQ broker-fault)**: a permanent render error like
  `lock="user-{user_id}"` with no `user_id` in the payload, combined with a
  flapping broker, produces an infinite loop:
  render-fail → publish_to_dead_letter_queue fails → `nack(requeue=True)` →
  message redelivered → render-fail again → loop with no backoff and no cap.
- **ADV-010 (empty post-render)**: `lock="{tag}"` task scheduled with `tag=""`
  passes TaskInfo validation, renders to `""` in the worker, and acquires
  `sq:lock:` — colliding with any other empty-rendered lock.

## What Didn't Work

- **First-pass fix for ADV-002 used misleading wording**. The Round-1 fix
  replaced the original `nack(requeue=True)` cascade (which would have looped
  forever) with `nack(requeue=False)` plus an ERROR log saying "message dropped
  — honest data loss with diagnostic beats silent infinite loop." The flag was
  correct, but the wording was factually wrong: the queue declaration in
  `src/swarmq/broker/rabbitmq.py:29-36` sets `x-dead-letter-exchange=""` +
  `x-dead-letter-routing-key={queue}.XQ` on every main queue, so any
  `nack(requeue=False)` on the main queue is automatically routed by RabbitMQ
  to `{queue}.XQ`. The message was never dropped. Operator runbook based on the
  log message would direct incident response toward "recovering a lost message"
  instead of "inspecting `.XQ` for a message missing its diagnostic headers."
  Caught by the second-iteration adversarial reviewer (ADV-008, confidence 0.92).

- **Render-fail fallback was written by analogy to rate-limit (session history)**.
  The rate-limit pipeline stage (Phase 3b.1) correctly uses `nack(requeue=True)`
  on broker fault — because rate-limit failures are transient (Valkey was momentarily
  unreachable; a different worker may succeed). The lock pipeline stage was
  written using the same template, so `_try_apply_lock`'s render-fail branch
  initially used `nack(requeue=True)` too. Render errors are structurally
  different — the payload cannot change between deliveries, so the same render
  failure reproduces on every redelivery, making `requeue=True` an unbounded
  bounce loop. The transient-vs-permanent distinction was missed because the
  pattern was copied without reasoning about the error class. Caught by the
  second-iteration adversarial reviewer (ADV-009, confidence 0.95).

- **Round-1 test for the render-fail path asserted `nack(requeue=True)`**. The
  initial regression test
  (`test_render_error_with_dlq_publish_failure_nacks_with_requeue`) verified
  that the broker-fault fallback nacked with requeue — the wrong flag.
  Test passing did not catch the bug; the test was wrong in the same way the
  code was wrong. Both were updated together in Round 2 to assert
  `nack(requeue=False)`.

- **TaskInfo `lock=""` validation only catches the literal case**. The Round-1
  fix added `if self.lock is not None and not self.lock.strip(): raise
  ValueError(...)` in `__post_init__`. This catches `lock=""` and `lock="   "`
  at construction but does NOT catch a format-string template that renders
  empty at runtime (e.g. `lock="{var}"` with `var=""`). Required a second
  guard in `_render_lock_name` — runtime checks complete what construction-time
  validation cannot.

## Solution

The two-level pattern for any pipeline-stage permanent-error path that wants
to route to the dead-letter queue:

**Level 1 — preferred, attaches application diagnostic headers**:
```python
try:
    await self._broker.publish_to_dead_letter_queue(
        queue, message.body, headers=original_headers,  # x-swarmq-* diagnostics
    )
    await self._broker.ack(message)
except Exception as broker_exc:
    # Level 2 fallback follows
    ...
```

**Level 2 — broker DLX fallback when Level 1 itself fails**:
```python
    logger.error(
        "lock_conflict_dlq_failed",  # or lock_render_dlq_failed, etc.
        ...,
        note=(
            f"message routed via broker DLX to {queue}.XQ "
            "without diagnostic headers"
        ),
    )
    await self._broker.nack(message, requeue=False)
```

**Empty-render guard** (prevents the bare-prefix collision):
```python
lock_name = self._render_lock_name(task_info.lock, task_class, args, kwargs)
if not lock_name.strip():
    # Same Level 1 / Level 2 pattern for routing the bad payload to DLQ
    ...
```

Three rules govern the pattern:

1. **`nack(requeue=False)` is the right primitive at every permanent-error
   fallback.** The broker's DLX config guarantees routing to `{queue}.XQ`. The
   message is not lost, just stripped of the application headers we tried to
   attach.
2. **`nack(requeue=True)` is reserved for transient errors** where redelivery
   to any worker can plausibly succeed (backend connection blip during lock
   acquire, broker channel transient close). Never use `requeue=True` for
   permanent payload errors (render failure, cap exceeded, malformed body).
3. **Log wording must match actual broker behavior.** Every `nack(requeue=False)`
   fallback logs `"routed via broker DLX to {queue}.XQ without diagnostic
   headers"`, not `"dropped"`, `"discarded"`, or `"lost"`. The diagnostic
   headers absence is the operationally relevant detail (operators need to
   inspect `.XQ` and use the original RabbitMQ `x-death` metadata to
   reconstruct what happened).

Applied in the codebase:

- `src/swarmq/worker.py` `_try_apply_lock` render-fail branch:
  Level 1 + Level 2 with `nack(requeue=False)` and DLX-honest log wording.
- `src/swarmq/worker.py` `_try_apply_lock` empty-render guard:
  same pattern, runs after `_render_lock_name` returns successfully.
- `src/swarmq/worker.py` `_try_apply_lock` cap → DLQ branch:
  same pattern, log message includes both DLX routing note AND wedge-recovery
  procedure (`valkey-cli DEL sq:lock:<name>`).

## Why This Works

RabbitMQ DLX semantics: when a queue is declared with `x-dead-letter-exchange`
and `x-dead-letter-routing-key`, any message that is nacked with `requeue=False`
(or expires via TTL, or is rejected) is automatically republished by the broker
to the configured exchange + routing key. SwarmQ's `RabbitMQBroker.declare_queue`
sets these to `""` (default exchange — routes by queue name) and
`f"{queue_name}.XQ"` respectively. The result: `nack(requeue=False)` on a
message in the main queue is functionally equivalent to a publish-to-`.XQ`,
just without our managed headers — only RabbitMQ's own `x-death` metadata is
attached. The message is never discarded.

`nack(requeue=True)` for a permanent error breaks this guarantee. The DLX is
only invoked on `requeue=False`. With `requeue=True`, the broker returns the
message to the head of the main queue, where the same permanent failure
condition is re-encountered immediately — unbounded loop, no backoff, no
operator-visible escape signal.

## Prevention

1. **Code-review checklist**: every `nack(requeue=False)` log message must
   explicitly name the destination (`{queue}.XQ via broker DLX`) rather than
   neutral wording like `"dropped"`, `"discarded"`, or `"failed"`. If the log
   says "dropped", it is wrong — the DLX guarantees delivery.

2. **Permanence question before copying nack patterns**: when adapting a
   broker-fault fallback from one pipeline stage to another, ask
   *"is this error permanent for this payload?"* explicitly:
   - Transient (backend connection, lock-acquire valkey blip) → `requeue=True`
   - Permanent (render error, cap exceeded, malformed body) → `requeue=False`
   - The render-fail bug was introduced because the new branch was written by
     pattern-matching to the rate-limit stage without making this distinction.

3. **Pattern search at PR time**: when adding a new pipeline stage with a DLQ
   branch, grep for `nack(requeue` in adjacent pipeline stages in the same
   file and verify each instance's `requeue` value matches the error's
   permanence. The render-fail bug was reachable in code one screenful below
   the cap-bug fix in the same function — but they were reviewed in different
   passes.

4. **Test the wording**: regression tests must assert the broker call has the
   correct flag AND the log message contains DLX-routing language:
   ```python
   broker.nack.assert_awaited_once_with(msg, requeue=False)
   # Plus, ideally, capture the log call and assert "via broker DLX" in note
   ```

5. **Runtime guards complement construction-time validation**. Any field that
   can be a format string and renders at runtime needs a runtime sanity check
   in addition to the static-input validation. `TaskInfo.__post_init__`
   catches `lock=""` literal; `_render_lock_name` must catch the rendered-empty
   case. Construction-time validation alone is incomplete for any input that
   can be parameterized.

6. **Cross-reviewer agreement boost is a strong signal**. Both ADV-008 and
   ADV-009 were caught by the second adversarial review pass — the first
   review accepted the `nack(requeue=False) = "dropped"` framing because no
   reviewer specifically checked the broker queue-declaration. When the
   second-pass adversarial reviewer was prompted to "look for the same shape
   elsewhere," it found ADV-009 immediately. Worth adding to plan-review
   prompts: explicitly direct reviewers to search for parallel sites of any
   bug they identify.

## References

- **Implementation**: `src/swarmq/worker.py` `_try_apply_lock`, `_acquire_and_register`, `_release_acquired_locks`
- **Broker DLX config (authoritative)**: `src/swarmq/broker/rabbitmq.py:29-36`
  (queue declaration with `x-dead-letter-exchange` + `x-dead-letter-routing-key`)
- **Tests**: `tests/unit/test_worker.py` — `TestFormatStringLock`,
  `TestEmptyRenderedLock`, `TestLockRequeueCounter::test_dlq_failure_at_cap_drops_message_no_infinite_loop`
- **Related solution doc — pipeline stage rules**:
  `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`
  — this doc EXTENDS Rule 2 of that one. Rule 2 currently says
  `"nack(requeue=True) lets another worker try"` which is correct for
  *transient* errors only. The DLX semantics make `nack(requeue=False)` the
  right primitive for *permanent* errors — see this doc.
- **Phase 3b.2 plan** (review iteration record):
  `docs/plans/2026-04-16-008-feat-phase3b2-task-locking-plan.md`
- **Fix commits**: `95cc2d6` (Round 1: cap cascade + log wording),
  `8b9ff67` (Round 2: render-fail loop + empty-render guard + DLX-honest log)
- **Adjacent learnings**:
  - `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`
    — `BackendConnectionError` wrapping pattern at the broker/backend boundary
  - `docs/solutions/patterns/measure-before-planning-variance-claims-2026-04-16.md`
    — same lesson family: library-specific contracts (here: RabbitMQ DLX)
    override generic review intuitions
