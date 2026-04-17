---
title: "fix: Phase 3b.3 review findings"
type: fix
status: active
date: 2026-04-17
origin: docs/plans/2026-04-16-009-feat-phase3b3-task-cancellation-plan.md
review_run: .context/compound-engineering/ce-review/20260416-222406-a2113afd/
---

# fix: Phase 3b.3 review findings

## Overview

`ce:review` on `feat/phase3b3-task-cancellation` returned ~45 findings across
12 reviewers. Three were auto-applied in commit `fcfe4f1` (monitor
double-sleep, vacuous test, fallback-result-dict schema gap). This plan
addresses the remaining structural and correctness-sensitive findings.

## Problem Frame

The R3.6 Task Cancellation feature is functionally complete but carries
architectural debt identified across multiple reviewer lenses:

- **Shield-orphan risk**: the detached-releases mechanism tracks the
  shield-wrapper task, not the inner release — shutdown-during-cancel can
  still leak locks despite the current safeguard.
- **Stale-flag-zombie class**: the 24h flag TTL creates three separate
  pre-kill scenarios (deterministic task_id reuse, nack-requeue
  redelivery, worker crash before DEL) that are accepted but not
  documented or mitigated.
- **Test-discipline debt**: `asyncio.sleep` as readiness barrier (against
  `AGENTS.md §Async`), tests for private methods, implementation-detail
  assertions (`_semaphore._value`).
- **Solution-doc divergence**: `worker-pipeline-stage-ack-ordering` Rule 3
  (per-stage counter header `x-swarmq-cancel-deferred-count`) not
  implemented; nack(requeue=True) on BackendConnectionError can infinite-
  bounce.
- **Maintainability**: `_handle_message` is 410 lines with 5-level
  nesting; cancel logic deserves its own module.

## Requirements Trace

- **REV-P1-1**: Shield-orphan — re-design so shutdown awaits the inner
  release, not the shield-wrapper (correctness + reliability + adversarial,
  confidence 0.90).
- **REV-P1-3**: Add timeouts on `backend.is_cancelled` / `store_result` /
  `clear_cancel_flag` — slow Valkey wedges monitor at TCP-default
  latency (reliability, 0.85).
- **REV-P2-5**: Stale-flag-zombie class — either document as accepted
  limitation + API docstring warnings, OR add mitigation (task_id reuse
  guard, flag-nonce) (adversarial + correctness + reliability, 0.92).
- **REV-P2-6**: Cancel during `post_execution` → retry fires despite
  user intent. Decision: either extend monitor coverage past
  run_task.done(), or document as accepted limitation with explicit
  pre-`post_execution` check (adversarial + reliability, 0.88).
- **REV-P2-7**: Per-stage counter header `x-swarmq-cancel-deferred-count`
  with cap → DLQ at threshold, per Solution-Doc Rule 3 (learnings +
  adversarial, 0.88).
- **REV-P2-8**: Extract cancel logic into `src/swarmq/cancellation.py`
  (analog `locking.py`); `_handle_message` shrinks below 300 lines
  (maintainability + kieran-python, 0.85).
- **REV-P2-9**: Magic-string discriminators → `Literal["cancel", "timeout"]`
  or Enum (maintainability + kieran-python, 0.92).
- **REV-P2-10**: Extract `_build_cancel_result_dict(...)` helper to DRY
  up 3× duplicated `result_data` construction (maintainability +
  kieran-python + api-contract, 0.95).
- **REV-P2-11**: Replace `asyncio.sleep` readiness barriers in tests
  with `asyncio.Event` / `wait_for_condition` per `AGENTS.md §Async #4`
  (project-standards + testing, 0.92).
- **REV-P2-12**: Remove tests for private methods (`_cancel_monitor`
  direct-call tests in `TestCancelMonitorIsolated`) and
  implementation-detail assertions (`_semaphore._value`) per
  `AGENTS.md §Verboten` (project-standards, 0.88).

P3 findings (Magic-number `86_400_000 ms`, `ttl_ms` vs `result_ttl`
unit inconsistency, `asyncio.ensure_future` → `create_task`,
`except Exception` width, missing `@pytest.mark.timeout(5)`, unused
`contextlib_suppress_cancelled`, etc.) are grouped into a cleanup
implementation unit.

## Scope Boundaries

**In-Scope:**
- All P1 + P2 findings above
- Solution-doc update: convert "Phase 3b.3" from forward reference to
  third worked example; ratify `x-swarmq-cancel-deferred-count` as the
  cancel-stage counter-header name
- New solution doc if a genuinely new pattern emerges during
  implementation (e.g., shield-orphan mitigation)

**Out-of-Scope:**
- Performance benchmarks beyond concurrency=10 (advisory only)
- Valkey-cluster Pub/Sub-shard support
- `self.is_cancelled` property on Task (still YAGNI, separate brainstorm)
- Signal-payload `phase` field reintroduction (deferred)
- CLI `swarmq cancel` (Phase 3c)

## Implementation Units

- [x] **Unit 1: Shield-orphan re-design (REV-P1-1)**

**Goal:** Track the INNER release coroutine in `_detached_releases`,
not the shield-wrapper. Shutdown then awaits real release tasks.

**Files:**
- Modify: `src/swarmq/worker.py` (`_handle_message` finally + `_execute_shutdown`)
- Test: `tests/unit/test_worker.py` (new test: outer-cancel during
  shielded release must still release the lock)
- Test: `tests/e2e/test_cancellation.py` (extend
  `test_e2e_shutdown_during_running_cancel` with explicit assertion
  the release completed, not just that the lock is gone)

**Approach:**
- Create the inner release task first: `release = asyncio.ensure_future(
  self._release_acquired_locks(handler_id, acquired_locks))`
- Add `release` (not the shield) to `self._detached_releases`
- Await via `await asyncio.shield(release)` — if outer-cancel fires,
  the shield raises CancelledError into the finally, but `release`
  itself is tracked and will be awaited by `_execute_shutdown`

**Execution note:** Test-first. RED test must exhibit the leak before
GREEN.

---

- [x] **Unit 2: Backend-operation timeouts (REV-P1-3)**

**Goal:** Wrap `is_cancelled` / `store_result` / `clear_cancel_flag`
in `asyncio.wait_for` with a configurable timeout. Default: 5s.

**Files:**
- Modify: `src/swarmq/config.py` (new `backend_op_timeout: float = 5.0`)
- Modify: `src/swarmq/backend/valkey.py` (wrap all three ops)
- Test: Unit + integration

**Approach:** Plain `asyncio.wait_for` around the underlying Valkey
call. Timeout raises `BackendConnectionError` (existing exception
path) so the worker's error pipeline handles it consistently.

---

- [x] **Unit 3: Stale-flag-zombie mitigation (REV-P2-5)**

**Goal:** Decide between (a) documented-limitation + API warning, or
(b) flag-nonce scheme (cancel sets flag with a task-specific fingerprint
derived at schedule-time; pre-start check only honors flags whose
fingerprint matches the current delivery).

**Files:**
- Modify: `docs/plans/2026-04-16-009-feat-phase3b3-task-cancellation-plan.md`
  (document accepted limitation) OR
- Modify: `src/swarmq/backend/valkey.py`, `src/swarmq/client.py`,
  `src/swarmq/worker.py` (add fingerprint)
- New: `docs/solutions/logic-errors/cancel-flag-stale-redelivery.md`

**Approach:** User decision required. Recommend (a) for pre-v1
simplicity unless flag-reuse has already bitten a real caller.

---

- [x] **Unit 4: Post-execution cancel (REV-P2-6)**

**Goal:** Decide whether to extend cancel-monitor coverage past
`run_task.done()` to include `post_execution`.

**Approach:** Most conservative fix: in the retry-decision branch,
re-check `is_cancelled` from backend before deciding to retry. Catches
"cancel arrived during post_execution" without restructuring.

**Files:**
- Modify: `src/swarmq/worker.py` (retry guard)
- Test: `tests/integration/test_worker_cancellation.py` (new scenario)

---

- [ ] **Unit 5: Per-stage counter header (REV-P2-7)**

**Goal:** Add `x-swarmq-cancel-deferred-count` header to pre-start
cancel-path nack-requeues; cap at LOCK_CONFLICT_DLQ_CAP-equivalent
(10) → DLQ with diagnostic headers.

**Files:**
- Modify: `src/swarmq/worker.py` (`_pre_start_cancel_check`)
- Modify: `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`
  (ratify the header name, convert Phase 3b.3 from forward-ref to
  worked example)
- Test: unit + integration

---

- [x] **Unit 6: Extract `cancellation.py` + `_build_cancel_result_dict` (REV-P2-8, P2-10)**

**Goal:** New module `src/swarmq/cancellation.py` containing
`CancelCheckResult` dataclass, `_pre_start_cancel_check` helper,
`_cancel_monitor` coroutine, and `_build_cancel_result_dict` factory.
`worker.py` uses via import; `_handle_message` shrinks to reasonable
size.

**Files:**
- New: `src/swarmq/cancellation.py`
- Modify: `src/swarmq/worker.py` (remove extracted code)
- Modify: `src/swarmq/__init__.py` if anything becomes user-facing

---

- [x] **Unit 7: Magic-strings → `Literal` (REV-P2-9)**

**Goal:** Replace `cancel_reason[0] == "cancel"` / `"timeout"` with
`Literal["cancel", "timeout"]` type annotation + named constants.

**Files:**
- Modify: `src/swarmq/worker.py` (or `cancellation.py` after Unit 6)
- Modify: `src/swarmq/cancellation.py` (new — adds
  `_CANCEL_REASON_CANCEL: Literal["cancel"]` and
  `_CANCEL_REASON_TIMEOUT: Literal["timeout"]`)

---

- [ ] **Unit 8: Test-discipline cleanup (REV-P2-11, P2-12)**

**Goal:** Replace `asyncio.sleep` readiness barriers with
`asyncio.Event` / fixture-readiness-signals. Remove direct-call tests
of `_cancel_monitor` (covered by public `_handle_message` integration
tests). Replace `_semaphore._value` assertion with observable
behavior.

**Files:**
- Modify: `tests/integration/test_worker_cancellation.py`
- Modify: `tests/e2e/test_cancellation.py`
- Modify: `tests/unit/test_worker.py` (remove `TestCancelMonitorIsolated`,
  adjust `test_cancel_flag_set_does_not_acquire_semaphore`)

---

- [ ] **Unit 9: P3 cleanup batch**

**Goal:** Cosmetic / low-impact findings bundled:
- Named constant `_CANCEL_DEFAULT_TTL_MS = 86_400_000`
- Unify `ttl_ms` vs `result_ttl` units (document why `Client.cancel`
  uses ms while config uses seconds, OR change to seconds)
- `asyncio.ensure_future` → `asyncio.create_task` where applicable
- Narrow `except Exception` blocks
- Add `message: aio_pika.abc.AbstractIncomingMessage` type hint on
  `_pre_start_cancel_check`
- Add `@pytest.mark.timeout(5)` on new async tests
- Remove unused `contextlib_suppress_cancelled` class (replace with
  stdlib `contextlib.suppress`)
- Docstring additions (`TaskCancelledError` hierarchy, `Client.cancel`
  bool-semantics, `Signal.TASK_CANCELLED` payload shape)

## Risks & Dependencies

| Risk | Mitigation |
|---|---|
| Unit 1 (shield re-design) regresses existing lock-cleanup tests | Require both existing `tests/e2e/test_locking.py` and new shield-orphan test green before commit |
| Unit 6 extraction leaves `_handle_message` in inconsistent state mid-refactor | Do it in one commit; run full suite before commit |
| Unit 8 removing private-method tests loses coverage of edge cases (e.g., `set_reason_if_unset` first-writer-wins) | Move the valuable scenarios into `TestRunningCancelInHandleMessage` as public-API assertions |
| Solution-doc update (Unit 5) is breaking for future phase implementers | Keep old forward-ref paragraph as a historical note; add new worked-example section |

## Non-Goals

- Running-benchmark campaign for concurrency-scaling (Perf advisory)
- Valkey-cluster-mode testing
- Changing the `ttl_ms` unit decision fundamentally (stays ms for v1)
- Reintroducing the `phase` field on TASK_CANCELLED (deferred)
