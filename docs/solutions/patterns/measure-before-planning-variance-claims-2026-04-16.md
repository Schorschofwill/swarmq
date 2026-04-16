---
title: "Measure Before Planning: baseline data beats premise-only reasoning"
date: 2026-04-16
category: patterns
module: workflow
problem_type: best_practice
component: planning
applies_when:
  - "A plan is being built on a claim about performance variance, latency, or statistical behavior"
  - "Review findings with high cross-reviewer confidence (>= 0.9) contradict a library's documented contract"
  - "The problem description reduces to 'we saw a big number once'"
  - "Before committing to architectural changes that assume a specific root cause"
root_cause: planning_error
resolution_type: workflow
severity: medium
tags:
  - planning
  - baseline
  - measurement
  - review
  - confidence
  - evidence
  - ce-plan
  - ce-review
  - statistics
  - workflow
---

# Measure Before Planning: baseline data beats premise-only reasoning

## Context

During a benchmark-stability planning session, two patterns surfaced that wasted ~2 hours of implementation time and would have cost more if not caught by the document-review pass. Both come down to the same meta-lesson: **evidence-based reasoning beats confidence-weighted reasoning**, even when the confidence comes from multiple independent reviewers. This note captures both as preventable patterns, because they will happen again on future plans.

## Guidance

### 1. Before planning against a variance claim, measure the variance.

If a plan starts with "we observe X % variance between runs, let's fix it", the first action is not to design a fix. The first action is to measure a real baseline. Two datapoints is not a distribution. A 10-run sample takes minutes and tells you:

- whether the observed variance is real (CV, not max-min)
- which scenario is the worst offender (not always the one you noticed)
- whether the assumed root cause is even consistent with the numbers

In the benchmark session: the observed "−18 % between two runs" was actually an N=2 outlier artifact. A baseline of N=10 runs showed CV = 4.4 % for Valkey and 7.1 % for RabbitMQ on the same scenario — well within normal async-network-benchmark jitter. The originally planned "warm-up + repeat + median aggregation" solution was built against a phantom. After measuring, the plan shrank by about 60 % and the per-scenario warm-up (the most invasive part) was dropped entirely because the existing global warm-up was already doing its job.

The adversarial document-review agent explicitly recommended this step before implementation, with pre-set decision thresholds:

- CV < 5 % → original single observation was an outlier, plan solves a non-problem
- CV 5–10 % → variance exists but cold-start is not the main driver, plan approach wrong
- CV > 10 % → problem confirmed, but root cause still needs independent verification

The threshold approach matters: it turns the measurement from "look at the numbers" into "the numbers tell us which branch to take". Without pre-set thresholds, a team will rationalize whatever they see.

**Why this matters:** Planning time is cheap compared to implementation time. Measurement time is cheap compared to planning time. The total cost of "measure first" is almost always lower than the cost of building a scoped-up plan and then reworking it after the review catches the premise error.

**How to apply:**

- Any plan whose Problem Frame cites a statistical claim (variance, latency percentiles, throughput deltas) must either (a) link to a baseline measurement, or (b) get one before the plan is considered ready for implementation.
- When an adversarial reviewer recommends "measure before implement", take it. The review pass is cheap — one round of empirical data collection at that point usually prevents a re-plan later.
- Baseline data goes into the plan document (frontmatter `baseline:` field or a "Baseline Evidence" section) so reviewers and future readers see what the plan actually assumes.

### 2. High cross-reviewer confidence is not proof; library-specific contracts can override it.

A code review produced a finding with cross-reviewer confidence 0.98 (correctness + reliability reviewers independently converging): "`await ch.close()` in a `finally` block should be wrapped in `try/except` to prevent the close exception from masking the original exception during cleanup."

In general Python, this is textbook correct. For `aio-pika` specifically, it is wrong. The close exception carries state-clearing side effects internal to aio-pika; swallowing it leaves the RobustConnection in a state where the NEXT `connection.channel()` call raises `RuntimeError('closed')`. The "fix" broke a downstream scenario that had been passing before.

The regression was caught by running the smoke test at a smaller N after applying the "safe" fix. Without that, it would have shipped. The lesson is not "never trust reviewers" — the lesson is that **library-specific lifecycle contracts override general-purpose correctness patterns**, and cross-reviewer agreement on a general principle does not validate a library-specific application.

**Why this matters:** Review systems that weight findings by reviewer count or confidence can systematically miss this class of error. Two reviewers who both know the general pattern will reinforce each other without either noticing that the library in hand has different semantics.

**How to apply:**

- For any review finding marked as "safe auto-fix" that touches a third-party library's lifecycle (close / cancel / shutdown / teardown), run at least one smoke test after applying — even if the unit tests pass.
- When a finding contradicts observed pre-fix behavior (the code was working before), that is a red flag regardless of confidence. Treat it as gated rather than safe.
- Document the library-specific contract where it was discovered. A code comment or a doc reference prevents the next reviewer from re-flagging the same "bug".

## Why This Matters

Both patterns trade a small upfront cost for a much larger avoided cost. Measuring a baseline is minutes; reworking a plan built against a phantom is hours. Running one smoke test after a "safe auto-fix" is seconds; debugging a library-specific regression in production is open-ended. The meta-lesson is the same in both: **local evidence about the specific system in hand outranks general reasoning about how systems in general tend to behave**.

## When to Apply

### Measure-before-plan
- Plan cites a performance, latency, or variance claim
- Problem is stated as "we saw X happen" with fewer than 5 observations
- Adversarial reviewer says "falsify before implement"
- A proposed fix targets a root cause that has not been isolated by measurement

### Library-contract-overrides-confidence
- Review finding touches `close()`, `cancel()`, `shutdown()`, `aclose()`, or any teardown path
- Finding recommends exception swallowing or retry wrapping around a third-party call
- Reviewer reasoning is "general Python best practice" rather than "I checked the library docs/source for this specific case"
- Pre-fix code was working; the finding argues it shouldn't be

## Examples

**Measure-before-plan, bad flow:**

```
Observation: "pubsub_notify was 127k msg/s in run 1, 105k in run 2. That's 18 % variance."
Plan: per-scenario warm-up + N-repeat-median + AggregateResult dataclass + 3 implementation units
Outcome: document-review catches that a global warm-up already exists and that N=2 is not a distribution
Rework: run N=10 baseline -> CV = 4.4 % -> plan shrinks to repeat+median only (no new warm-up)
```

**Measure-before-plan, good flow:**

```
Observation: same as above
Action: run 10 iterations, compute CV, min, max, median
Result: CV 4.4 %, max-min/median 16 %, existing global warm-up sufficient
Plan: narrower scope, only --repeat + median aggregation, 2 units instead of 3
```

**Library-contract, bad flow:**

```
Review finding (confidence 0.98, two reviewers): "wrap ch.close() in try/except to prevent masking"
Action: apply as safe_auto
Outcome: next smoke test run hits RuntimeError('closed') in a scenario that previously passed
Rework: bisect, revert the "fix", document that aio-pika's ch.close() carries required side effects
```

**Library-contract, good flow:**

```
Review finding: same
Action: apply BUT run one end-to-end smoke test before committing
Outcome: catch the regression immediately, revert and escalate the finding to "library-specific, needs review"
```

## References

- **Benchmark session commits:** 6f90079, da250b5, ecc5fc2, b7345f0, 455e59b
- **Related solution:** `docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md`
- **Baseline data:** `/tmp/baseline_pubsub.csv` (N=10 pubsub_notify, with existing global warm-up active)
- **Plan that was rewritten:** `docs/plans/2026-04-16-004-feat-benchmark-stability-plan.md` (the final minimal version)
- **Document-review stages:** coherence-reviewer, scope-guardian-reviewer, feasibility-reviewer, adversarial-document-reviewer — the adversarial agent raised the "measure before implement" recommendation that unblocked this one
