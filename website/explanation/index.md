# Explanation

Background reading: the *why* behind SwarmQ. These pages are prose,
not procedure. Read them to build a mental model you can reason from
when something unusual happens, or before you decide whether a new use
case fits SwarmQ.

- [Architecture](architecture.md): client, broker, worker, supervisor
  and backend; the process model, the message pipeline, the scheduler,
  workflows, the queue topology and where every piece of state lives.
- [Delivery semantics](delivery-semantics.md): at-least-once, when the
  worker acks, where duplicates come from, the execution guard, delay
  tiers and the idempotency your task code has to provide.
- [Design decisions](design-decisions.md): the trade-offs behind the
  design, the alternatives considered and what was left out on purpose.

The [reference](../reference/index.md) answers *what*; these pages
answer *why*.
