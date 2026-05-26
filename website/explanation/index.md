# Explanation

Background reading — the *why* behind SwarmQ's design decisions.
These pages are prose, not procedure. Read them to build the mental
model you can reason from when something unusual happens.

- [Architecture](architecture.md) — how broker, backend, workers,
  and clients fit together, and where state lives.
- [Delivery semantics](delivery-semantics.md) — at-least-once, ack
  timing, redelivery scenarios, and the idempotency contract your
  task code has to honour.
- [Design decisions](design-decisions.md) — the trade-offs made,
  the alternatives considered, and the things deliberately left
  out.

Use these pages as context when the reference answers "what" but
you need "why". They are also the fastest way to understand whether
a new use case fits SwarmQ's shape before writing code.
