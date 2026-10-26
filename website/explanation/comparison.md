# Comparison with Celery, Dramatiq, arq and taskiq

SwarmQ was measured against Celery, Dramatiq, arq and taskiq on one
reference machine, under faults and under load. This page shows the
results and their limits. How the measurement works, and the raw data, are
in the repository under `benchmarks/comparison/` (`METHODIK.md`,
`wertung/`).

**Setup.** celsius: Xeon E5-2650 v3, 20 threads, 60 GiB ECC RAM, SATA SSD;
RabbitMQ 4.3.6 and Valkey 8.1.10 in containers. Every system runs in two
profiles:

- `ab_werk`: the system's defaults.
- `stark`: its strongest comparable settings, such as late acknowledgement,
  publisher confirms and quorum queues.

SwarmQ has fixed guarantees, so both profiles are the same for it. Dramatiq
cannot declare quorum queues and uses classic durable queues in `stark`.

| System | Version |
|---|---|
| SwarmQ | `main` at `a9237312` (guarantees: `b18499a9`) |
| Celery | 5.6.3 |
| Dramatiq | 2.2.1 |
| arq | 0.28.0 |
| taskiq | 0.13.0 with taskiq-aio-pika 0.6.1 |

## Guarantees under faults

Each combination publishes tasks for a while, injects one fault, waits for
the system to recover and then compares a ledger of confirmed publishes
with executed tasks. Three rounds on 2026-10-25; numbers are sums over the
three rounds.

Faults: worker killed with `SIGKILL`, broker restart, network partition,
Valkey restart with persistence, Valkey restart without persistence.

| System | Profile | Lost tasks | Duplicate executions | Rejected publishes |
|---|---|---:|---:|---:|
| SwarmQ | both | **0** | **0** | **0** |
| Celery | `ab_werk` | 60 (`SIGKILL`) | 0 | 29 |
| Celery | `stark` | 0 | 120 (broker restart, partition) | 31 |
| Dramatiq | `ab_werk` | 0 | 189 (broker restart, partition) | 171 |
| Dramatiq | `stark` | 0 | 107 (broker restart, partition) | 172 |
| arq | both | 65 / 67 (Valkey restart without persistence) | 7 / 3 | 115 / 113 |
| taskiq | `ab_werk` | 0 | 120 (broker restart, partition) | 321 |
| taskiq | `stark` | 0 | 120 (broker restart, partition) | 291 |

- **Lost tasks.** Celery's defaults acknowledge before execution and lose
  the running tasks when a worker is killed. arq uses Valkey as its broker
  and loses the queued jobs when Valkey restarts without persistence.
- **Duplicates.** After a broker restart or a partition, Celery (`stark`),
  Dramatiq and taskiq run tasks a second time whose acknowledgement was
  lost with the connection. SwarmQ keeps an execution guard in Valkey and
  skips such redeliveries ([Delivery semantics](delivery-semantics.md)).
- **Rejected publishes.** The taskiq producer accepts no task after a broker
  restart or partition for the rest of the run. SwarmQ retries publishes that
  provably never left the client and rejected none.

## Performance

Median of three rounds on 2026-10-26, ten concurrent tasks per worker for
every system, profile `stark`. Values for `ab_werk` are in the raw data.

**`noop`**: one empty task at a time, enqueue to result.

| System | Tasks/s | P50 | P99 |
|---|---:|---:|---:|
| Dramatiq | 288 | 3.3 ms | 13.1 ms |
| Celery | 119 | 8.1 ms | 15.1 ms |
| taskiq | 117 | 8.6 ms | 11.7 ms |
| SwarmQ | 111 | 9.0 ms | 12.9 ms |
| arq | 2 | 501 ms | 504 ms |

**`latency`**: 100 tasks per second for 30 s, each sleeps 10 ms.

| System | P50 | P95 | P99 |
|---|---:|---:|---:|
| Dramatiq | 14.9 ms | 16.7 ms | 17.7 ms |
| SwarmQ | 19.5 ms | 23.1 ms | 25.0 ms |
| Celery | 19.5 ms | 22.9 ms | 25.6 ms |
| taskiq | 19.6 ms | 22.9 ms | 24.5 ms |
| arq | 369 ms | 550 ms | 588 ms |

**`io_bound`**: 1000 tasks that each sleep 10 ms, enqueued and collected
concurrently.

| System | Tasks/s | P50 |
|---|---:|---:|
| taskiq | 447 | 102 ms |
| Dramatiq | 435 | 50 ms |
| SwarmQ | 386 | 552 ms |
| Celery | 299 | 48 ms |
| arq | 181 | 1106 ms |

What the numbers say:

- **Quorum queues cost time per task.** A quorum queue confirms a publish
  only after writing it to disk. In `noop`, SwarmQ, Celery (`stark`) and
  taskiq, all on quorum queues, are level; Dramatiq on classic queues is
  faster. Locally, the publish in SwarmQ takes 2.1 ms with a quorum queue
  and 0.45 ms with a classic one.
- **SwarmQ's worker limits `io_bound`.** A task holds one of the ten worker
  slots for 15.1 ms (locally), of which 10.5 ms is the task itself; the rest
  are six backend calls in sequence and worker overhead. The high P50 is
  tasks waiting for a slot. This is where SwarmQ has to get faster.
- **arq polls for jobs every 0.5 s by default**, which dominates `noop` and
  `latency`.

## Limits

- One machine; it is 2.3 to 3.3 times slower than a current laptop for every
  system. Compare numbers only within this page.
- In `noop`, SwarmQ waits for the result by Pub/Sub, the others are polled
  every 1 ms and lose up to 1 ms per task. In `latency`, all are polled the
  same way.
- For Celery, Dramatiq and taskiq it was not examined whether the worker or
  the producer limits `io_bound`.
- 61 to 90 tasks per fault combination; rare cases may not show.
