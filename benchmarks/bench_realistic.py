"""
SwarmQ Realistic Broker Benchmark: RabbitMQ vs Valkey

Simuliert das exakte SwarmQ-Muster:
  - Prefetcher/Runner-Pattern mit Semaphoren (wie TaskIQ)
  - N concurrent async Worker die gleichzeitig Tasks abarbeiten
  - Jeder Task hat realistische async I/O-Arbeit (asyncio.sleep)
  - Ack nach Verarbeitung (at-least-once)
  - Valkey Streams mit Batch-Reads und Consumer Groups

Testet:
  A. Concurrent async ack-after-process (5ms, 10ms, 50ms Arbeit)
  B. Skalierung: 1, 5, 10, 25, 50 concurrent Worker
  C. Burst-Last: 10.000 Tasks auf einmal, dann abarbeiten
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from dataclasses import dataclass, field

import aio_pika
from aio_pika import DeliveryMode, Message as AMQPMessage
from valkey.asyncio import Valkey


RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5692/")
VALKEY_URL = os.getenv("VALKEY_URL", "valkey://localhost:6392/0")
QUEUE_NAME = "bench_realistic"
STREAM_KEY = f"{QUEUE_NAME}:stream"


@dataclass
class Result:
    broker: str
    scenario: str
    messages: int
    concurrency: int
    work_ms: float
    duration_s: float
    throughput_msg_s: float
    effective_msg_s: float  # Throughput abzueglich simulierter Arbeit
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def p50(self) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        return s[len(s) // 2]

    @property
    def p95(self) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        return s[int(len(s) * 0.95)]

    @property
    def p99(self) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        return s[int(len(s) * 0.99)]


def print_result(r: Result) -> None:
    print(
        f"  {r.broker:<16} | workers={r.concurrency:<3} work={r.work_ms:>5.0f}ms | "
        f"{r.throughput_msg_s:>8,.0f} msg/s | eff: {r.effective_msg_s:>8,.0f} msg/s | "
        f"P50={r.p50:>7.2f}ms P95={r.p95:>7.2f}ms P99={r.p99:>7.2f}ms"
    )


# ---------------------------------------------------------------------------
# RabbitMQ: Concurrent Async Worker mit Ack-after-Process
# ---------------------------------------------------------------------------


async def rmq_concurrent_ack(
    n: int,
    concurrency: int,
    work_ms: float,
) -> Result:
    conn = await aio_pika.connect_robust(RABBITMQ_URL)

    # Phase 1: Queue befuellen
    ch_pub = await conn.channel()
    queue_obj = await ch_pub.declare_queue(QUEUE_NAME, durable=True)
    await queue_obj.purge()

    for i in range(n):
        body = f"{time.perf_counter()}|{i}".encode()
        msg = AMQPMessage(body, delivery_mode=DeliveryMode.PERSISTENT)
        await ch_pub.default_exchange.publish(msg, routing_key=QUEUE_NAME)
    await ch_pub.close()

    # Phase 2: Concurrent Consumer mit ack-after-process
    consumed = 0
    done = asyncio.Event()
    lock = asyncio.Lock()
    latencies: list[float] = []

    async def worker(worker_id: int) -> None:
        nonlocal consumed
        ch = await conn.channel()
        await ch.set_qos(prefetch_count=2)  # Kleiner Prefetch fuer faire Verteilung
        q = await ch.declare_queue(QUEUE_NAME, durable=True)

        async for message in q:
            t_sent = float(message.body.split(b"|")[0])

            # Simulierte async I/O-Arbeit
            await asyncio.sleep(work_ms / 1000)

            # Ack NACH Verarbeitung
            await message.ack()

            lat = (time.perf_counter() - t_sent) * 1000
            async with lock:
                latencies.append(lat)
                consumed += 1
                if consumed >= n:
                    done.set()
            if done.is_set():
                break
        await ch.close()

    t0 = time.perf_counter()
    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await done.wait()
    dur = time.perf_counter() - t0

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await conn.close()

    # Theoretisches Maximum: n / (work_ms/1000) * concurrency wuerde
    # unendlich schnellen Broker voraussetzen. effective = abzueglich Arbeitszeit
    theoretical_max = concurrency / (work_ms / 1000) if work_ms > 0 else float("inf")
    throughput = n / dur
    effective = min(throughput, theoretical_max)

    return Result(
        "RabbitMQ",
        "concurrent_ack",
        n,
        concurrency,
        work_ms,
        dur,
        throughput,
        effective,
        latencies,
    )


# ---------------------------------------------------------------------------
# Valkey Streams: Concurrent Async Worker mit XREADGROUP + XACK
# ---------------------------------------------------------------------------


async def valkey_stream_concurrent_ack(
    n: int,
    concurrency: int,
    work_ms: float,
) -> Result:
    client = Valkey.from_url(VALKEY_URL, decode_responses=False)

    # Aufraeumen
    await client.delete(STREAM_KEY)

    # Phase 1: Stream befuellen
    pipe = client.pipeline()
    for i in range(n):
        ts = time.perf_counter()
        pipe.xadd(STREAM_KEY, {"ts": str(ts).encode(), "i": str(i).encode()})
        if (i + 1) % 500 == 0:
            await pipe.execute()
            pipe = client.pipeline()
    await pipe.execute()

    # Consumer Group erstellen
    group = "bench_workers"
    try:
        await client.xgroup_create(STREAM_KEY, group, id="0", mkstream=True)
    except Exception:
        await client.xgroup_destroy(STREAM_KEY, group)
        await client.xgroup_create(STREAM_KEY, group, id="0", mkstream=True)

    # Phase 2: Concurrent Consumer
    consumed = 0
    done = asyncio.Event()
    lock = asyncio.Lock()
    latencies: list[float] = []

    async def worker(worker_id: int) -> None:
        nonlocal consumed
        consumer_name = f"w{worker_id}"
        batch_size = 5  # Batch-Reads wie SwarmQ's Prefetcher

        while not done.is_set():
            results = await client.xreadgroup(
                group,
                consumer_name,
                {STREAM_KEY: ">"},
                count=batch_size,
                block=500,
            )
            if not results:
                continue

            for _stream, messages in results:
                for msg_id, data in messages:
                    t_sent = float(data[b"ts"])

                    # Simulierte async I/O-Arbeit
                    await asyncio.sleep(work_ms / 1000)

                    # Ack NACH Verarbeitung
                    await client.xack(STREAM_KEY, group, msg_id)

                    lat = (time.perf_counter() - t_sent) * 1000
                    async with lock:
                        latencies.append(lat)
                        consumed += 1
                        if consumed >= n:
                            done.set()
                            return

    t0 = time.perf_counter()
    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await done.wait()
    dur = time.perf_counter() - t0

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.aclose()

    theoretical_max = concurrency / (work_ms / 1000) if work_ms > 0 else float("inf")
    throughput = n / dur
    effective = min(throughput, theoretical_max)

    return Result(
        "Valkey/Stream",
        "concurrent_ack",
        n,
        concurrency,
        work_ms,
        dur,
        throughput,
        effective,
        latencies,
    )


# ---------------------------------------------------------------------------
# Valkey LIST: Concurrent Async Worker mit BRPOP (kein natives Ack)
# ---------------------------------------------------------------------------


async def valkey_list_concurrent_ack(
    n: int,
    concurrency: int,
    work_ms: float,
) -> Result:
    client = Valkey.from_url(VALKEY_URL, decode_responses=False)

    # Aufraeumen
    await client.delete(QUEUE_NAME)

    # Phase 1: Queue befuellen mit Pipeline
    pipe = client.pipeline()
    for i in range(n):
        ts = time.perf_counter()
        pipe.lpush(QUEUE_NAME, f"{ts}|{i}".encode())
        if (i + 1) % 500 == 0:
            await pipe.execute()
            pipe = client.pipeline()
    await pipe.execute()

    # Phase 2: Concurrent Consumer
    consumed = 0
    done = asyncio.Event()
    lock = asyncio.Lock()
    latencies: list[float] = []

    async def worker(worker_id: int) -> None:
        nonlocal consumed
        while not done.is_set():
            result = await client.brpop(QUEUE_NAME, timeout=1)
            if result is None:
                continue

            t_sent = float(result[1].split(b"|")[0])

            # Simulierte async I/O-Arbeit
            await asyncio.sleep(work_ms / 1000)

            # Kein explizites Ack bei LIST — Message ist nach BRPOP weg (at-most-once)

            lat = (time.perf_counter() - t_sent) * 1000
            async with lock:
                latencies.append(lat)
                consumed += 1
                if consumed >= n:
                    done.set()
                    return

    t0 = time.perf_counter()
    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await done.wait()
    dur = time.perf_counter() - t0

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.aclose()

    theoretical_max = concurrency / (work_ms / 1000) if work_ms > 0 else float("inf")
    throughput = n / dur
    effective = min(throughput, theoretical_max)

    return Result(
        "Valkey/List",
        "concurrent_ack",
        n,
        concurrency,
        work_ms,
        dur,
        throughput,
        effective,
        latencies,
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def main() -> None:
    parser = argparse.ArgumentParser(description="SwarmQ Realistic Broker Benchmark")
    parser.add_argument("--messages", "-n", type=int, default=5000)
    args = parser.parse_args()
    n = args.messages

    print(f"\nSwarmQ Realistic Benchmark — {n:,} Messages pro Test")
    print(f"RabbitMQ: {RABBITMQ_URL}")
    print(f"Valkey:   {VALKEY_URL}")
    print()

    all_results: list[Result] = []

    # ===================================================================
    # Test A: Verschiedene Arbeitszeiten bei fester Concurrency (10)
    # ===================================================================
    print("=" * 100)
    print("  TEST A: Einfluss der Arbeitszeit (Concurrency=10)")
    print("=" * 100)

    for work_ms in [1, 5, 10, 50]:
        print(f"\n  --- work_ms={work_ms} ---")
        r = await rmq_concurrent_ack(n, 10, work_ms)
        print_result(r)
        all_results.append(r)

        r = await valkey_stream_concurrent_ack(n, 10, work_ms)
        print_result(r)
        all_results.append(r)

        r = await valkey_list_concurrent_ack(n, 10, work_ms)
        print_result(r)
        all_results.append(r)

    # ===================================================================
    # Test B: Skalierung der Concurrency bei fester Arbeitszeit (10ms)
    # ===================================================================
    print()
    print("=" * 100)
    print("  TEST B: Concurrency-Skalierung (work_ms=10)")
    print("=" * 100)

    for conc in [1, 5, 10, 25, 50, 100]:
        print(f"\n  --- concurrency={conc} ---")
        r = await rmq_concurrent_ack(n, conc, 10)
        print_result(r)
        all_results.append(r)

        r = await valkey_stream_concurrent_ack(n, conc, 10)
        print_result(r)
        all_results.append(r)

        r = await valkey_list_concurrent_ack(n, conc, 10)
        print_result(r)
        all_results.append(r)

    # ===================================================================
    # Zusammenfassung
    # ===================================================================
    print()
    print("=" * 100)
    print("  ZUSAMMENFASSUNG")
    print("=" * 100)

    # Gruppiere nach (work_ms, concurrency)
    groups: dict[tuple[float, int], list[Result]] = {}
    for r in all_results:
        key = (r.work_ms, r.concurrency)
        groups.setdefault(key, []).append(r)

    print(f"\n  {'work_ms':>8} {'conc':>5} | {'RabbitMQ':>12} {'Valkey/Stream':>14} {'Valkey/List':>12} | Schnellster")
    print("  " + "-" * 90)

    for (work_ms, conc), entries in sorted(groups.items()):
        vals = {}
        for e in entries:
            vals[e.broker] = e.throughput_msg_s
        rmq = vals.get("RabbitMQ", 0)
        vs = vals.get("Valkey/Stream", 0)
        vl = vals.get("Valkey/List", 0)
        winner = max(entries, key=lambda x: x.throughput_msg_s)
        print(f"  {work_ms:>7.0f}ms {conc:>5} | {rmq:>10,.0f}   {vs:>12,.0f}   {vl:>10,.0f}  | {winner.broker}")

    print()


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.install()
        print("(uvloop aktiviert)")
    except ImportError:
        pass

    asyncio.run(main())
