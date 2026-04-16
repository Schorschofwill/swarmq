"""
SwarmQ Broker-Benchmark: RabbitMQ vs Valkey

Testet beide Systeme unter async-lastigen Szenarien, die den realen
Einsatz in einem vollstaendig asynchronen Task Manager abbilden.

Szenarien:
  1. Enqueue-Throughput      — Wie schnell koennen Messages gesendet werden?
  2. Dequeue-Throughput      — Wie schnell koennen Messages empfangen + geackt werden?
  3. Round-Trip-Latenz       — Enqueue → Dequeue → Ack Latenz (P50/P95/P99)
  4. Concurrent Producers    — N parallele async Tasks senden gleichzeitig
  5. Concurrent Consumers    — N parallele async Tasks konsumieren gleichzeitig
  6. Full Pipeline           — Producers + Consumers gleichzeitig (realistisch)
  7. Payload-Groessen        — Einfluss der Nachrichtengroesse auf Throughput
  8. Pub/Sub Result-Notify   — Push-basierte Benachrichtigung (fuer Result-Backend)
  9. Acknowledge-Patterns    — ack-after-process vs. ack-on-receive

Aufruf:
  python bench.py [--messages N] [--concurrency N] [--scenario NAME|all]

Diagnose bei Hangs:
  kill -USR1 <pid>   dumpt alle Python-Threads + asyncio-Tasks nach stderr
  kill -USR2 <pid>   dumpt C-Level-Stacks via faulthandler (async-signal-safe)
"""

from __future__ import annotations

import argparse
import asyncio
import faulthandler
import json
import os
import signal
import statistics
import sys
import time
import traceback
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import FrameType

import aio_pika
from aio_pika import DeliveryMode, Message as AMQPMessage
from valkey.asyncio import Valkey


@asynccontextmanager
async def _bench_channel(
    connection: aio_pika.abc.AbstractRobustConnection,
):
    """Open an aio-pika channel and guarantee it is closed on exit.

    Wraps the channel lifecycle so benchmark functions do not have to
    remember to put `await ch.close()` in a finally. On task
    cancellation the channel is still closed, which is what originally
    caused the aiormq `_reader`-task leak in full_pipeline.
    """
    ch = await connection.channel()
    try:
        yield ch
    finally:
        await ch.close()


# ---------------------------------------------------------------------------
# Diagnostic hooks — enable `kill -USR1 <pid>` to dump stacks of every thread
# AND every live asyncio task, so we can see where a benchmark hang is parked.
# SIGUSR2 uses faulthandler, which is async-signal-safe; SIGUSR1 gives us the
# Python-level view (asyncio tasks) that faulthandler cannot provide.
# ---------------------------------------------------------------------------

def _dump_all_stacks(signum: int, frame: FrameType | None) -> None:
    try:
        out = sys.stderr
        out.write("\n==== SIGUSR1 STACK DUMP ====\n")
        out.write(f"time={time.strftime('%H:%M:%S')} pid={os.getpid()}\n\n")
        out.write("-- THREADS --\n")
        frames = sys._current_frames()
        main_tid = threading_main_tid()
        for tid, f in frames.items():
            # For the interrupted main thread, prefer the `frame` argument —
            # it points to the user code site, not the signal handler.
            effective = frame if (main_tid is not None and tid == main_tid and frame is not None) else f
            out.write(f"\nThread {tid}:\n")
            traceback.print_stack(effective, file=out)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        tasks = asyncio.all_tasks(loop) if loop is not None else set()
        out.write(f"\n-- ASYNCIO TASKS ({len(tasks)}) --\n")
        for t in tasks:
            out.write(
                f"\nTask {t.get_name()!r} done={t.done()} cancelled={t.cancelled()}\n"
            )
            # Task.print_stack() renders the full coroutine call chain; frame-
            # by-frame traceback.print_stack(limit=1) collapsed each await to
            # a single line and hid the async shape we need for debugging.
            t.print_stack(file=out)
        out.write("==== END STACK DUMP ====\n\n")
        out.flush()
    except BaseException as exc:  # noqa: BLE001 — handler must never leak
        try:
            sys.stderr.write(f"[bench] SIGUSR1 handler failed: {exc!r}\n")
        except Exception:
            pass


def threading_main_tid() -> int | None:
    """Return the main-thread ID if known (used only inside the dump)."""
    import threading

    try:
        return threading.main_thread().ident
    except Exception:
        return None


try:
    signal.signal(signal.SIGUSR1, _dump_all_stacks)
    faulthandler.register(signal.SIGUSR2, file=sys.__stderr__, all_threads=True)
except (AttributeError, OSError, ValueError) as _exc:
    # SIGUSR1/SIGUSR2 are unavailable on Windows and may be claimed by a
    # parent supervisor. Tell the operator rather than swallow silently.
    sys.stderr.write(f"[bench] stack-dump handlers unavailable: {_exc!r}\n")


# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
VALKEY_URL = os.getenv("VALKEY_URL", "valkey://localhost:6379/0")

QUEUE_NAME = "bench_queue"
RESULT_CHANNEL = "bench_results"

# Upper bound for any per-scenario completion wait. 5 minutes is generous
# for benchmark-scale workloads (n up to ~500k) but catches true hangs
# long before CI or a human loses patience. On timeout we raise rather
# than let the whole suite stall silently; stack dumps via SIGUSR1/USR2
# give the operator the exact parking point.
_BENCH_HANG_TIMEOUT = 300.0


# ---------------------------------------------------------------------------
# Ergebnis-Datenstruktur
# ---------------------------------------------------------------------------

@dataclass
class BenchResult:
    broker: str
    scenario: str
    messages: int
    concurrency: int
    duration_s: float
    throughput_msg_s: float
    latencies_ms: list[float] = field(default_factory=list)
    extra: dict = field(default_factory=dict)

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

    @property
    def avg(self) -> float:
        if not self.latencies_ms:
            return 0.0
        return statistics.mean(self.latencies_ms)


def print_result(r: BenchResult) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {r.broker:>10} | {r.scenario}")
    print(f"{'=' * 60}")
    print(f"  Messages:    {r.messages:>10,}")
    print(f"  Concurrency: {r.concurrency:>10}")
    print(f"  Duration:    {r.duration_s:>10.3f} s")
    print(f"  Throughput:  {r.throughput_msg_s:>10,.0f} msg/s")
    if r.latencies_ms:
        print(f"  Latency AVG: {r.avg:>10.3f} ms")
        print(f"  Latency P50: {r.p50:>10.3f} ms")
        print(f"  Latency P95: {r.p95:>10.3f} ms")
        print(f"  Latency P99: {r.p99:>10.3f} ms")
    for k, v in r.extra.items():
        print(f"  {k}: {v}")
    print()


# ---------------------------------------------------------------------------
# Payload-Helfer
# ---------------------------------------------------------------------------

def make_payload(size_bytes: int = 256) -> bytes:
    """Erzeugt eine realistische JSON-Task-Message mit Padding."""
    msg = {
        "task": {"id": "550e8400-e29b-41d4-a716-446655440000", "queue": QUEUE_NAME, "name": "example_task"},
        "args": [1, 2, 3],
        "kwargs": {"key": "value"},
    }
    base = json.dumps(msg).encode()
    if len(base) >= size_bytes:
        return base
    # Padding um gewuenschte Groesse zu erreichen
    msg["_pad"] = "x" * (size_bytes - len(base) - 12)
    return json.dumps(msg).encode()


# ---------------------------------------------------------------------------
# RabbitMQ-Implementierung
# ---------------------------------------------------------------------------

class RabbitMQBench:
    def __init__(self) -> None:
        self.connection: aio_pika.abc.AbstractRobustConnection | None = None

    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async def close(self) -> None:
        if self.connection:
            await self.connection.close()

    async def _ensure_connection(self) -> aio_pika.abc.AbstractRobustConnection:
        """Return a live RobustConnection, reconnecting if the underlying
        aiormq socket is dead.

        A previous scenario can leave the shared connection in a state
        where ``RobustConnection.is_closed`` still reports ``False`` but
        the underlying aiormq connection is actually closed (observed
        after a bursty close-storm when multiple consumer channels tear
        down concurrently in full_pipeline). We therefore probe the
        connection by opening a throwaway channel; on failure we rebuild
        the RobustConnection outright.
        """
        if self.connection is None or self.connection.is_closed:
            self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
            return self.connection
        # Probe: try to open a channel. If the aiormq socket is dead the
        # RobustConnection won't reopen it on demand and we get a stale
        # 'closed' error. Replace the connection in that case.
        try:
            probe_ch = await self.connection.channel()
            await probe_ch.close()
        except (RuntimeError, aio_pika.exceptions.AMQPError, OSError):
            try:
                await self.connection.close()
            except Exception:
                pass
            self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
        return self.connection

    async def purge(self) -> None:
        conn = await self._ensure_connection()
        async with _bench_channel(conn) as ch:
            queue = await ch.declare_queue(QUEUE_NAME, durable=True)
            await queue.purge()

    # --- Szenario 1: Enqueue-Throughput ---
    async def enqueue_throughput(self, n: int, concurrency: int, payload: bytes) -> BenchResult:
        assert self.connection
        async with _bench_channel(self.connection) as ch:
            await ch.declare_queue(QUEUE_NAME, durable=True)

            sem = asyncio.Semaphore(concurrency)
            msg = AMQPMessage(payload, delivery_mode=DeliveryMode.PERSISTENT)

            async def publish_one() -> None:
                async with sem:
                    await ch.default_exchange.publish(msg, routing_key=QUEUE_NAME)

            t0 = time.perf_counter()
            await asyncio.gather(*(publish_one() for _ in range(n)))
            dur = time.perf_counter() - t0

        return BenchResult("RabbitMQ", "enqueue_throughput", n, concurrency, dur, n / dur)

    # --- Szenario 2: Dequeue-Throughput ---
    async def dequeue_throughput(self, n: int, concurrency: int) -> BenchResult:
        assert self.connection
        async with _bench_channel(self.connection) as ch:
            await ch.set_qos(prefetch_count=concurrency)
            queue = await ch.declare_queue(QUEUE_NAME, durable=True)

            count = 0
            done = asyncio.Event()

            async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
                nonlocal count
                await message.ack()
                count += 1
                if count >= n:
                    done.set()

            consumer_tag = await queue.consume(on_message)
            t0 = time.perf_counter()
            await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
            dur = time.perf_counter() - t0

            await queue.cancel(consumer_tag)
        return BenchResult("RabbitMQ", "dequeue_throughput", n, concurrency, dur, n / dur)

    # --- Szenario 3: Round-Trip-Latenz ---
    async def roundtrip_latency(self, n: int) -> BenchResult:
        assert self.connection
        async with (
            _bench_channel(self.connection) as ch_pub,
            _bench_channel(self.connection) as ch_sub,
        ):
            await ch_sub.set_qos(prefetch_count=1)
            queue = await ch_sub.declare_queue(QUEUE_NAME, durable=True)

            latencies: list[float] = []
            received = asyncio.Event()
            count = 0

            async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
                nonlocal count
                t_sent = float(message.body.split(b"|")[0])
                latencies.append((time.perf_counter() - t_sent) * 1000)
                await message.ack()
                count += 1
                if count >= n:
                    received.set()

            consumer_tag = await queue.consume(on_message)

            t0 = time.perf_counter()
            for _ in range(n):
                body = f"{time.perf_counter()}|ping".encode()
                msg = AMQPMessage(body, delivery_mode=DeliveryMode.PERSISTENT)
                await ch_pub.default_exchange.publish(msg, routing_key=QUEUE_NAME)

            await asyncio.wait_for(received.wait(), timeout=_BENCH_HANG_TIMEOUT)
            dur = time.perf_counter() - t0

            await queue.cancel(consumer_tag)
        return BenchResult("RabbitMQ", "roundtrip_latency", n, 1, dur, n / dur, latencies)

    # --- Szenario 4+5: Concurrent Producers / Consumers ---
    async def concurrent_enqueue(self, n: int, concurrency: int, payload: bytes) -> BenchResult:
        assert self.connection

        per_producer = n // concurrency
        remainder = n % concurrency

        async def producer(count: int) -> None:
            assert self.connection
            async with _bench_channel(self.connection) as ch:
                await ch.declare_queue(QUEUE_NAME, durable=True)
                msg = AMQPMessage(payload, delivery_mode=DeliveryMode.PERSISTENT)
                for _ in range(count):
                    await ch.default_exchange.publish(msg, routing_key=QUEUE_NAME)

        t0 = time.perf_counter()
        tasks = []
        for i in range(concurrency):
            c = per_producer + (1 if i < remainder else 0)
            tasks.append(producer(c))
        await asyncio.gather(*tasks)
        dur = time.perf_counter() - t0

        return BenchResult("RabbitMQ", "concurrent_enqueue", n, concurrency, dur, n / dur)

    async def concurrent_dequeue(self, n: int, concurrency: int) -> BenchResult:
        assert self.connection

        count = 0
        done = asyncio.Event()
        lock = asyncio.Lock()

        async def consumer() -> None:
            nonlocal count
            assert self.connection
            # _bench_channel guarantees ch.close() even on task.cancel() —
            # `async with queue.iterator()` in turn guarantees the
            # consumer is cancelled broker-side and the iterator task
            # tree is torn down (otherwise aiormq leaves orphaned
            # _reader tasks behind).
            async with _bench_channel(self.connection) as ch:
                await ch.set_qos(prefetch_count=10)
                queue = await ch.declare_queue(QUEUE_NAME, durable=True)
                async with queue.iterator() as it:
                    async for message in it:
                        await message.ack()
                        async with lock:
                            count += 1
                            if count >= n:
                                done.set()
                        if done.is_set():
                            break

        t0 = time.perf_counter()
        tasks = [asyncio.create_task(consumer()) for _ in range(concurrency)]
        await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
        dur = time.perf_counter() - t0

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        return BenchResult("RabbitMQ", "concurrent_dequeue", n, concurrency, dur, n / dur)

    # --- Szenario 6: Full Pipeline ---
    async def full_pipeline(self, n: int, producers: int, consumers: int, payload: bytes) -> BenchResult:
        assert self.connection

        consumed = 0
        done = asyncio.Event()
        lock = asyncio.Lock()
        latencies: list[float] = []

        per_producer = n // producers
        remainder = n % producers

        async def producer(count: int) -> None:
            assert self.connection
            async with _bench_channel(self.connection) as ch:
                await ch.declare_queue(QUEUE_NAME, durable=True)
                for _ in range(count):
                    body = f"{time.perf_counter()}|".encode() + payload
                    msg = AMQPMessage(body, delivery_mode=DeliveryMode.PERSISTENT)
                    await ch.default_exchange.publish(msg, routing_key=QUEUE_NAME)

        async def consumer() -> None:
            nonlocal consumed
            assert self.connection
            # See concurrent_dequeue: _bench_channel + queue.iterator()
            # together guarantee clean teardown on cancel.
            async with _bench_channel(self.connection) as ch:
                await ch.set_qos(prefetch_count=10)
                queue = await ch.declare_queue(QUEUE_NAME, durable=True)
                async with queue.iterator() as it:
                    async for message in it:
                        t_sent = float(message.body.split(b"|", 1)[0])
                        lat = (time.perf_counter() - t_sent) * 1000
                        await message.ack()
                        async with lock:
                            latencies.append(lat)
                            consumed += 1
                            if consumed >= n:
                                done.set()
                        if done.is_set():
                            break

        t0 = time.perf_counter()
        consumer_tasks = [asyncio.create_task(consumer()) for _ in range(consumers)]
        # Kurz warten damit Consumer bereit sind
        await asyncio.sleep(0.1)
        producer_tasks = []
        for i in range(producers):
            c = per_producer + (1 if i < remainder else 0)
            producer_tasks.append(asyncio.create_task(producer(c)))

        try:
            await asyncio.gather(*producer_tasks)
            await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
            dur = time.perf_counter() - t0
        finally:
            # Always tear down consumer tasks — even when a producer raises or
            # done.wait() times out — otherwise orphaned consumers keep
            # aiormq reader tasks alive into the next scenario.
            for t in consumer_tasks:
                t.cancel()
            await asyncio.gather(*consumer_tasks, return_exceptions=True)

        return BenchResult(
            "RabbitMQ", "full_pipeline", n, producers + consumers,
            dur, n / dur, latencies,
            extra={"producers": producers, "consumers": consumers},
        )

    # --- Szenario 8: Pub/Sub Result-Notify ---
    async def pubsub_notify(self, n: int) -> BenchResult:
        """RabbitMQ Fanout-Exchange fuer Result-Benachrichtigungen."""
        assert self.connection
        async with (
            _bench_channel(self.connection) as ch_pub,
            _bench_channel(self.connection) as ch_sub,
        ):
            exchange = await ch_pub.declare_exchange(
                "bench_results_fanout", aio_pika.ExchangeType.FANOUT
            )
            queue = await ch_sub.declare_queue("", exclusive=True)
            await queue.bind(exchange)

            latencies: list[float] = []
            count = 0
            done = asyncio.Event()

            async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
                nonlocal count
                t_sent = float(message.body.split(b"|")[0])
                latencies.append((time.perf_counter() - t_sent) * 1000)
                await message.ack()
                count += 1
                if count >= n:
                    done.set()

            consumer_tag = await queue.consume(on_message)

            t0 = time.perf_counter()
            for _ in range(n):
                body = f"{time.perf_counter()}|result_ready".encode()
                msg = AMQPMessage(body)
                await exchange.publish(msg, routing_key="")
            await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
            dur = time.perf_counter() - t0

            await queue.cancel(consumer_tag)

        return BenchResult("RabbitMQ", "pubsub_notify", n, 1, dur, n / dur, latencies)

    # --- Szenario 9: Ack-Patterns ---
    async def ack_after_process(self, n: int, work_ms: float) -> BenchResult:
        """Simuliert ack-after-process: erst Arbeit, dann ack."""
        assert self.connection
        async with _bench_channel(self.connection) as ch:
            await ch.set_qos(prefetch_count=10)
            queue = await ch.declare_queue(QUEUE_NAME, durable=True)

            count = 0
            done = asyncio.Event()

            async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
                nonlocal count
                await asyncio.sleep(work_ms / 1000)  # Simulierte Arbeit
                await message.ack()
                count += 1
                if count >= n:
                    done.set()

            consumer_tag = await queue.consume(on_message)
            t0 = time.perf_counter()
            await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
            dur = time.perf_counter() - t0

            await queue.cancel(consumer_tag)
        return BenchResult(
            "RabbitMQ", "ack_after_process", n, 1, dur, n / dur,
            extra={"simulated_work_ms": work_ms},
        )


# ---------------------------------------------------------------------------
# Valkey-Implementierung
# ---------------------------------------------------------------------------

class ValkeyBench:
    def __init__(self) -> None:
        self.client: Valkey | None = None

    async def connect(self) -> None:
        self.client = Valkey.from_url(VALKEY_URL, decode_responses=False)

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()

    async def purge(self) -> None:
        assert self.client
        await self.client.delete(QUEUE_NAME)
        # Stream aufraeuemen falls vorhanden
        try:
            await self.client.delete(f"{QUEUE_NAME}:stream")
        except Exception:
            pass

    # --- Szenario 1: Enqueue-Throughput (LIST: LPUSH) ---
    async def enqueue_throughput(self, n: int, concurrency: int, payload: bytes) -> BenchResult:
        assert self.client
        sem = asyncio.Semaphore(concurrency)

        async def push_one() -> None:
            async with sem:
                await self.client.lpush(QUEUE_NAME, payload)  # type: ignore[union-attr]

        t0 = time.perf_counter()
        await asyncio.gather(*(push_one() for _ in range(n)))
        dur = time.perf_counter() - t0

        return BenchResult("Valkey", "enqueue_throughput", n, concurrency, dur, n / dur)

    # --- Szenario 1b: Enqueue-Throughput (STREAM: XADD) ---
    async def enqueue_throughput_stream(self, n: int, concurrency: int, payload: bytes) -> BenchResult:
        assert self.client
        stream_key = f"{QUEUE_NAME}:stream"
        sem = asyncio.Semaphore(concurrency)

        async def push_one() -> None:
            async with sem:
                await self.client.xadd(stream_key, {"data": payload})  # type: ignore[union-attr]

        t0 = time.perf_counter()
        await asyncio.gather(*(push_one() for _ in range(n)))
        dur = time.perf_counter() - t0

        return BenchResult("Valkey/Stream", "enqueue_throughput", n, concurrency, dur, n / dur)

    # --- Szenario 2: Dequeue-Throughput (LIST: BRPOP) ---
    async def dequeue_throughput(self, n: int, concurrency: int) -> BenchResult:
        assert self.client
        count = 0
        done = asyncio.Event()
        lock = asyncio.Lock()

        async def pop_loop() -> None:
            nonlocal count
            while not done.is_set():
                result = await self.client.brpop(QUEUE_NAME, timeout=1)  # type: ignore[union-attr]
                if result is None:
                    continue
                async with lock:
                    count += 1
                    if count >= n:
                        done.set()
                        return

        t0 = time.perf_counter()
        tasks = [asyncio.create_task(pop_loop()) for _ in range(concurrency)]
        await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
        dur = time.perf_counter() - t0

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        return BenchResult("Valkey", "dequeue_throughput", n, concurrency, dur, n / dur)

    # --- Szenario 2b: Dequeue-Throughput (STREAM: XREADGROUP) ---
    async def dequeue_throughput_stream(self, n: int, concurrency: int) -> BenchResult:
        assert self.client
        stream_key = f"{QUEUE_NAME}:stream"
        group = "bench_group"
        try:
            await self.client.xgroup_create(stream_key, group, id="0", mkstream=True)
        except Exception:
            pass  # Gruppe existiert bereits

        count = 0
        done = asyncio.Event()
        lock = asyncio.Lock()

        async def read_loop(consumer_name: str) -> None:
            nonlocal count
            while not done.is_set():
                results = await self.client.xreadgroup(  # type: ignore[union-attr]
                    group, consumer_name,
                    {stream_key: ">"},
                    count=10, block=1000,
                )
                if not results:
                    continue
                for _stream, messages in results:
                    for msg_id, _data in messages:
                        await self.client.xack(stream_key, group, msg_id)  # type: ignore[union-attr]
                        async with lock:
                            count += 1
                            if count >= n:
                                done.set()
                                return

        t0 = time.perf_counter()
        tasks = [asyncio.create_task(read_loop(f"c{i}")) for i in range(concurrency)]
        await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
        dur = time.perf_counter() - t0

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        return BenchResult("Valkey/Stream", "dequeue_throughput", n, concurrency, dur, n / dur)

    # --- Szenario 3: Round-Trip-Latenz ---
    async def roundtrip_latency(self, n: int) -> BenchResult:
        assert self.client
        latencies: list[float] = []
        count = 0
        done = asyncio.Event()

        async def consumer() -> None:
            nonlocal count
            while not done.is_set():
                result = await self.client.brpop(QUEUE_NAME, timeout=1)  # type: ignore[union-attr]
                if result is None:
                    continue
                t_sent = float(result[1].split(b"|")[0])
                latencies.append((time.perf_counter() - t_sent) * 1000)
                count += 1
                if count >= n:
                    done.set()
                    return

        consumer_task = asyncio.create_task(consumer())

        t0 = time.perf_counter()
        for _ in range(n):
            payload = f"{time.perf_counter()}|ping".encode()
            await self.client.lpush(QUEUE_NAME, payload)
        await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
        dur = time.perf_counter() - t0

        consumer_task.cancel()
        await asyncio.gather(consumer_task, return_exceptions=True)

        return BenchResult("Valkey", "roundtrip_latency", n, 1, dur, n / dur, latencies)

    # --- Szenario 4: Concurrent Enqueue ---
    async def concurrent_enqueue(self, n: int, concurrency: int, payload: bytes) -> BenchResult:
        assert self.client
        per_producer = n // concurrency
        remainder = n % concurrency

        async def producer(count: int) -> None:
            pipe = self.client.pipeline()  # type: ignore[union-attr]
            for i in range(count):
                pipe.lpush(QUEUE_NAME, payload)
                # Batch-Flush alle 500 Messages
                if (i + 1) % 500 == 0:
                    await pipe.execute()
                    pipe = self.client.pipeline()  # type: ignore[union-attr]
            await pipe.execute()

        t0 = time.perf_counter()
        tasks = []
        for i in range(concurrency):
            c = per_producer + (1 if i < remainder else 0)
            tasks.append(producer(c))
        await asyncio.gather(*tasks)
        dur = time.perf_counter() - t0

        return BenchResult("Valkey", "concurrent_enqueue", n, concurrency, dur, n / dur)

    # --- Szenario 5: Concurrent Dequeue ---
    async def concurrent_dequeue(self, n: int, concurrency: int) -> BenchResult:
        assert self.client
        count = 0
        done = asyncio.Event()
        lock = asyncio.Lock()

        async def consumer() -> None:
            nonlocal count
            while not done.is_set():
                result = await self.client.brpop(QUEUE_NAME, timeout=1)  # type: ignore[union-attr]
                if result is None:
                    continue
                async with lock:
                    count += 1
                    if count >= n:
                        done.set()
                        return

        t0 = time.perf_counter()
        tasks = [asyncio.create_task(consumer()) for _ in range(concurrency)]
        await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
        dur = time.perf_counter() - t0

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        return BenchResult("Valkey", "concurrent_dequeue", n, concurrency, dur, n / dur)

    # --- Szenario 6: Full Pipeline ---
    async def full_pipeline(self, n: int, producers: int, consumers: int, payload: bytes) -> BenchResult:
        assert self.client
        consumed = 0
        done = asyncio.Event()
        lock = asyncio.Lock()
        latencies: list[float] = []

        per_producer = n // producers
        remainder = n % producers

        async def producer(count: int) -> None:
            pipe = self.client.pipeline()  # type: ignore[union-attr]
            for i in range(count):
                ts = time.perf_counter()
                pipe.lpush(QUEUE_NAME, f"{ts}|".encode() + payload)
                if (i + 1) % 500 == 0:
                    await pipe.execute()
                    pipe = self.client.pipeline()  # type: ignore[union-attr]
            await pipe.execute()

        async def consumer() -> None:
            nonlocal consumed
            while not done.is_set():
                result = await self.client.brpop(QUEUE_NAME, timeout=1)  # type: ignore[union-attr]
                if result is None:
                    continue
                parts = result[1].split(b"|", 1)
                t_sent = float(parts[0])
                lat = (time.perf_counter() - t_sent) * 1000
                async with lock:
                    latencies.append(lat)
                    consumed += 1
                    if consumed >= n:
                        done.set()
                        return

        t0 = time.perf_counter()
        consumer_tasks = [asyncio.create_task(consumer()) for _ in range(consumers)]
        await asyncio.sleep(0.05)
        producer_tasks = []
        for i in range(producers):
            c = per_producer + (1 if i < remainder else 0)
            producer_tasks.append(asyncio.create_task(producer(c)))

        await asyncio.gather(*producer_tasks)
        await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
        dur = time.perf_counter() - t0

        for t in consumer_tasks:
            t.cancel()
        await asyncio.gather(*consumer_tasks, return_exceptions=True)

        return BenchResult(
            "Valkey", "full_pipeline", n, producers + consumers,
            dur, n / dur, latencies,
            extra={"producers": producers, "consumers": consumers},
        )

    # --- Szenario 8: Pub/Sub Result-Notify ---
    async def pubsub_notify(self, n: int) -> BenchResult:
        assert self.client
        latencies: list[float] = []
        count = 0
        done = asyncio.Event()

        pubsub = self.client.pubsub()
        listener_task: asyncio.Task | None = None
        try:
            await pubsub.subscribe(RESULT_CHANNEL)

            async def listener() -> None:
                nonlocal count
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    t_sent = float(message["data"].split(b"|")[0])
                    latencies.append((time.perf_counter() - t_sent) * 1000)
                    count += 1
                    if count >= n:
                        done.set()
                        return

            listener_task = asyncio.create_task(listener())
            await asyncio.sleep(0.05)  # Subscriber bereit

            t0 = time.perf_counter()
            pipe = self.client.pipeline()
            for i in range(n):
                pipe.publish(RESULT_CHANNEL, f"{time.perf_counter()}|result")
                if (i + 1) % 500 == 0:
                    await pipe.execute()
                    pipe = self.client.pipeline()
            await pipe.execute()
            await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
            dur = time.perf_counter() - t0
        finally:
            # Ensure the listener task and the pubsub connection are always
            # torn down, even when done.wait() times out or the publish
            # pipeline raises. Otherwise the next run reuses a half-closed
            # pubsub (the Valkey equivalent of the aiormq _reader leak).
            if listener_task is not None:
                listener_task.cancel()
                await asyncio.gather(listener_task, return_exceptions=True)
            await pubsub.unsubscribe(RESULT_CHANNEL)
            await pubsub.aclose()

        return BenchResult("Valkey", "pubsub_notify", n, 1, dur, n / dur, latencies)

    # --- Szenario 9: Ack-Patterns (Streams) ---
    async def ack_after_process(self, n: int, work_ms: float) -> BenchResult:
        assert self.client
        stream_key = f"{QUEUE_NAME}:stream"
        group = "bench_ack_group"
        consumer_name = "ack_consumer"

        try:
            await self.client.xgroup_create(stream_key, group, id="0", mkstream=True)
        except Exception:
            pass

        count = 0
        done = asyncio.Event()

        async def read_loop() -> None:
            nonlocal count
            while not done.is_set():
                results = await self.client.xreadgroup(
                    group, consumer_name,
                    {stream_key: ">"},
                    count=1, block=1000,
                )
                if not results:
                    continue
                for _stream, messages in results:
                    for msg_id, _data in messages:
                        await asyncio.sleep(work_ms / 1000)  # Simulierte Arbeit
                        await self.client.xack(stream_key, group, msg_id)
                        count += 1
                        if count >= n:
                            done.set()
                            return

        t0 = time.perf_counter()
        task = asyncio.create_task(read_loop())
        await asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)
        dur = time.perf_counter() - t0

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        return BenchResult(
            "Valkey/Stream", "ack_after_process", n, 1, dur, n / dur,
            extra={"simulated_work_ms": work_ms},
        )


# ---------------------------------------------------------------------------
# Szenario-Runner
# ---------------------------------------------------------------------------

async def run_scenario(
    name: str,
    rmq: RabbitMQBench,
    vk: ValkeyBench,
    messages: int,
    concurrency: int,
) -> list[BenchResult]:
    results: list[BenchResult] = []
    payload_256 = make_payload(256)
    payload_1k = make_payload(1024)
    payload_10k = make_payload(10240)

    if name in ("enqueue_throughput", "all"):
        print("\n--- Szenario 1: Enqueue-Throughput ---")
        await rmq.purge()
        r = await rmq.enqueue_throughput(messages, concurrency, payload_256)
        print_result(r)
        results.append(r)
        await rmq.purge()

        await vk.purge()
        r = await vk.enqueue_throughput(messages, concurrency, payload_256)
        print_result(r)
        results.append(r)

        r = await vk.enqueue_throughput_stream(messages, concurrency, payload_256)
        print_result(r)
        results.append(r)
        await vk.purge()

    if name in ("dequeue_throughput", "all"):
        print("\n--- Szenario 2: Dequeue-Throughput ---")
        # RabbitMQ: erst befuellen, dann konsumieren
        await rmq.purge()
        await rmq.enqueue_throughput(messages, concurrency, payload_256)
        r = await rmq.dequeue_throughput(messages, concurrency)
        print_result(r)
        results.append(r)

        # Valkey LIST
        await vk.purge()
        await vk.enqueue_throughput(messages, concurrency, payload_256)
        r = await vk.dequeue_throughput(messages, concurrency)
        print_result(r)
        results.append(r)

        # Valkey STREAM
        await vk.purge()
        await vk.enqueue_throughput_stream(messages, concurrency, payload_256)
        r = await vk.dequeue_throughput_stream(messages, concurrency)
        print_result(r)
        results.append(r)

    if name in ("roundtrip_latency", "all"):
        print("\n--- Szenario 3: Round-Trip-Latenz ---")
        n_lat = min(messages, 5000)  # Latenz-Tests mit weniger Messages
        await rmq.purge()
        r = await rmq.roundtrip_latency(n_lat)
        print_result(r)
        results.append(r)

        await vk.purge()
        r = await vk.roundtrip_latency(n_lat)
        print_result(r)
        results.append(r)

    if name in ("concurrent_enqueue", "all"):
        print("\n--- Szenario 4: Concurrent Producers ---")
        await rmq.purge()
        r = await rmq.concurrent_enqueue(messages, concurrency, payload_256)
        print_result(r)
        results.append(r)
        await rmq.purge()

        await vk.purge()
        r = await vk.concurrent_enqueue(messages, concurrency, payload_256)
        print_result(r)
        results.append(r)
        await vk.purge()

    if name in ("concurrent_dequeue", "all"):
        print("\n--- Szenario 5: Concurrent Consumers ---")
        await rmq.purge()
        await rmq.enqueue_throughput(messages, concurrency, payload_256)
        r = await rmq.concurrent_dequeue(messages, concurrency)
        print_result(r)
        results.append(r)

        await vk.purge()
        await vk.enqueue_throughput(messages, concurrency, payload_256)
        r = await vk.concurrent_dequeue(messages, concurrency)
        print_result(r)
        results.append(r)

    if name in ("full_pipeline", "all"):
        print("\n--- Szenario 6: Full Pipeline (Producers + Consumers gleichzeitig) ---")
        producers = max(1, concurrency // 2)
        consumers = max(1, concurrency - producers)

        await rmq.purge()
        r = await rmq.full_pipeline(messages, producers, consumers, payload_256)
        print_result(r)
        results.append(r)

        await vk.purge()
        r = await vk.full_pipeline(messages, producers, consumers, payload_256)
        print_result(r)
        results.append(r)

    if name in ("payload_sizes", "all"):
        print("\n--- Szenario 7: Payload-Groessen ---")
        for label, payload in [("256B", payload_256), ("1KB", payload_1k), ("10KB", payload_10k)]:
            await rmq.purge()
            r = await rmq.enqueue_throughput(messages, concurrency, payload)
            r.scenario = f"payload_{label}"
            print_result(r)
            results.append(r)
            await rmq.purge()

            await vk.purge()
            r = await vk.enqueue_throughput(messages, concurrency, payload)
            r.scenario = f"payload_{label}"
            print_result(r)
            results.append(r)
            await vk.purge()

    if name in ("pubsub_notify", "all"):
        print("\n--- Szenario 8: Pub/Sub Result-Benachrichtigung ---")
        n_ps = min(messages, 10000)
        await rmq.purge()
        r = await rmq.pubsub_notify(n_ps)
        print_result(r)
        results.append(r)

        r = await vk.pubsub_notify(n_ps)
        print_result(r)
        results.append(r)

    if name in ("ack_patterns", "all"):
        print("\n--- Szenario 9: Ack-after-Process (1ms simulierte Arbeit) ---")
        n_ack = min(messages, 5000)

        # RabbitMQ: befuellen
        await rmq.purge()
        await rmq.enqueue_throughput(n_ack, concurrency, payload_256)
        r = await rmq.ack_after_process(n_ack, work_ms=1.0)
        print_result(r)
        results.append(r)

        # Valkey Streams: befuellen
        await vk.purge()
        await vk.enqueue_throughput_stream(n_ack, concurrency, payload_256)
        r = await vk.ack_after_process(n_ack, work_ms=1.0)
        print_result(r)
        results.append(r)

    return results


# ---------------------------------------------------------------------------
# Zusammenfassung
# ---------------------------------------------------------------------------

def print_summary(results: list[BenchResult]) -> None:
    print("\n")
    print("=" * 80)
    print("  ZUSAMMENFASSUNG")
    print("=" * 80)
    print(f"  {'Broker':<16} {'Szenario':<25} {'msg/s':>10} {'P50ms':>8} {'P95ms':>8} {'P99ms':>8}")
    print("-" * 80)

    for r in results:
        p50 = f"{r.p50:.2f}" if r.latencies_ms else "—"
        p95 = f"{r.p95:.2f}" if r.latencies_ms else "—"
        p99 = f"{r.p99:.2f}" if r.latencies_ms else "—"
        print(f"  {r.broker:<16} {r.scenario:<25} {r.throughput_msg_s:>10,.0f} {p50:>8} {p95:>8} {p99:>8}")

    print("=" * 80)

    # Gewinner pro Szenario
    scenarios = {}
    for r in results:
        scenarios.setdefault(r.scenario, []).append(r)

    print("\n  GEWINNER PRO SZENARIO:")
    print("-" * 60)
    for scenario, entries in scenarios.items():
        winner = max(entries, key=lambda x: x.throughput_msg_s)
        print(f"  {scenario:<30} -> {winner.broker} ({winner.throughput_msg_s:,.0f} msg/s)")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    parser = argparse.ArgumentParser(description="SwarmQ Broker Benchmark: RabbitMQ vs Valkey")
    parser.add_argument("--messages", "-n", type=int, default=50_000, help="Anzahl Messages pro Test (Default: 50000)")
    parser.add_argument("--concurrency", "-c", type=int, default=10, help="Async-Concurrency (Default: 10)")
    parser.add_argument(
        "--scenario", "-s", type=str, default="all",
        choices=[
            "all", "enqueue_throughput", "dequeue_throughput", "roundtrip_latency",
            "concurrent_enqueue", "concurrent_dequeue", "full_pipeline",
            "payload_sizes", "pubsub_notify", "ack_patterns",
        ],
        help="Welches Szenario (Default: all)",
    )
    args = parser.parse_args()

    print(f"\nSwarmQ Broker Benchmark")
    print(f"Messages: {args.messages:,} | Concurrency: {args.concurrency} | Szenario: {args.scenario}")
    print(f"RabbitMQ: {RABBITMQ_URL}")
    print(f"Valkey:   {VALKEY_URL}")

    rmq = RabbitMQBench()
    vk = ValkeyBench()

    print("\nVerbinde zu Brokern...")
    await rmq.connect()
    await vk.connect()
    print("Verbunden.\n")

    try:
        # Warmup
        print("Warmup...")
        warmup_payload = make_payload(256)
        await rmq.purge()
        await rmq.enqueue_throughput(1000, 10, warmup_payload)
        await rmq.dequeue_throughput(1000, 10)
        await vk.purge()
        await vk.enqueue_throughput(1000, 10, warmup_payload)
        await vk.dequeue_throughput(1000, 10)
        await rmq.purge()
        await vk.purge()
        print("Warmup abgeschlossen.\n")

        results = await run_scenario(args.scenario, rmq, vk, args.messages, args.concurrency)
        print_summary(results)
    finally:
        await rmq.close()
        await vk.close()


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
        print("(uvloop aktiviert)")
    except ImportError:
        pass

    asyncio.run(main())
