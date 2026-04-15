# Broker-Benchmark: RabbitMQ vs Valkey

**Erstellt:** 2026-04-14
**Zweck:** Referenzdokument mit allen Benchmark-Ergebnissen zur Broker-Wahl fuer SwarmQ.
**Setup:** Docker (RabbitMQ 4 Management Alpine, Valkey 8 Alpine), Python 3.12, uvloop, aio-pika 9.6, valkey-py 6.x

---

## Inhaltsverzeichnis

1. [Testumgebung](#1-testumgebung)
2. [Synthetischer Benchmark — Rohe Broker-Performance](#2-synthetischer-benchmark)
3. [Realistischer Benchmark — SwarmQ-Pattern](#3-realistischer-benchmark)
4. [Analyse und Interpretation](#4-analyse-und-interpretation)
5. [Entscheidung und Begruendung](#5-entscheidung-und-begruendung)

---

# 1. Testumgebung

| Komponente | Detail |
|---|---|
| **OS** | Linux 6.17.0-1017-oem |
| **Python** | 3.12 mit uvloop |
| **RabbitMQ** | 4-management-alpine (Docker) |
| **Valkey** | 8-alpine (Docker), AOF Persistenz, 512MB maxmemory |
| **RabbitMQ Client** | aio-pika 9.6.2 (async AMQP) |
| **Valkey Client** | valkey-py 6.x (async) |
| **Messages** | JSON-Payload 256 Bytes (realistische Task-Message) |
| **Persistenz** | RabbitMQ: persistent delivery. Valkey: appendonly yes |

Alle Tests mit Warmup-Phase. Jedes Szenario isoliert (Queue vor jedem Test gepurged).

---

# 2. Synthetischer Benchmark

Misst den rohen Broker-Overhead ohne simulierte Arbeit.
Datei: `benchmarks/bench.py`

## 2.1 Enqueue-Throughput (20.000 Messages, Concurrency 10)

Wie schnell koennen Messages in die Queue geschrieben werden?

| Broker | msg/s | Dauer |
|---|---|---|
| **Valkey (LIST/LPUSH)** | **34,128** | 0.586s |
| Valkey (STREAM/XADD) | 31,591 | 0.633s |
| RabbitMQ (AMQP Publish) | 14,225 | 1.406s |

**Gewinner: Valkey (2.4x schneller)**

## 2.2 Dequeue-Throughput (20.000 Messages, Concurrency 10)

Wie schnell koennen Messages konsumiert und geackt werden?

| Broker | msg/s | Dauer |
|---|---|---|
| **Valkey (LIST/BRPOP)** | **35,318** | 0.566s |
| Valkey (STREAM/XREADGROUP+XACK) | 32,716 | 0.611s |
| RabbitMQ (AMQP Consume+Ack) | 14,447 | 1.384s |

**Gewinner: Valkey (2.4x schneller)**

## 2.3 Round-Trip-Latenz (5.000 Messages, Single Producer/Consumer)

Enqueue → Dequeue → Ack Latenz.

| Broker | msg/s | P50 | P95 | P99 |
|---|---|---|---|---|
| **Valkey (LIST)** | **27,976** | 0.17ms | 0.26ms | 0.66ms |
| RabbitMQ (AMQP) | 5,536 | **0.15ms** | **0.25ms** | **0.38ms** |

**Throughput: Valkey (5.1x). Latenz: nahezu gleich** — RabbitMQ hat minimal bessere P99 (0.38ms vs 0.66ms).

## 2.4 Concurrent Producers (20.000 Messages, 10 parallele Channels/Connections)

| Broker | msg/s | Dauer |
|---|---|---|
| **Valkey (Pipeline-Batches)** | **118,843** | 0.168s |
| RabbitMQ (10 separate Channels) | 22,709 | 0.881s |

**Gewinner: Valkey (5.2x schneller)** — Valkeys Pipeline-Batching ist extrem effizient fuer parallele Writes.

## 2.5 Concurrent Consumers (20.000 Messages, 10 parallele Consumer)

| Broker | msg/s | Dauer |
|---|---|---|
| **Valkey (10x BRPOP)** | **37,812** | 0.529s |
| RabbitMQ (10x async iterate) | 21,539 | 0.929s |

**Gewinner: Valkey (1.8x schneller)**

## 2.6 Full Pipeline (20.000 Messages, 5 Producers + 5 Consumers gleichzeitig)

| Broker | msg/s | P50 Latenz | P95 Latenz | P99 Latenz |
|---|---|---|---|---|
| **Valkey** | **31,616** | 287.15ms | 432.03ms | 443.61ms |
| RabbitMQ | 8,394 | **0.61ms** | **1.26ms** | **1.40ms** |

**Throughput: Valkey (3.8x). Latenz: RabbitMQ (470x besser)**

Erklaerung: Valkey hat hoeheren Throughput durch Pipeline-Batching, aber die Latenz leidet massiv darunter — Messages werden in Batches geschrieben und erst spaeter einzeln gelesen. RabbitMQ liefert push-basiert und hat dadurch sub-Millisekunden-Latenz.

## 2.7 Payload-Groessen (20.000 Messages, Concurrency 10)

| Payload | RabbitMQ | Valkey | Valkey-Vorteil |
|---|---|---|---|
| 256 Bytes | 15,481 msg/s | **42,531** msg/s | 2.7x |
| 1 KB | 17,197 msg/s | **40,201** msg/s | 2.3x |
| 10 KB | 17,968 msg/s | **30,358** msg/s | 1.7x |

**Valkey gewinnt immer, aber der Vorteil schrumpft mit groesseren Payloads** (von 2.7x auf 1.7x). RabbitMQ's AMQP-Overhead wird bei groesseren Messages relativ kleiner.

## 2.8 Pub/Sub Result-Benachrichtigung (10.000 Messages)

Fuer SwarmQ's Result-Backend: Push-basierte Benachrichtigung wenn ein Task fertig ist.

| Broker | msg/s | P50 Latenz | P95 Latenz | P99 Latenz |
|---|---|---|---|---|
| **Valkey (Pub/Sub)** | **60,875** | 10.60ms | 18.41ms | 20.54ms |
| RabbitMQ (Fanout Exchange) | 6,345 | **0.18ms** | **0.36ms** | **0.48ms** |

**Throughput: Valkey (9.6x). Latenz: RabbitMQ (59x besser)**

Gleicher Trade-off wie bei Full Pipeline: Valkey's hoher Throughput kommt durch Batching, auf Kosten der Latenz. Fuer Result-Notifications ist die Latenz aber weniger kritisch als der Throughput.

## 2.9 Ack-after-Process (5.000 Messages, 1ms simulierte Arbeit, Single Consumer)

Simuliert den einfachsten Fall: eine Message lesen, kurz arbeiten, dann acken.

| Broker | msg/s |
|---|---|
| **RabbitMQ** | **6,509** |
| Valkey (STREAM XREADGROUP+XACK) | 823 |

**Gewinner: RabbitMQ (7.9x schneller)** — AMQP-Ack ist ein Frame auf der bestehenden Connection. Valkey XACK ist ein separater Redis-Roundtrip. Bei Single-Consumer ohne Batch-Reads wird das zum Flaschenhals.

---

# 3. Realistischer Benchmark

Simuliert das exakte SwarmQ-Pattern:
- Prefetcher/Runner-Architektur mit Semaphoren
- N concurrent async Worker die gleichzeitig Tasks abarbeiten
- Realistische async I/O-Arbeit (asyncio.sleep)
- Ack nach Verarbeitung (at-least-once)
- Valkey Streams mit Batch-Reads (count=5) und Consumer Groups

Datei: `benchmarks/bench_realistic.py`, 3.000 Messages pro Test.

## 3.1 Test A: Einfluss der Arbeitszeit (10 concurrent Worker)

| Arbeitszeit | RabbitMQ | Valkey/Stream | Valkey/List | Schnellster |
|---|---|---|---|---|
| 1ms | **7,851** msg/s | 6,762 | 6,789 | RabbitMQ (+16%) |
| 5ms | **1,862** msg/s | 1,787 | 1,708 | RabbitMQ (+4-9%) |
| 10ms | **958** msg/s | 906 | 913 | RabbitMQ (+5%) |
| 50ms | **197** msg/s | 194 | 195 | RabbitMQ (+1%) |

**RabbitMQ gewinnt in jedem Szenario.** Der Vorteil ist bei kurzer Arbeit (1ms) am groessten (+16%) und wird bei laengerer Arbeit (50ms) vernachlaessigbar (+1%), weil die Arbeitszeit den Broker-Overhead dominiert.

## 3.2 Test B: Concurrency-Skalierung (10ms Arbeit pro Task)

| Concurrent Worker | RabbitMQ | Valkey/Stream | Valkey/List | Schnellster |
|---|---|---|---|---|
| 1 | **96** msg/s | 91 | 91 | RabbitMQ (+5%) |
| 5 | **481** msg/s | 466 | 461 | RabbitMQ (+3%) |
| 10 | **944** msg/s | 900 | 908 | RabbitMQ (+4%) |
| 25 | **2,400** msg/s | 2,261 | 2,284 | RabbitMQ (+5%) |
| 50 | **4,592** msg/s | 4,225 | 4,276 | RabbitMQ (+7%) |
| 100 | **8,062** msg/s | 7,627 | 7,860 | RabbitMQ (+3-6%) |

**RabbitMQ gewinnt bei jeder Concurrency-Stufe.** Beide Systeme skalieren linear mit der Concurrency. Der Abstand bleibt konsistent bei 3-7%.

## 3.3 Latenz-Vergleich im realistischen Szenario (10ms Arbeit)

| Concurrent Worker | RabbitMQ P50 | Valkey/Stream P50 | Valkey/List P50 |
|---|---|---|---|
| 1 | 15,856ms | 16,530ms | 16,426ms |
| 10 | 1,853ms | 1,652ms | 1,709ms |
| 50 | 515ms | **369ms** | **385ms** |
| 100 | 433ms | **227ms** | **201ms** |

**Interessant: Bei hoher Concurrency hat Valkey bessere P50-Latenz** — die Messages werden gleichmaessiger auf die Consumer verteilt. RabbitMQ's Prefetch kann dazu fuehren, dass einige Consumer mehr Messages vorhalten als sie verarbeiten koennen.

---

# 4. Analyse und Interpretation

## 4.1 Warum Valkey im synthetischen Benchmark schneller ist

1. **Leichtgewichtige Befehle:** Redis/Valkey-Befehle (LPUSH, BRPOP) sind simpler als AMQP-Frames (Method, Header, Body).
2. **Pipeline-Batching:** Valkey erlaubt das Buendeln von Hunderten Befehlen in einer Pipeline. RabbitMQ muss jeden Publish einzeln ueber AMQP senden.
3. **Kein Routing-Overhead:** AMQP hat Exchange → Binding → Queue Routing. Valkey schreibt direkt in die Datenstruktur.
4. **Single-Threaded Event-Loop:** Valkey's Event-Loop ist extrem optimiert fuer kurze Befehle.

## 4.2 Warum RabbitMQ im realistischen Benchmark schneller ist

1. **Push-basierte Delivery:** RabbitMQ liefert Messages aktiv an Consumer. Valkey erfordert BRPOP/XREADGROUP (poll mit block).
2. **Natives Ack auf bestehender Connection:** AMQP-Ack ist ein einzelner Frame auf der bereits offenen TCP-Connection. Valkey XACK ist ein separater Redis-Command-Roundtrip.
3. **Prefetch-Buffer:** `prefetch_count=2` bedeutet, dass der naechste Task bereits im Client-Buffer liegt waehrend der aktuelle noch verarbeitet wird. Keine Wartezeit zwischen Tasks.
4. **Effizienteres Delivery bei Arbeit:** Sobald echte Arbeit anfaellt (>1ms), wird der Broker-Overhead relativ klein. RabbitMQ's Push-Modell minimiert die Zeit *zwischen* Tasks, was bei kurzer Arbeit den groessten Unterschied macht.

## 4.3 Valkey LIST vs. Valkey STREAM

| Aspekt | LIST (LPUSH/BRPOP) | STREAM (XADD/XREADGROUP) |
|---|---|---|
| **Throughput** | Etwas schneller | Etwas langsamer (XACK-Overhead) |
| **Message-Garantie** | At-most-once (Message nach BRPOP weg) | At-least-once (XACK erforderlich) |
| **Consumer Groups** | Nein | Ja |
| **Re-Delivery** | Nicht moeglich | Via XAUTOCLAIM nach idle_timeout |
| **Fazit** | Nur fuer Fire-and-forget geeignet | Einzige Option fuer zuverlaessige Queue |

Fuer SwarmQ kaeme nur Valkey STREAM in Frage (at-least-once erforderlich). LIST faellt wegen fehlender Ack-Semantik aus.

## 4.4 Trade-off: Throughput vs. Latenz

Der synthetische Benchmark zeigt zwei verschiedene Staerken:

| Metrik | Gewinner | Erklaerung |
|---|---|---|
| **Roher Throughput** | Valkey (2-5x) | Leichtgewichtige Befehle, Pipeline-Batching |
| **End-to-End-Latenz** | RabbitMQ (bis 470x) | Push-basierte Delivery, kein Polling |
| **Realistischer Throughput** | RabbitMQ (+3-16%) | Push + Prefetch eliminieren Wartezeit zwischen Tasks |

## 4.5 Wann Valkey als Broker Sinn machen wuerde

Valkey waere als Broker besser wenn:
- Tasks extrem kurzlebig sind (<0.1ms) und reiner Throughput zaehlt
- Keine Ack-Semantik benoetigt wird (fire-and-forget)
- Man bereits Valkey hat und keine zweite Infrastruktur will
- Die Queue nur als Notification-Kanal dient (Pub/Sub)

Keines dieser Szenarien trifft auf SwarmQ zu.

---

# 5. Entscheidung und Begruendung

## Empfohlene Architektur

| Rolle | Technologie | Begruendung |
|---|---|---|
| **Broker (Task-Queue)** | **RabbitMQ** | Schneller im realistischen Szenario (+3-16%), natives AMQP-Ack, Push-basierte Delivery, erstklassige DLQ, Prefetch-Buffer, bewiesene Zuverlaessigkeit |
| **Result-Backend** | **Valkey** | Hoher Pub/Sub-Throughput (60k msg/s) fuer Result-Notifications, schnelles Key-Value fuer Result-Storage, Sorted Sets fuer zeitbasiertes Scheduling |

## Zusammengefasste Benchmark-Daten

| Szenario | RabbitMQ | Valkey (bestes) | Gewinner |
|---|---|---|---|
| Enqueue Throughput | 14,225 msg/s | 34,128 msg/s | Valkey (2.4x) |
| Dequeue Throughput | 14,447 msg/s | 35,318 msg/s | Valkey (2.4x) |
| Round-Trip Latenz P50 | 0.15ms | 0.17ms | Gleich |
| Concurrent Enqueue | 22,709 msg/s | 118,843 msg/s | Valkey (5.2x) |
| Full Pipeline Throughput | 8,394 msg/s | 31,616 msg/s | Valkey (3.8x) |
| Full Pipeline Latenz P50 | 0.61ms | 287.15ms | RabbitMQ (470x) |
| Pub/Sub Throughput | 6,345 msg/s | 60,875 msg/s | Valkey (9.6x) |
| Pub/Sub Latenz P50 | 0.18ms | 10.60ms | RabbitMQ (59x) |
| Ack-after-Process (synth.) | 6,509 msg/s | 823 msg/s | RabbitMQ (7.9x) |
| **Realistisch 1ms/10 Worker** | **7,851 msg/s** | **6,789 msg/s** | **RabbitMQ (+16%)** |
| **Realistisch 10ms/10 Worker** | **958 msg/s** | **913 msg/s** | **RabbitMQ (+5%)** |
| **Realistisch 10ms/50 Worker** | **4,592 msg/s** | **4,276 msg/s** | **RabbitMQ (+7%)** |
| **Realistisch 10ms/100 Worker** | **8,062 msg/s** | **7,860 msg/s** | **RabbitMQ (+3%)** |

## Fazit

Valkey gewinnt bei rohem Broker-Overhead (Enqueue/Dequeue ohne Arbeit). **Aber sobald realistische Arbeit ins Spiel kommt — und das ist der einzig relevante Fall fuer einen Task Manager — gewinnt RabbitMQ konsistent.** Dazu kommen die architektonischen Vorteile: natives AMQP-Acknowledgment, Push-basierte Delivery, erstklassige Dead Letter Queues, und ein bewiesenes Zuverlaessigkeitsmodell.

Die Aufteilung RabbitMQ (Broker) + Valkey (Backend) nutzt die Staerken beider Systeme optimal.
