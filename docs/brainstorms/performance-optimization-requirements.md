---
date: 2026-04-15
topic: performance-optimization
---

# SwarmQ Performance-Optimierung

## Problem Frame

SwarmQ v2 erreicht auf RabbitMQ mit Quorum Queues 78 tasks/s (No-op, sequenziell) mit einer P50-Latenz von 12.5ms. Die Benchmark-Analyse zeigt, dass das Framework selbst effizient ist (schneller als TaskIQ auf demselben Broker), aber drei vermeidbare Overhead-Quellen 92% der Roundtrip-Latenz ausmachen:

```
Roundtrip 12.96ms = Polling     10.25ms (79%)
                  + Enqueue      1.62ms (13%)  ← davon ~1.2ms Quorum-Overhead
                  + Worker+Store 1.04ms (8%)
```

## Requirements

**Pub/Sub Result-Notification (größter Hebel)**

- R1. Worker published nach `store_result()` eine Notification auf einen Valkey Pub/Sub Channel. Die Notification ist ein reiner Hinweis — das Result in Valkey bleibt die Source of Truth.
- R2. Client subscribed auf den Notification-Channel (`sq:notify:{task_id}`, pro Task-ID) bevor er den Task enqueued, um das Race-Window zu minimieren. Pub/Sub ist fire-and-forget — verpasste Notifications sind kein Datenverlust.
- R2b. Nach Subscribe + Enqueue führt der Client einen kurzen Initial-Check (50ms) durch bevor er auf die Pub/Sub-Notification wartet. Das löst das häufigste Race-Window (Task fertig bevor Subscribe aktiv) in 50ms statt 1s auf.
- R3. Falls nach **1 Sekunde** keine Notification eintrifft, fällt der Client automatisch auf Polling (5ms-Intervall) zurück. Das Fallback-Timeout ist per `get_result()`-Aufruf konfigurierbar.
- R4. Die bestehende `get_result()`-API bleibt signatur-kompatibel. Pub/Sub ist der Default-Mechanismus, Polling der automatische Fallback.
- R5. Notification-Publish im Worker ist fire-and-forget mit Warning-Log bei Fehler. Ein Publish-Failure darf den Task-Ack nicht blockieren.

**Sicherheitsgarantien (keine Verschlechterung)**

- R6. Result-Delivery-Garantie bleibt identisch: Wenn ein Result in Valkey gespeichert ist, kann der Client es abrufen — unabhängig davon ob die Pub/Sub-Notification ankam.
- R7. Bei Valkey-Reconnect (Subscription verloren): automatischer Fallback auf Polling für alle laufenden `get_result()`-Aufrufe.
- R8. Bei Worker-Crash nach `store_result()` aber vor `publish()`: Result ist in Valkey, Polling-Fallback findet es.

**Batch-Operations**

- R9. Neue `schedule_many(tasks: list[tuple[str, dict]]) -> list[str]` API für effizientes Bulk-Publishing. Implementierungspfad in Planning.
- R10. Neue `get_results(task_ids: list[str]) -> list[Any]` API mit Valkey MGET. Blockiert mit Polling bis alle Results verfügbar oder Timeout.
- R11. Bestehende Einzeltask-APIs (`schedule()`, `get_result()`) bleiben unverändert.

**Queue-Type-Konfiguration**

- R12. `SwarmQConfig` bekommt `queue_type: str = "quorum"`. Global, nicht pro Queue. Wiring zu `RabbitMQBroker(queue_type=...)`.
- R13. Default bleibt "quorum" — kein Breaking Change.

**Dokumentation**

- R14. Docstring von `queue_type` warnt: Classic Queues verlieren Messages bei Broker-Crash.

## Success Criteria

- No-op Roundtrip-Latenz (P50) sinkt von 12.5ms auf unter 3ms (Pub/Sub + Quorum)
- No-op Throughput steigt von 78 auf mindestens 300 tasks/s (sequenziell, Quorum)
- Bei Pub/Sub-Ausfall: automatischer Fallback auf Polling innerhalb von 1s, keine verlorenen Results
- Mit Classic Queues: Roundtrip unter 1ms, Throughput über 1000 tasks/s
- Batch-Enqueue von 10.000 Tasks (happy path) mindestens 5× schneller als sequenzielle Einzel-Enqueues
- Bestehende API vollständig abwärtskompatibel
- Alle bestehenden Tests laufen ohne Änderung

## Scope Boundaries

- Keine Änderung am Worker-Concurrency-Modell (Semaphore bleibt)
- Kein Redis/Valkey-Broker-Adapter (separates Projekt)
- Keine Änderung der Serialisierungsstrategie (msgspec/msgpack bleibt)
- Kein Multi-Worker/Process-Manager (Phase 4 Feature)
- Keine Änderung an DLX-basierter Retry-Architektur
- Per-Queue queue_type deferred — aktuell nur global

## Key Decisions

- **Pub/Sub als Optimierungshinweis, nicht als Delivery-Mechanismus:** Das Result in Valkey ist die Source of Truth. Pub/Sub sagt nur "es ist jetzt da". Verpasste Notifications → Polling-Fallback. Kein Sicherheitsverlust.
- **1s Fallback-Timeout mit 50ms Initial-Check:** Subscribe → Enqueue → 50ms Check → Pub/Sub Wait (bis 1s) → Polling. Löst das häufigste Race-Window sofort, ohne den 1s-Fallback für echte Pub/Sub-Ausfälle zu verkürzen.
- **Channel-Naming Pro-Task-ID:** `sq:notify:{task_id}` statt pro Queue. Zero Noise, zero Client-Filtering, Channels sind billig.
- **Worker-Publish ist fire-and-forget:** Pub/Sub-Fehler blockieren niemals den Task-Ack. Lieber eine verpasste Notification als ein steckengebliebener Worker.
- **Queue-Type global, nicht pro Queue:** Einfacher. Per-Queue deferred.
- **Batch-API additiv:** `schedule()` und `get_result()` bleiben primäre API.

## Dependencies / Assumptions

- `queue_type` in `RabbitMQBroker` ist bereits implementiert (Benchmark-Commit). R12 erfordert Wiring über SwarmQConfig.
- **Client** (nicht Backend) braucht eine dedizierte Pub/Sub-Connection für Subscribe (Subscribe-Mode blockiert GET/SET). Der Worker publiziert Notifications über die normale Backend-Connection. `valkey.asyncio.Valkey.pubsub()` nutzt den Connection-Pool.
- aio-pika Publisher Confirms sind per-Message. Batch-Speedup via pipelined `asyncio.gather`.

## Outstanding Questions

### Resolved During Brainstorm Review

- [Affects R1][Resolved] Channel-Naming: **Pro-Task-ID** (`sq:notify:{task_id}`). Pro-Queue erzeugt O(clients × throughput) Noise da jeder Client alle Notifications empfängt und filtern muss. Pro-Task-ID: Client subscribed nur auf seinen eigenen Task, zero Noise, Channels sind billig (kein Storage).
- [Affects R2][Resolved] Pub/Sub-Subscribe-Connection gehört zum **Client** (der wartet auf Results), nicht zum Backend. Worker publiziert über die normale Backend-Connection.
- [Affects R2b][Resolved] **Initial-Check (50ms)** nach Subscribe + Enqueue: löst das Race-Window (Task fertig bevor Subscribe aktiv) in 50ms statt 1s.

### Deferred to Planning

- [Affects R9][Technical] `schedule_many()` via asyncio.gather auf existierendem publish()-Pfad oder eigene Broker-Methode?
- [Affects R1][Technical] Wo genau im Worker-Flow die Notification publishen? Nach store_result(), vor ack?

## Next Steps

-> `/ce:plan` für strukturierte Implementierungsplanung
