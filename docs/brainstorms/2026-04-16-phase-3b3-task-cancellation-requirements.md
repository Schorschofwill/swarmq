---
title: "Phase 3b.3: Task Cancellation (R3.6)"
type: feature-brief
status: ready-for-planning
date: 2026-04-16
audience: solo-developer (next feature after Locking, before Scheduler)
roadmap_refs: [R3.6]
---

# Phase 3b.3: Task Cancellation (R3.6)

## Problem Frame

SwarmQ kann heute keinen Task abbrechen. Wenn ein langlaufender Task hängt,
ein User-Request zurückgezogen wird, oder ein Workflow-Teil gescheitert ist,
bleibt der Task bis zum `hard_timeout` laufen — was Minuten oder Stunden
dauern kann, je nach Konfiguration. Queued Tasks, die noch nicht gestartet
sind, sind ebenfalls nicht abrufbar; sie müssen durchlaufen oder via
Queue-Purge grob gelöscht werden.

Cancellation ist **table-stakes**: Celery (`result.revoke()`), arq
(`job.abort()`), SAQ und RQ haben es. Ohne ist SwarmQ für interaktive
Workloads (User-Cancel-Button, Workflow-Abort, Deployment-Drain) ungeeignet.

## Goal

R3.6 (Task Cancellation) als Phase 3b.3. Baut auf R3.4 (Locking) — Pipeline-
Stage-Pattern und Valkey-Backend sind etabliert.

## Scope

**In-Scope:**

- **Client-API**
  - `await client.cancel(task_id) -> bool` — returnt `True` wenn neu
    gecancelt, `False` wenn Flag bereits existierte. Fire-and-forget:
    setzt Flag (`SET PX`), wartet **nicht** auf Worker-Reaktion.
  - Auf `SwarmQ`-Klasse: `app.cancel()` als Delegation zum Client
    (analog `schedule`).
  - **Bulk-Cancel (`cancel_many`) ist NICHT in v1** — caller kann
    `asyncio.gather(*[client.cancel(id) for id in ids])` nutzen.
    Nachziehen, wenn Workflow-Cancel (Phase 4) es braucht.
- **Worker: Pre-Start-Check**
  - Direkt nach Deserialize: `EXISTS sq:cancel:{task_id}`. Falls gesetzt:
    Cancel-Result speichern, Flag löschen, Message ack'en (in dieser
    Reihenfolge, alle drei Operationen in einem gemeinsamen `try/except`
    — symmetrisch zum Result-Path). Kein Semaphore-Acquire, keine
    Lock-Acquisition — Kurzschluss auf frühestmöglicher Stufe.
  - Pipeline-Position: **vor** Semaphore-Acquire (spart Slot für gecancelte
    Messages). **Implementation Note**: Dies erfordert ein neues
    Try-Block-Layout in `Worker._handle_message` — aktuell wird die
    Semaphore direkt nach Deserialize acquired (`worker.py:701`), und
    der Pre-Start-Check muss **vor** diesem Acquire eingefügt werden,
    **außerhalb** des bestehenden try/finally das die Semaphore released.
    Ebenso muss der Check **vor** dem existierenden
    `_try_apply_rate_limit`-Aufruf laufen (Phase 3b.1-Code wird also
    umgestellt, nicht nur ergänzt).
- **Worker: Poll-Loop während Ausführung**
  - Für jeden Task, der tatsächlich ausgeführt wird (nach allen Stages,
    unmittelbar vor `task_instance.run()`), startet der Worker eine
    parallele Monitor-Coroutine neben `run_task`.
  - Die Monitor-Coroutine ruft in einer Schleife
    `backend.is_cancelled(task_id)` (ein `EXISTS`-Command) auf; Intervall
    konfigurierbar über `config.cancel_poll_interval`, Default
    **1.0 Sekunden**.
  - Bei `True`: `run_task.cancel()` → `asyncio.CancelledError` bubbles
    durch `run()` und `post_execution()`. Monitor beendet sich.
  - Monitor-Coroutine in `finally` des Execute-Blocks cancelt und
    awaited — kein Leak bei Success-Path oder bei Run-Task-Exception.
  - Begründung für Poll statt Pub/Sub: einfachere Architektur, keine
    per-Task Valkey-Connection, kein Race-Window zwischen Pre-Start-Check
    und Subscribe, keine Connection-Loss-Edge-Cases. Latenz 1-2s statt
    <500ms — akzeptabel für User-Cancel-Button- und Workflow-Abort-
    Workloads. Entspricht dem Pattern von arq (siehe
    `docs/recherche-workflows-cancel-progress.md` §8.2).
- **Status-Modell**
  - `result_data` erweitert um `is_cancelled: bool` (neben `is_error`).
  - Neue Exception: `TaskCancelledError(TaskError)` in
    `src/swarmq/exceptions.py`.
  - `Client._unpack` prüft `is_cancelled` vor `is_error` und wirft
    `TaskCancelledError`.
- **Interaktion mit bestehenden Pfaden**
  - **Retry/DLQ**: `asyncio.CancelledError` in `run_task` wird im
    Exception-Handler als Cancel-Pfad detektiert (unterschieden von
    Hard-Timeout-Cancel durch separates Flag oder Kontext-Variable).
    Cancel-Pfad: **kein Retry**, **kein DLQ**, sofort
    `is_cancelled=True` Result schreiben + Ack.
  - **Lock**: Keine Sonderbehandlung. `CancelledError` bubbles durch
    `run()` → `finally` in `_handle_message` → bestehender
    `_release_acquired_locks()`-Aufruf mit dem eigenen `owner_token`.
    Der `__force__`-Pfad in `locking.py` bleibt reserviert für Admin/CLI
    (Phase 3c). **Code-Change**: Der aktuelle Release-Aufruf in
    `worker.py:983-984` ist **nicht** in `asyncio.shield` gewickelt
    (nur der Acquire-Pfad bei Zeile 381). Im Zuge von R3.6 muss der
    Release-Aufruf mit `asyncio.shield` umhüllt werden, damit ein
    `CancelledError` aus `run_task` nicht das `finally` mid-release
    abbricht. Der outer-cancel-Fall (graceful-shutdown cancelt das
    komplette `_handle_message`) wird vom bestehenden
    Shutdown-Cleanup-Pfad in `_execute_shutdown` abgedeckt.
  - **FIRE_AND_FORGET**: Pre-Start-Check greift genauso. Running-Cancel
    via Poll greift genauso — FAF speichert kein Result (Default),
    also auch kein `is_cancelled`-Result, nur Log + Signal.
  - **Rate-Limit**: Pre-Start-Check läuft **vor** Rate-Limit-Stage —
    gecancelte Tasks blockieren keinen Rate-Limit-Slot.
- **Flag-Lifecycle**
  - Key: `sq:cancel:{task_id}` (String-Key, nicht Set — konsistent mit
    Recherche §6).
  - Gesetzt via einfachem `SET sq:cancel:{id} "1" PX ttl_ms` — **kein
    Lua-Script nötig**, da nur eine Operation atomar abläuft. Kein
    PUBLISH (Poll statt Pub/Sub). TTL = `config.result_ttl * 1000` (ms).
    Default: 24 h.
  - Gelöscht vom Worker **nach** erfolgreichem Result-Write und **vor**
    Message-Ack — symmetrisch zum Result-Path.
  - TTL ist Safety-Net: wenn Message nie konsumiert wird (Queue purge,
    expired in DQ), verfällt Flag automatisch.
- **Signal**
  - `Signal.TASK_REVOKED` → rename zu `Signal.TASK_CANCELLED`. Aktuell
    in `signals.py:52` definiert, wird **nirgends in `src/` dispatched**
    (0 Treffer) — sicherer Rename ohne Breaking-Change.
  - Value-String: `"task_cancelled"`.
  - Payload: `task_id`, `task_name`, `phase` (`"queued"` | `"running"`).
- **Lua-Script**: Nicht erforderlich. Poll-Architektur kommt mit
  Standard-Valkey-Commands (`SET PX`, `EXISTS`, `DEL`) aus. Der
  Boolean-Return der Client-API wird über `SET NX PX` realisiert
  (returnt `None` wenn Flag bereits existierte, `"OK"` wenn neu
  gesetzt).
- **Backend-Methoden** (neue Sektion in `ValkeyBackend`)
  - `set_cancel_flag(task_id, ttl_ms) -> bool` — `SET sq:cancel:{id} "1" NX PX ttl_ms`. Returnt `True` wenn neu, `False` wenn bereits gesetzt.
  - `is_cancelled(task_id) -> bool` — `EXISTS`-Wrapper (für Pre-Start-Check und Monitor-Poll).
  - `clear_cancel_flag(task_id) -> None` — `DEL`-Wrapper.
- **Tests** (Unit + Integration + E2E — Zahl ergibt sich aus Behavior-Matrix, kein numerisches Ziel)
  - Queued Task cancel: Message wird acked, kein `run()`, Client sieht
    `TaskCancelledError`.
  - Running Task cancel: `asyncio.sleep(10)`-Task, Cancel nach 100ms,
    Client sieht `TaskCancelledError` innerhalb ~2s (Poll-Intervall-Budget).
  - Cancel vor Schedule (Flag wird gesetzt, Task danach geschedult):
    Task läuft einmal vollständig (Flag hat keine Wirkung für bereits
    gestartete Tasks, die die Flag-Prüfung passiert haben — Race-Window
    dokumentieren).
  - Cancel nach Completion: Flag wird gesetzt, Task ist schon fertig,
    Flag-TTL läuft ab — kein Effekt, keine Exception.
  - Cancel mit Lock: Task hält Lock, Cancel greift, `CancelledError`
    bubbles, `finally` released Lock — kein Lock-Leak.
  - Cancel bei FIRE_AND_FORGET: Message acked bei Pre-Start-Check oder
    Running-Cancel, kein Result gespeichert, kein Retry.
  - Cancel im Retry-Delay (Message in `{queue}.DQ`): nach Delay kommt
    Message zurück in Main-Queue, Pre-Start-Check greift, Cancel-Result
    wird geschrieben.
  - TTL: Flag verfällt nach `result_ttl`, wenn Message nie konsumiert
    wird.
  - Explizites Delete: Nach erfolgreicher Cancel-Verarbeitung ist
    `sq:cancel:{id}` weg (kein Residuum in Valkey).

**Out-of-Scope:**

- **`self.is_cancelled`-Property** im Task für cooperative Cancellation in
  blocking Loops ohne `await`-Punkte. YAGNI — `asyncio.Task.cancel()` reicht
  für idiomatischen async-Code. Kann später nachgezogen werden, wenn ein
  konkreter Bedarf entsteht.
- **Wait-for-effect-Semantik** (`cancel_and_wait(task_id)`). Caller kann
  sich über `get_result()` oder `@on_signal(TASK_CANCELLED)` informieren,
  falls nötig — als Helper-Funktion später.
- **Cancel-Bestätigung durch Worker** (z.B. `sq:cancelled:{id}` acknowledgement
  channel, auf den cancel() warten könnte). Brauchen wir erst, wenn
  Wait-for-effect gewünscht ist.
- **`cancel_many` / Bulk-Cancel**: Nicht in v1. Caller kann
  `asyncio.gather(*[client.cancel(id) for id in ids])` nutzen.
  Dedizierte Bulk-API kommt mit Workflow-Cancel (Phase 4, R4.1).
- **Workflow-Cancel** (Chain/Group/Chord in einem Aufruf abbrechen) —
  Phase 4 (R4.1).
- **CLI-Subcommand** `swarmq cancel <task_id>` — Phase 3c zusammen mit
  `swarmq dlq`/`swarmq schedule`.
- **Progress-Cancel-Integration** (R3.7 Progress Tracking). Wenn beide
  Features da sind, kann `update_progress()` implizit `is_cancelled`
  prüfen — hier nicht vorziehen.
- **Pub/Sub-Push für Running-Cancel**: Bewusst gegen Poll abgewogen
  (siehe Scope-Begründung bei „Worker: Poll-Loop"). Akzeptierte
  Limitation: Running-Cancel-Latenz ~1-2 s statt <500 ms. Upgrade auf
  Pub/Sub möglich, wenn konkreter Bedarf entsteht.
- **Cross-Worker-Broadcast** (wie Celery's `app.control.revoke`):
  Flag-basierter Mechanismus erreicht jeden Worker via Poll. Kein
  separater Broadcast-Mechanismus.
- **Lua-Script**: Einfache `SET NX PX` / `EXISTS` / `DEL` reichen;
  keine Script-Registrierung nötig (Poll statt Pub/Sub → keine
  Atomaritätsanforderung über mehrere Operationen).

## Behavior Constraints

1. **Pipeline-Stage-Discipline** (siehe
   `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`):
   - Pre-Start-Check läuft **vor** FIRE_AND_FORGET-Ack, **vor** Semaphore,
     **vor** Rate-Limit, **vor** Lock.
   - Result-Write + Flag-Delete + Ack in einem try/except, damit kein
     Half-State entsteht.
2. **Kein Retry bei Cancel**: `CancelledError` mit Cancel-Herkunft
   (unterschieden von Hard-Timeout) bypassed `_evaluate_retry` komplett.
3. **Idempotenz**: `cancel()` mehrfach auf denselben `task_id`
   aufgerufen ist harmlos — `SET NX PX` returnt beim zweiten Aufruf
   kein `OK`, der Boolean-Return wird zu `False` („bereits gesetzt").
4. **Poll-Intervall-Race**: Zwischen zwei Monitor-Polls können bis zu
   `cancel_poll_interval` Sekunden vergehen, bevor der Worker den
   Cancel sieht. Das ist die akzeptierte Latenzobergrenze. Für Tasks,
   die unterhalb des Poll-Intervalls komplettieren, gibt es eine
   Winz-Race — dokumentierte Limitation (Task läuft fertig, Client
   bekommt normales Result statt `TaskCancelledError`).
5. **Wire-Format Backward-Compat**: `is_cancelled` ist ein neues Feld
   im msgpack-Result-Dict. `Client._unpack` MUSS
   `data.get("is_cancelled", False)` verwenden, damit ältere
   Result-Blobs ohne dieses Feld nicht `KeyError` werfen. Kein
   Schema-Migrations-Pfad nötig — der Default False ist korrekt für
   alle Pre-R3.6-Results.
6. **Hard-Timeout vs. Cancel Discriminator**: Im Worker wird ein
   lokales `cancel_reason: str | None` pro `_handle_message`-Aufruf
   gehalten. Die Cancel-Monitor-Coroutine schreibt
   `cancel_reason = "cancel"` **bevor** sie `run_task.cancel()`
   aufruft; der Hard-Timeout-Watchdog schreibt
   `cancel_reason = "timeout"` **bevor** er `run_task.cancel()`
   aufruft. Beide Writes laufen auf demselben Event-Loop und sind
   damit serialisiert. Der bestehende `except asyncio.CancelledError`
   in `worker.py:827-837` liest `cancel_reason` und verzweigt:
   `"cancel"` → Cancel-Pfad (kein Retry, `is_cancelled=True`),
   `"timeout"` oder `None` → bestehender Hard-Timeout-Pfad.
7. **Monitor-Coroutine-Lifecycle**: Monitor wird unmittelbar vor
   `run_task`-Erstellung gestartet, im `finally` des Execute-Blocks
   gecancelt und awaited. Exceptions im Monitor (z.B. transiente
   Valkey-Connection-Drops) werden geloggt und geschluckt — die
   Schleife läuft weiter, der nächste Poll greift wieder.

## Success Criteria

User-visible:
- `await client.cancel(task_id)` vor Task-Start → Task läuft nie, Client
  sieht `TaskCancelledError`.
- `await client.cancel(task_id)` während Task läuft **und der Task
  regelmäßig `await`-Punkte hat** → Task wird innerhalb
  ~`cancel_poll_interval + kleine Puffer` (Default ~1-2 s) abgebrochen,
  Client sieht `TaskCancelledError`. Für CPU-bound Tasks ohne
  `await`-Punkte: Cancel wird am nächsten `await` wirksam, Fallback ist
  `hard_timeout`.
- `TaskCancelledError` ist von `TaskError` per `isinstance`-Check trennbar.
- Gecancelte Tasks landen **nicht** in DLQ, werden **nicht** retried.
- Locks werden bei Cancel sauber freigegeben (kein Lock-Leak).

Engineering-internal (nice-to-have):
- 0 neue P1/P2 Findings in `compound-engineering:review`.
- Alle bestehenden Tests grün. Neue Tests ergeben sich aus der
  Behavior-Matrix (Queued/Running/Lock/FAF/DQ/TTL-Edge-Cases).

## Risks

| Risiko | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `asyncio.CancelledError` wird als Hard-Timeout fehlinterpretiert (oder umgekehrt) | mittel | hoch | `cancel_reason`-Local-Variable im Worker (Constraint 6); Test mit Timeout + Cancel kombiniert |
| Flag-TTL zu kurz → Message im DQ (Retry-Delay) verpasst den Cancel | gering | mittel | TTL = `result_ttl` (24h default) — deutlich länger als typischer Retry-Delay (max. 1h); dokumentieren |
| Lock-Release bricht ab, weil `CancelledError` das `finally` abwürgt | gering | hoch | `asyncio.shield` um `_release_acquired_locks` (wie beim Acquire) — Test mit Cancel-während-Lock verifiziert |
| Signal-Rename `TASK_REVOKED` → `TASK_CANCELLED` bricht User-Code | sehr gering | niedrig | Rename ist sicher: 0 Treffer in `src/`, Signal nie dispatched. Im CHANGELOG markieren. |
| Monitor-Coroutine-Exception bei transientem Valkey-Drop → Cancel verzögert | gering | niedrig | Exceptions werden geloggt und geschluckt (Constraint 7), Loop läuft weiter; nächster Poll fängt Cancel. Hard-Timeout als Safety-Net. |
| Cancel für CPU-bound Tasks ohne `await`-Punkte unerreichbar | hoch | niedrig | `asyncio.Task.cancel()` delivered erst am nächsten `await`. Success-Criterion-Klausel „für cooperative-async-Tasks". CPU-bound → `hard_timeout` als Fallback. Ohne `self.is_cancelled`-Property (Out-of-Scope) kein Weg, CPU-Loops zu stoppen. |
| Poll-Intervall-Winz-Race: Task beendet sich zwischen zwei Polls, Client kriegt normales Result statt `TaskCancelledError` | mittel | niedrig | Akzeptierte Limitation (Constraint 4). Für Tasks, die schneller als Poll-Intervall laufen, hat Cancel keinen Effekt — das ist erwartet und harmlos. |

## Closed Decisions (vom Brainstorm)

Diese Punkte sind **entschieden** (nicht Open Questions für Planning):

1. **Terminologie**: `cancel` durchgängig. `Signal.TASK_REVOKED` → `Signal.TASK_CANCELLED` (Rename, da ungenutzt).
2. **Check-Mechanismus**: **Poll** (nicht Pub/Sub). Pre-Start-Check via
   `EXISTS` nach Deserialize; Running-Check via Monitor-Coroutine mit
   periodischem `EXISTS` (Default-Intervall 1.0 s, konfigurierbar via
   `config.cancel_poll_interval`). Begründung: einfachere Architektur,
   keine Pub/Sub-Lifecycle-Probleme, keine zusätzliche Connection
   pro Task, Latenz 1-2 s akzeptabel für Target-Use-Cases.
3. **Status-Modell**: `is_cancelled: bool` im `result_data` + neue `TaskCancelledError(TaskError)`-Exception.
4. **Lock-Interaktion**: Keine Sonderbehandlung im Haupt-Code-Pfad, aber `asyncio.shield` um `_release_acquired_locks` (neuer Code-Change).
5. **Retry/DLQ-Interaktion**: Cancel bypasst Retry und DLQ vollständig.
6. **Flag-Lifecycle**: TTL = `result_ttl` (Default 24 h), gesetzt via `SET NX PX`. Worker DELetet nach Result-Write und vor Ack.
7. **Client-API v1**: `cancel(task_id) -> bool` (True=neu, False=already). **Bulk-API (`cancel_many`) NICHT in v1** — Caller kann `asyncio.gather` nutzen.
8. **Cooperative `is_cancelled`-Property im Task**: NICHT in v1 (YAGNI).
9. **Pipeline-Position pre-start**: vor Semaphore, Rate-Limit, Lock.
10. **Hard-Timeout vs. Cancel Discriminator**: lokales `cancel_reason`-String pro Handler-Invocation (Constraint 6). Nicht Exception-Subclass, nicht Worker-weites Flag.
11. **Sequencing**: R3.6 vor R3.8/R3.9 (Scheduler) ist „maintainer-convenience ordering" — Lua/Valkey-Pattern sind nach R3.4 warm, Cancel nutzt nur Standard-Commands. Scheduler folgt unmittelbar nach R3.6.
12. **Lua-Script**: **KEIN** eigenes Script nötig — Poll-Architektur kommt mit `SET NX PX` / `EXISTS` / `DEL` aus.

## Open Questions (für Planning)

Nur echte Planning-Fragen, keine Wiederholung von Closed Decisions:

1. **Signal-Payload-Felder**: `phase`-Feld behalten oder droppen? Drei
   Optionen: (a) dritter Wert `"pre_execution"` ergänzen, (b) `"queued"`
   als Sammelbegriff für alles vor `run()` dokumentieren, (c) `phase`
   komplett streichen bis konkreter Subscriber existiert. Empfehlung:
   (c) für YAGNI — kein aktueller Consumer.
2. **FIRE_AND_FORGET + Cancel**: Kein Result gespeichert — soll trotzdem
   `TASK_CANCELLED`-Signal emittiert werden? (Empfehlung: ja, Signal ist
   der einzige Beobachtungspunkt für FAF.)
3. **`cancel()` für nicht existierende `task_id`**: Flag wird gesetzt,
   nie konsumiert, verfällt per TTL. Dokumentieren oder
   `TaskNotFoundError` werfen (wenn Registry-Lookup möglich)?
   Empfehlung: harmlos zulassen, nicht validieren (Registry hat eh
   keinen Lookup für `task_id`, nur für `task_name`).
4. **Metriken/Observability**: Separater Log-Event `task_cancelled` mit
   `phase` ("queued" | "running")? Welche strukturierten Felder?
5. **Worker-Shutdown-Interaktion**: Bei Graceful-Shutdown laufen
   Monitor-Coroutinen. Ordered Teardown: Consumer stoppen → Monitor-
   Coroutinen canceln → Tasks drain/cancel → Lock-Release → Backend-Close.
   Detail-Spec im Plan.
6. **Flag-DEL-Failure-Verhalten**: Wenn `DEL sq:cancel:{id}` nach Result-
   Write fehlschlägt — Empfehlung: log + ack (orphan self-heals via
   TTL), neues Log-Event `cancel_flag_delete_failed`.
7. **API-Stabilität-Marker**: Cancel-API (`TaskCancelledError`,
   `is_cancelled`-Feld, Signal-Rename) als „experimental, may change
   before v1" im Release-Doc markieren?

## Dependencies

- **R3.4 (Task Locking)** — DONE. Pattern-Source für Pipeline-Stage-
  Reihenfolge + `asyncio.shield`-Pattern für acquire (muss jetzt auch
  für release eingesetzt werden).
- **R3.3 (Rate Limiting)** — DONE. Pattern-Reuse für Pre-Execute-Stages.
- **R2.5 (Graceful Shutdown)** — DONE. Worker-Shutdown muss Monitor-
  Coroutinen vor Backend-Close cancellen.
- **Worker-Pipeline-Stage-Solution-Doc** — vorhanden.
- **`Signal.TASK_REVOKED` Rename** — triviales Rename, kein User-Code
  im Repo betroffen.

## Non-Goals

- Distributed fairness oder Priorisierung von Cancels
- Cancel-Reason-String im Result (`error_message` reicht)
- Wait-for-effect-Semantik in `cancel()` selbst
- Bulk-Cancel (`cancel_many`) — v1 deferred
- Pub/Sub-Push-Architektur — bewusst gegen Poll abgewogen (siehe
  Out-of-Scope)
- Cooperative `self.is_cancelled`-Property im Task
- CLI-Subcommand `swarmq cancel`
- Workflow-Cancel (Chain/Group/Chord)
- Progress-Tracking-Integration (`update_progress` + Cancel-Check)
- Cross-Cluster-Cancel (nur ein Valkey)
- R3.5 (Unique Tasks)
- R3.7 (Progress Tracking)
