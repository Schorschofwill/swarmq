# Learnings-Recherche: Fix-Plan Fehlersuche Runde 2 (swarmq v0.12.0)

## Suchkontext

- **Aufgabe**: Fix-Plan für 23 FEHLER (FS-02 … FS-31) aus `docs/befunde/fehlersuche-runde-2-2026-10.md`, ohne Folgefehler und ohne eine Regel E-001 … E-049 zu verletzen. Jeder Fix muss im Rolling Deploy neben 0.12.0-Workern funktionieren.
- **Suchbegriffe**: Publish-Deadline, PublishIndeterminate, aiormq, Channel, schedfire, Claim, Lua, idempotent, Retry-Politik, Unique, start-Modus, Dishka, finally, Semaphor, Lock, LockKeeper, SoftTimeout, Workflow-Cancel, Prozessgruppe, Recycling, Drain, DLQ-Header, x-delivery-count, Consumer, basic.cancel, Expiry, Ack, Header-Parsing, Leader-Renew, Mischbetrieb.
- **Durchsucht**: 15 Learnings unter `swarmq-fehlersuche/docs/solutions/` (vollständig gesichtet, weil es so wenige sind) und alle 49 Regeln im Pack `swarmq-entscheidungen` (vollständig gelesen, weil sie für den Plan verbindlich sind).
- **Relevante Treffer**: 9 Learnings und 30 Pack-Regeln.
- **Übersprungene Pack-Dateien**: keine. Alle 49 Regeln haben Frontmatter und `applies_when`. Die `README.md` ist nur die Beschreibung des Packs.
- **Critical Patterns**: `docs/solutions/patterns/critical-patterns.md` gibt es nicht. Die Rolle übernimmt am ehesten `patterns/invarianten-der-worker-pipeline-2026-08.md` (unten unter Querschnitt).
- Quellpfade der Learnings beziehen sich auf `/home/georg/Dokumente/Development-Privat/swarmq-fehlersuche/docs/solutions/`. Pack-Pfade beziehen sich auf `compound-packs/swarmq-entscheidungen/`.

---

## Querschnitt: Regeln, die für jeden Fix gelten

### Invarianten der Worker-Pipeline
**Datei**: `patterns/invarianten-der-worker-pipeline-2026-08.md` (2026-08-27, Stand 0.2.0, `invariant_enforcement`)

1. **Ack-Ordering.** Die Gates geben ein `DeliveryOutcome` zurück. Nur der Runner und der Ergebnis-Tail rufen eines der vier Verben auf (`ack`, `redeliver`, `retry_later`, `dead_letter`), genau eines pro Zustellung. Ein Fix darf in keinem Gate ein Broker-Verb selbst aufrufen.
2. **Terminal-Flags als Summentyp.** `worker/outcome.py` kennt `Continue`, `Redelivered` und `Terminal(release_unique, finalize_schedfire)` mit den Konstruktoren `release_in_finally()`, `already_released()` und `never_release()`. Die Fixes für FS-03, FS-13 und FS-17 sind Änderungen an der Wahl des Outcomes, nicht neue Flags.
3. **Owner-Checks.** Locks und Unique-Flags gibt nur ihr Besitzer frei, per Token bzw. Owner-String. Das gilt auch für die Freigabe im Hintergrund (FS-07) und für idempotente Claims (FS-06).
4. **publish-then-mark** bei Workflows: Ein Schritt wird erst nach dem `PublishReceipt` markiert.

### Fallen im `finally` von Dedup-, Lock- und Marker-Lebenszyklen
**Datei**: `logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md` (2026-06-02, critical)

- Wer im `finally` einen Abschluss-Zustand schreibt, muss terminale von nicht-terminalen Austritten unterscheiden.
- Die sichere Fehlerrichtung ist, eher zu viel freizugeben. Ein seltenes Doppel-Fire ist at-least-once und tolerierbar. Ein fälschliches „terminal“ reproduziert dagegen den stillen Drop.
- Fehlversuch: nur den Completed-Marker zu unterdrücken genügte nicht. Der Inflight-Marker blockierte die Neuzustellung trotzdem.

### Fallen bei Ack-Reihenfolge und Unique-Freigabe
**Dateien**: `logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md` (high) und `patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`

- **Rule 2:** Transiente Fehler werden neu zugestellt, permanente gehen über DLX bzw. DLQ.
- **Pattern 4:** Freigabe **nach** dem Ack, im Rückfall auf Requeue keine Freigabe.
- **Pattern 5:** Im start-Modus erst nach Rate-Limit und Lock freigeben.
- **Pattern 7:** msgspec-Structs nur am Ende erweitern, mit Default. **Keine neue `Literal`-Variante** in `source`, weil alte Worker beim Dekodieren daran scheitern. Das ist für den Mischbetrieb verbindlich.
- **Pattern 8:** Ein ungültiger Header zählt wie keiner.
- **Pattern 10:** Für „nicht übergeben“ das `_MISSING`-Sentinel verwenden.

### Mischbetrieb und Grenzen der Ausführungsidentität
**Pack-Regel**: E-009 (pack: swarmq-entscheidungen, e009-replay-umgeht-done-marker.md)

> „Eine normale Neuzustellung derselben Ausführung (gleiche Identität) wird weiter als Duplikat verworfen.“

Jeder Fix an Gates, Markern oder Headern muss die Identität `task_id|H#n` unverändert lassen.

---

## Je Befund-Thema

### 1. Publish-Endfrist und aiormq-Kanal-Sperre: FS-02 (P1), FS-08 (P2), FS-30 (P2)

**Pack-Regeln**

- **E-042** (pack: swarmq-entscheidungen, e042-publish-indeterminate-nur-nach-senden.md)
  > „Ist die Nachricht auf den Socket geschrieben und reißt die Verbindung vor dem Publisher-Confirm, endet der Publish als `PublishIndeterminate` und wird nicht erneut gesendet. Riss die Verbindung, bevor der Frame geschrieben war, wiederholt swarmq auf einem neuen Kanal und meldet, wenn das nicht hilft, `BrokerConnectionError`.“

  Relevanz: Das ist der Maßstab für FS-02a und FS-08; beide stehen dort als Verletzung. Die festschreibenden Tests in `tests/unit/broker/test_publish_connection_loss.py` (E-042 und E-041) müssen grün bleiben.
  Ausgeschlossen: Verbindungsfehler pauschal als unbestimmt **oder** pauschal als wiederholbar zu behandeln.

- **E-041** (pack: …, e041-publish-deadline-unbestimmter-ausgang.md)
  > „Die Publish-Deadline … deckelt den gesamten Publish-Vorgang samt Wiederholungen … Reißt sie, während der Broker die Nachricht schon haben kann, ist der Ausgang unbestimmt … Im Client bleibt die Unique-Sperre bei unbestimmtem Ausgang stehen.“

  Ausgeschlossen: die Frist je Versuch statt je Aufruf zu messen. Ebenfalls ausgeschlossen: die Unique-Sperre nach einem echten `PublishIndeterminate` freizugeben (Test `test_unique_claim_survives_an_indeterminate_publish`).
  Richtung für FS-02a: eine Frist, die vor Beginn verstrichen ist, muss vor dem Senden mit `BrokerConnectionError` scheitern. Das ist kein `PublishIndeterminate`; Nicht-Senden ist hier nachweisbar.

- **E-025** (pack: …, e025-blockierter-broker-publish-wartet.md)
  > „Ist die Verbindung blockiert, sendet ein Publish nichts, wartet bis zur Publish-Frist und scheitert dann mit `BrokerPublishBlocked` … Grenze: … der erste Publish während des Alarms … endet als `PublishIndeterminate`.“

  Relevanz: Der Fix für FS-08 (Warten in aiormqs Kanal-Sperre) darf das entschiedene Warten bei Blockade nicht verkürzen. Er darf auch die dokumentierte Grenze „erster Publish im Alarm“ nicht in `BrokerConnectionError` umdeuten. Tests in `test_publish_blocked.py`.

- **E-038** (pack: …, e038-recycling-verbraucht-keinen-retry.md)
  > „Die Drainfrist ist der größte registrierte `hard_timeout` plus Puffer, nicht `shutdown_timeout`. Ein laufender Task verliert durch das Recycling keinen Retry“

  Weiter heißt es zu FS-30: der Rückfall auf `shutdown_timeout` ist „nirgends entschieden“, eine „Umsetzungslücke gegen R22. Den Test bei einem Fix anpassen.“ Die Regel nennt dazu `tests/unit/worker/test_recycling.py:84` (gemeint ist die Zeile `assert worker._drain_timeout_override == float(worker._config.shutdown_timeout)` in `test_task_count_threshold_recycles_with_ack`). Der Test trägt **keinen** `entscheidung`-Marker und darf angepasst werden.
  Zu FS-02b: Die Publish-Endfrist liegt beim Recycling bei `drain_until`.
  Ausgeschlossen: `shutdown_timeout` als Recycling-Frist und ein erzwungener Abbruch beim Recycling.
  **Nicht im Register entschieden:** welche Frist gilt, wenn kein Task einen `hard_timeout` hat. R22/KTD11 sagt „ohne erzwungenen Abbruch“. Der Plan muss hier einen Wert festlegen und ihn als Abhängigkeit benennen.

- **E-018** (pack: …, e018-shutdown-timeout-gesamtbudget.md)
  > „der Drain läuft höchstens bis `finish_by − 5 s` … Der Recycling-Drain ist ausgenommen.“

  Ausgeschlossen: das Recycling an das Shutdown-Budget zu koppeln (`test_recycling_drain_is_exempt_from_the_budget`). Offener Nebeneinwand: `restart_budget.py:57` und `setpoint.py:210` rechnen noch mit der alten Bedeutung.

- **E-001** (pack: …, e001-drain-abbruch-fehlerpfad.md)
  > „Bricht der Drain einen laufenden Handler ab, wird das wie ein Timeout als Fehlerergebnis abgebildet … Der Abbruch wird nie als `CancelledError` propagiert.“

  Ausgeschlossen: ein Fix für FS-02 oder FS-30, der einen Drain-Abbruch zur Neuzustellung macht. Das ist laut Kontext ausdrücklich nicht Teil des Plans. Tests: `compute/test_executor.py` (E-001).

- **E-013** (pack: …, e013-leader-ruecktritt-bei-publish-fehlern.md), offener Einwand: Ein echter 403 kommt nach dem Senden und wird über `_raise_if_sent` vermutlich zu `PublishIndeterminate`. Wer die Phasen-Einordnung in `delivery.py` anfasst (FS-02, FS-08), berührt das. Die Einordnung von `ACCESS_REFUSED` darf sich dabei nicht unbemerkt ändern.

**Learnings**

- `spikes/2026-04-17-toxiproxy-reconnect-spike.md`, Spike 1: Ein zuvor geöffneter Publish-Kanal wird nach einem Reconnect nicht neu erstellt (`ChannelInvalidStateError`). Für FS-08 heißt das: Der Wartende in der Kanal-Sperre muss den Verbindungsabriss als „nicht gesendet“ erkennen und auf einem **neuen** Kanal wiederholen. Auf den alten Kanal zu warten, führt nicht zum Ziel.
- `logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md`, Teil B und C: `is_closed` ist kein Lebenszeichen. Ein `close()` von aio-pika darf nicht in `try/except: pass` gekapselt werden, weil das den Verbindungszustand verdirbt. Gilt bei FS-08 und FS-09 als Falle.

**Zusammenspiel, das ich melde und nicht unterdrücke (E-001 mit FS-02):** Nach dem Fix für FS-02a scheitert ein Retry- oder DLQ-Publish am Ende eines **Shutdown**-Drains vor dem Senden mit `BrokerConnectionError`. Die Nachricht bleibt dann unbestätigt und wird neu zugestellt. Die in E-001 genannte Folge „verbraucht einen Retry, bei `max_retries=0` DLQ“ tritt am Shutdown-Ende damit praktisch nicht ein. Es bleiben Fehlerergebnis (falls vorher gespeichert) und erneuter Lauf; die DLQ-Kopie entfällt.

- Die Entscheidung E-001 bleibt formal eingehalten: Es gibt kein `CancelledError` und der Fehlerpfad läuft.
- Ihre Prämisse („Retry und DLQ laufen normal durch“) trägt aber nicht. E-001 nennt dieses Argument selbst als offen.
- Für den Plan heißt das: als Abhängigkeit benennen und klären, ob ein gespeichertes Fehlerergebnis vor der Neuzustellung stehen bleibt.

---

### 2. Schedfire-Gate und Zwei-Marker-Absturz: FS-03 (P1)

**Pack-Regeln**

- **E-046** (pack: …, e046-schedfire-at-least-once.md)
  > „Stürzt der Worker, der den Claim gewonnen hat, vor dem Ack ab, muss die Neuzustellung das Fire nach Ablauf bzw. Übernahme des Claims erneut ausführen, statt es als Duplikat zu bestätigen.“

  Und zum Fix:
  > „`sched_fire_claim.lua:9-11` (d93d3ab, 0.9.0) [sagt], eine Broker-Redelivery derselben Kopie bleibe Duplikat; das schützt vor Parallelausführung bei Kanalverlust und muss ein Fix erhalten (Zurückstellen statt Bestätigen).“

  Ausgeschlossen:
  - Bestätigen (Ack) einer Neuzustellung mit fremdem Claim.
  - Sofortiges Ausführen bei fremdem, noch lebendem Claim. Das wäre Parallelausführung.

  Geforderte Richtung: zurückstellen (`retry_later` oder `redeliver`), bis der Claim abläuft oder übernommen werden kann. Außerdem gehört das Warten auf den Semaphor in den try/finally-Bereich.
  `test_schedfire.py::test_duplicate_when_inflight_claim_lost` beruht laut Regel auf einer falschen Prämisse und trägt keinen `entscheidung`-Marker. Er darf angepasst werden.
  Ein in Plan H-2 geforderter Absturztest fehlt (`ohne_test`).

- **E-009** (pack: …, e009-replay-umgeht-done-marker.md)
  > „Ein Cron-Replay läuft unter dem Schedfire-Key `K|<Identität>`; ein normaler Doppel-Fire behält K und wird verworfen.“

  Ausgeschlossen: den Completed-Marker zu lockern. Ein echtes Doppel-Fire (E-047) muss weiter verworfen werden. Tests in `test_schedfire_replay.py`.

- **E-010** (pack: …, e010-marker-ttl-max-age-restluecke.md): Die Marker-TTL von mindestens `max_age + 300` s darf durch Zurückstellen nicht verkürzt werden.
- **E-047** (pack: …, e047-split-brain-leader-hingenommen.md)
  > „Ein Fehler ist erst, wenn das Duplikat tatsächlich zweimal ausgeführt wird“

  Begrenzt die Lockerung in die andere Richtung.
- **E-008** (pack: …, e008-unbekannter-task-gnadenfrist.md): Das Outcome `Redelivered` verhindert, dass das `finally` den Completed-Marker schreibt. Dieses Muster eignet sich für den Pfad „zurückstellen bei fremdem Claim“.
- **E-001**, Begründung: Ein propagiertes `CancelledError` träfe den Terminal-Default im `finally`, der den Completed-Marker schreibt. Ein Shutdown-Abbruch beim Warten auf den Semaphor (Teil 2 von FS-03) muss deshalb ein **nicht-terminales** Outcome ergeben, nicht den Terminal-Default.

**Learnings**

- `logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md`: Es gibt zwei Marker. Bei nicht-terminalem Ende muss der Inflight-Marker gelöscht werden (`clear_schedfire_inflight`), statt den Completed-Marker zu schreiben. Der Nachweis lief per Integrationstest über die Logsequenz. Ein Absturz (SIGKILL) erreicht kein `finally`. Für diesen Fall muss die Neuzustellung selbst erkennen, dass der Claim tot ist (Ablauf oder Übernahme). Das Löschen im `finally` reicht dafür nicht.
- `logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md` (P0): Der Semaphor darf nie im Consume-Callback gehalten werden; der Dispatch muss sofort zurückkehren. Wenn das Warten auf den Semaphor in den try/finally-Bereich wandert, muss es in der per `create_task` gestarteten Aufgabe bleiben.
- **Mischbetrieb:** 0.12.0-Worker bestätigen das Duplikat weiterhin. Während des Rolling Deploys besteht der Verlust also fort, bis alle Worker neu sind. Das gehört als Grenze in den Plan.
  - Eine Änderung an `sched_fire_claim.lua` muss Marker-Werte und Rückgabecodes kompatibel halten. Jeder Worker lädt sein eigenes Skript, beide Skriptversionen arbeiten auf denselben Keys.

---

### 3. Idempotente Valkey-Lua-Claims bei Client-Retry: FS-06 (P2)

**Pack-Regeln**

- **E-022** (pack: …, e022-lock-lease-erneuerung-ohne-fencing.md)
  > „`lock_acquire.lua` setzt `PEXPIRE` nur noch verlängernd. … Es gibt kein Fencing-Token.“

  Ausgeschlossen: Fencing einzuführen. Ein idempotentes Acquire mit gleichem Owner-Token darf die Lease nur verlängern, nie verkürzen. Tests in `test_lock_keeper.py`.

- **E-011** (pack: …, e011-cron-unique-aus-definition.md)
  > „Bei einem Publish-Fehler gibt der Leader die Unique-Sperre nur frei, wenn er sie in genau diesem Fire beansprucht hat“

  Ein wiederholter Claim, der „eigener Owner“ zurückgibt, muss als „in diesem Fire beansprucht“ zählen. Sonst bleibt die Sperre fälschlich stehen.
- **E-033** (pack: …, e033-unique-for-none-opt-out.md): Das betrifft dieselbe Claim-Stelle in `Client.schedule`.

**Learnings**

- `spikes/2026-04-17-toxiproxy-reconnect-spike.md`, Spike 2, ist die **Wurzel von FS-06**. Der Spike hat die Retry-Politik empfohlen. Sie ist seitdem umgesetzt: `src/swarmq/backend/valkey/connection.py:221`, `Retry(ExponentialBackoff(cap=2.0, base=0.05), retries=5)` mit `retry_on_error`. Sie wiederholt auch nicht-idempotente Skripte.
  Ausgeschlossen: die Retry-Politik global abzuschalten. Sie trägt die Stabilität bei Netzflattern.
- **Vorlage im Code (kein Learning-Dokument): Commit d7d94c5** („Claim des Ausfuehrungsschutzes idempotent bei Transport-Retry“).
  - Bei einem Claim mit dem **eigenen** Owner-Token liefert das Skript `claimed`.
  - Lua und In-Memory-Store verhalten sich gleich, mit einem Contract-Test für beide (`tests/contracts/coordination.py`, `test_contracts_valkey.py`).
  - Dasselbe Muster gilt für `lock_acquire.lua`, den Unique-Claim (`SET NX GET` liefert die eigene `task_id`, das gilt dann als beansprucht) und `sched_fire_claim.lua`.
- `patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`, Pattern 1: `SET NX GET PX` liefert den bestehenden Wert. Dass „bestehender Wert == eigene `task_id`“ als eigener Claim gilt, ist sicher, weil die `task_id` je Aufruf neu erzeugt wird.

---

### 4. Freigabe von Claims nach Timeout: FS-07 (P2)

**Pack-Regeln**

- **E-041**
  > „Im Client bleibt die Unique-Sperre bei unbestimmtem Ausgang stehen.“
- **E-011**: keine Freigabe „nicht bei fremden `cli_unique`-Flags und nicht bei `PublishIndeterminate`“.

Ausgeschlossen: pauschal bei jedem Fehler nach dem Claim freizugeben. Freigegeben wird nur, wenn der **Claim selbst** unbestimmt endete (Timeout, verlorene Antwort) und noch nichts publiziert wurde. Die Freigabe ist immer an den Owner gebunden.

- **E-022**: Ein Lock, dessen Acquire-Antwort verloren ging und den der LockKeeper nicht kennt, läuft per TTL ab. Die Hintergrund-Freigabe muss mit dem Token arbeiten.

**Learnings**

- **Vorlage im Code: Commit 7e6abec** (`worker/gates/execution_guard.py`). Nach einem gescheiterten Claim läuft eine begrenzte, an den Owner gebundene Freigabe im Hintergrund; einen fremden Marker lässt sie stehen. Das Muster lässt sich auf das Lock-Gate, den Unique-Claim des Leaders und `Client.schedule` übertragen.
- `patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`, Pattern 2: Rollback bei Fehlern nach dem Claim. FS-07 ist die Lücke dort, wo der Rollback erst **nach** dem Claim-Aufruf beginnt, der Claim aber selbst per Timeout endet.
- `patterns/worker-cancel-safe-teardown-detached-tasks-2026-06-01.md`: Hintergrund-Aufgaben brauchen Tracking (`_detached_releases`) und das Muster detached-shield, sonst gibt es beim Shutdown „Future exception was never retrieved“.

---

### 5. Dishka-Finalizer im `finally`: FS-04 (P1)

**Pack-Regeln**

- **E-022**
  > „Der Lock-Keeper verlängert Mutex- und Semaphor-Leases …, solange der Handler lebt“

  Das macht ein übersprungenes Release zu einem Dauerlock. FS-04 ist P1 mit Lock genau deshalb. Ausgeschlossen: den Fix über eine Begrenzung der Erneuerung zu lösen.
- **E-018**: Jeder Aufräumschritt hat ein eigenes Zeitlimit. Ein hängender Finalizer darf Lock- und Semaphor-Freigabe auch zeitlich nicht blockieren.
- Offene Produktfrage „Ack vor DI-Scope-Ende“ (Kontext): nicht Teil des Plans. Der Fix darf die Reihenfolge von Ack und Scope-Ende **nicht** ändern, sondern nur dafür sorgen, dass ein `ExitError` die Freigaben nicht überspringt.

**Learnings**

- `architecture-patterns/dishka-di-integration-async-task-queue.md` (2026-05-31)
  - Guidance #5: `__aexit__` bekommt die Ausnahme des Tasks, damit der Finalizer zurückrollt.
  - Guidance #7: Scope-Ende als verfolgte Aufgabe mit `shield`.
  - Guidance #8: Ein Fehler beim Schließen wird geloggt, nicht geworfen. Das ist für den APP-Container dokumentiert, für den REQUEST-Scope fehlt die Regel. Genau dort liegt FS-04.
  - Richtung: Der `ExitError` aus dem REQUEST-`__aexit__` wird gefangen und geloggt bzw. als Fehler gewertet. Die übrigen Schritte im `finally` (Lock, Semaphor) laufen trotzdem, entweder verschachtelt in try/finally oder als je eigener Schritt.
  - FS-26 (Finalizer-Fehler im Kind) ist eine LÜCKE und nicht Teil des Plans.
- `patterns/invarianten-der-worker-pipeline-2026-08.md`, Nr. 1 und 2: Das Outcome bestimmt das Verb. Ein Finalizer-Fehler darf kein zweites Verb auslösen.
- `logic-errors/lock-ttl-shorter-than-hard-timeout-duplicate-execution-2026-06-02.md`: Der `finally`-Block gibt den Lock kooperativ frei, die TTL ist nur Absturzschutz. Seit BZ-25 erneuert der Keeper; ein übersprungenes `finally` hebelt beide Schutzmechanismen aus.

---

### 6. TimeoutError gegen SoftTimeoutError: FS-05 (P2), FS-20 (P3, Doku)

**Pack-Regel E-045** (pack: …, e045-soft-timeout-ueber-wait-for.md)
> „Ist `soft_timeout` gesetzt, läuft `run()` unter `asyncio.wait_for`. … Die Umwandlung gilt nur für den Soft-Timeout selbst.“

- **FS-05:** Ausgeschlossen ist eine Umwandlung ohne gesetzten oder abgelaufenen `soft_timeout`. Eine eigene `TimeoutError` des Tasks muss als solche bei `retry_exceptions` ankommen. Das gilt auch im CPU-Kind (`compute/child.py`).
  - Festschreibende Tests: `test_worker_timeouts.py`, Zeilen 100, 131, 248 (E-045). `post_execution` entfällt nur beim echten Soft-Timeout.
- **FS-20:** Ausgeschlossen ist, `SoftTimeoutError` in `run()` fangbar zu **machen**. Das ist die verworfene Anforderung R2.4. Der Fix ist rein dokumentarisch: `scheduling-and-retries.md:674-675`, `errors.md:422-441`.

**Pack-Regel E-037** (pack: …, e037-hard-timeout-und-kindtod-im-retry-pfad.md): Der Hard-Timeout bleibt ein eigener Pfad (Tests in `test_worker_timeouts.py`, Zeilen 175, 282, 368).

---

### 7. Workflow-Cancel vor dem Start: FS-12 (P2)

- **E-040** (pack: …, e040-workflow-fail-fast.md)
  > „Endet ein Workflow-Schritt endgültig ohne Erfolg (… Abbruch per Cancel), markiert der Worker den Workflow synchron im selben Pfad als `failed`“

  Weiter: „Scheitert das Fail-Mark selbst transient, wird die Nachricht neu zugestellt statt bestätigt“.
  Ausgeschlossen: ein asynchrones Fail-Mark bzw. eine Lösung über DLQ-Beobachtung.
  Der Nebenbefund `handle.cancel()`/`status()` (FS-32) ist eine LÜCKE und nicht Teil des Plans.
- **E-020** (pack: …, e020-cancel-flag-lebensdauer.md): Nach Wirkung wird das Cancel-Flag nicht gelöscht, sondern per `PEXPIRE … LT` auf höchstens 1 h verkürzt. Der Fix im Cancel-Gate muss das beibehalten.
- **E-039**: Das Fail-Mark nutzt denselben Pfad wie der Laufzeit-Guard.

---

### 8. Freigabe bei `unique_until="start"`: FS-13 (P2), FS-17 (P3)

**Pack-Regeln**

- **E-034** (pack: …, e034-unique-start-freigabe-vor-ausfuehrung.md)
  > „gibt der Worker das Unique-Flag genau einmal frei, und zwar erst, wenn der Task tatsächlich gleich läuft … Wird die Nachricht vorher zurückgestellt oder requeuet, bleibt das Flag gesetzt.“

  Zu FS-13: „Ein Fix muss die Einmal-Freigabe dieses Tests einhalten.“ Tests: `test_worker.py:2490`, `:2940`.
  Ausgeschlossen: die Freigabe beim Abholen (ADV-002) und eine Freigabe bei nicht-terminalen Rückstellungen.
  Richtung: Die terminalen Pfade vor dem Start (Expiry, Pre-Start-Cancel, Ende der Lock-Wartezeit, Delivery-Limit, unbekannter Task nach Gnadenfrist) liefern im start-Modus `release_in_finally()` statt `already_released()`.

- **E-008**
  > „Ein zurückgestellter Cron-Fire [hält] seinen Unique-Claim“

  Das zeigt `test_deferred_cron_fire_keeps_claim_open_and_unique_flag_held`. Nur die abgelaufene Gnadenfrist ist terminal.
- **E-007**: Das Ende der Lock-Wartezeit (`LockWaitExceeded`) ist terminal; ein Warten davor nicht.
- **E-043** (pack: …, e043-dlq-ack-nach-publish-terminal.md)
  > „Hat der Broker die DLQ-Kopie bestätigt und scheitert danach nur das Ack des Originals …, behandelt der Worker das wie einen erfolgreichen Dead-Letter: Warnung `dlq_ack_failed_after_publish`, Unique-Freigabe und `TASK_DLQ`.“

  Zu FS-17: „Das Expiry-Gate … hält sich nicht an diese Regel“. Richtung: das Expiry-Gate an `dead_letter_copy`/`SettleFailedAfterPublish` angleichen.
- **E-031**: Expiry speichert ein Fehlerergebnis. Ausnahme ist nur eine globale `result_ttl=0` (`test_expired_skips_result_store_when_ttl_zero`). Den Kommentar in `expiry.py:81-83` korrigieren, das Verhalten nicht ändern.

**Learnings**

- `patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`
  - Pattern 3: Die Grundeinstellung ist „release pending“; eine zu frühe Freigabe toleriert der Owner-Check.
  - Pattern 4: erst Ack, dann Freigabe; im Rückfall auf Requeue keine Freigabe.
  - Pattern 5: Freigabe im start-Modus nach dem Lock.
- `logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md`: Im Zweifel freigeben. Eine doppelte Freigabe fängt der Owner-Check ab; eine fehlende hält das Flag bis zur TTL.

---

### 9. Opt-out `unique_for=None` beim verzögerten Schedule: FS-14 (P2)

- **E-032** (pack: …, e032-leader-taskinfo-rueckfall.md)
  > „Der Fix muss den Opt-out in der Definition persistieren, statt den Rückfall zu entfernen.“

  Ausgeschlossen: den Rückfall auf die TaskInfo des Leaders zu entfernen (`test_old_definition_takes_both_unique_values_from_the_registry`).
- **E-033**
  > „Die Zusage gilt für sofortige und für verzögerte Aufrufe (`delay`, `eta`).“

  Tests in `test_schedule.py`.
- **E-011**: Ist `definition.unique_for` gesetzt, kommen beide Werte aus der Definition. Der Fingerabdruck enthält sie nur bei gesetztem `unique_for` und ist sonst byte-gleich zu 0.11.
- **Learning** `patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`
  - Pattern 7: neues Feld am Ende, mit Default. **Keine** neue `source`-Variante, weil alte Leader und Worker an der `Literal`-Validierung scheitern.
  - Pattern 10: Das Sentinel `_MISSING` unterscheidet „nicht übergeben“ von `None`.
  - Im Mischbetrieb ignoriert ein 0.12.0-Leader das neue Feld und fällt auf den heutigen Zustand zurück. Das gehört als Grenze in den Plan.

---

### 10. Prozessgruppen-Kill und Slots im CPU-Modus: FS-16 (P2), FS-23, FS-24 (P3)

- **E-036** (pack: …, e036-cpu-kill-trifft-prozessgruppe.md)
  > „Nach dem Kill lebt kein Nachkomme des Kindes mehr (R8, AE13). Stirbt das Kind ohne Ergebnis (OOM, Segfault), beendet der Loop-Prozess verbliebene Nachkommen des toten Kindes (R9).“

  FS-16: Richtung ist, SIGKILL immer an die Gruppe zu schicken, auch wenn das Kind schon tot ist. Ein überlebender Enkel nach SIGKILL des **Loop-Prozesses** bleibt erlaubt.
  Tests: `test_pool.py:478`, e2e `test_kill_takes_the_grandchild_with_it`.
- **E-035** (pack: …, e035-cpu-unreapbares-kind-blockiert-slot.md)
  > „Ein Nachweis soll nur ‚Index nicht mehr frei‘ prüfen, nicht die Aufhebung der Sperre.“

  FS-23: Ausgeschlossen ist eine Abkühlzeit bzw. Entsperrung von Rechen-Slots. E-024 gilt nur für Supervisor-Slots. Test: `test_pool.py:508`.
- **E-037**: Ein Kindtod ist ein Fehler im Retry-Pfad. FS-24 (doppelt gezählter Kindtod) darf am Retry-Pfad nichts ändern, nur an der Zählung bzw. am Log.

---

### 11. DLQ-Kopie-Header: FS-10 (P2)

- **E-009**: Die Identität `n` stammt aus `x-swarmq-xdeath-base` und `x-death`, nicht aus `x-delivery-count`. Das Entfernen von `x-delivery-count`/`x-acquired-count` aus der Kopie ändert die Identität nicht. Das ist trotzdem gegen `test_identity_sequences.py:261/285` und `test_publish_indeterminate_double_copy_replays_once` zu prüfen.
  - Grenzen im Mischbetrieb laut Regel: Ein UI-Replay an einen 0.11- oder 0.12.0-Worker kann weiter sofort in die DLQ gehen.
- **E-028**: `dlq list` kopiert ans Ende. Der Fix muss auch dort Header bereinigen oder ausdrücklich nicht. Spike S4: Eine Rückgabe zählt den Delivery-Count der XQ hoch.
- **E-016**: Das Gate nutzt `min(Konfiguration, effektives Limit)` und bleibt unverändert.

---

### 12. Consumer-Wächter-RPC: FS-09 (P3)

- **Learning** `logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md`
  - Ein Abbruch eines RPC in aiormq schließt den Kanal.
  - Teil C: den close-Fehler nicht schlucken.
  - Richtung: das RPC `basic.cancel`/`basic.consume` nicht per `wait_for` abbrechen. Stattdessen mit `shield` weiter warten oder den Kanal-Verlust als Neuaufbau behandeln.
  - Laut Scope-Notiz nutzt der Produktionscode `queue.consume` mit Tag-Verwaltung.
- **E-019**: Prefetch je Consumer bleibt; kein gemeinsamer Kanal und kein globales QoS als Ausweg.

---

### 13. Ack im Expiry-Gate: FS-17. Siehe Abschnitt 8 (E-043, E-031).

---

### 14. Header-Parsing: FS-19 (P3)

- **Learning** `patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`, Pattern 8: „Invalid headers are treated as ‚no flag‘ — safer than rejecting the message“.
- **Learning** `logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`, Rule 3: Das dortige Muster `int(original_headers.get("x-swarmq-rate-limit-count", 0)) + 1` ist **genau die fehlerhafte Form**, die FS-19 beschreibt; ein nicht numerischer Wert wirft. Das Learning ist an dieser Stelle selbst veraltet bzw. unsicher.
- Richtung: eine einzige tolerante Parse-Hilfe für alle Zähler-Header, so wie die drei Stellen, die es schon richtig machen.
  - Bei `x-swarmq-retry-count` gilt: Der String `"3"` wird von Python bzw. aio-pika heute schon abgelehnt. Wie Strings behandelt werden (als Zahl lesen oder wie fehlend werten), muss der Plan festlegen. Pattern 8 spricht für „wie keiner“, ist aber für Zähler nicht ausdrücklich entschieden.

---

### 15. Leader-Renew-Timeout: FS-31 (P3)

- **E-048** (pack: …, e048-leader-ruecktritt-vor-ttl-ablauf.md)
  > „nach `N = max(1, lock_ttl // heartbeat - 1)` aufeinanderfolgenden Fehlern tritt der Leader proaktiv zurück …, einen Heartbeat bevor die Lock-TTL abgelaufen sein kann.“

  Richtung: den Renew mit `backend_op_timeout` begrenzen, sodass ein hängender Renew als Fehler zählt.
  Tests: `test_loop.py::TestElectionLoop`.
- **E-047**: Ausgeschlossen ist Fencing bzw. Konsens. Die Doppel-Fires bleiben ein akzeptiertes Risiko.
- **E-013**: Rücktritt nach Publish-Fehlern und Abkühlzeit sind ein eigener Mechanismus und bleiben unberührt.
- **Learning** zum Spike, Spike 2: Die Retry-Politik von valkey-py (5 Wiederholungen, Socket-Timeout) erklärt die rund 65 s. Ein `op_timeout` außen um den Aufruf deckelt sie, ohne die Politik zu ändern. Wichtig im Zusammenspiel mit FS-06: Läuft der Renew nach einem Timeout im Hintergrund weiter, muss er idempotent sein.

---

### 16. Kleinere P3-Befunde

- **FS-15** (subscribe ohne Frist), E-030 (pack: …, e030-get-result-polling.md)
  > „Nicht von dieser Entscheidung gedeckt ist ein `subscribe()` ohne Frist … Das ist ein Fehler (FS-15, Teil 3)“

  Ausgeschlossen: Polling-Intervall, Backoff oder die Dauer von Pub/Sub zu ändern; das ist entschieden.
  Learning Spike 3: Pub/Sub verbindet sich nicht automatisch neu.
- **FS-27**, E-012
  > „Der Namensraum muss kleingeschrieben sein (`[a-z0-9][a-z0-9_.-]{0,63}`, nicht `sq`).“

  Der Vorschlag in der Fehlermeldung muss ebenfalls gültig sein.
- **FS-28**, E-039
  > „FS-28 betrifft nur den Meldungstext … Das Verhalten bleibt.“

  Ausgeschlossen: `result_ttl=0` schon bei `Task.s()` abzulehnen; das ist eine offene Produktfrage.

---

## Empfehlungen für den Plan

1. **Abhängigkeit E-001 mit FS-02a ausdrücklich benennen** (Abschnitt 1). Nach dem Fix führt ein Drain-Abbruch am Shutdown-Ende zu Neuzustellung plus eventuell gespeichertem Fehlerergebnis, statt zu Retry oder DLQ. E-001 bleibt formal eingehalten, seine Prämisse trägt aber nicht. Das ist zu melden, nicht zu unterdrücken.
2. **Lücke in E-038:** Welche Recycling-Drainfrist gilt ohne registrierten `hard_timeout`? Das ist nicht entschieden. Der Plan muss eine technische Festlegung treffen, `test_recycling.py:84` anpassen (kein Marker) und den Wert ins Register übernehmen (AGENTS.md des Registers, Regel 4).
3. **Vorlagen im Code nutzen:**
   - d7d94c5 (idempotenter Claim mit eigenem Owner, Lua und In-Memory, Contract-Test) für FS-06.
   - 7e6abec (begrenzte, an den Owner gebundene Freigabe im Hintergrund) für FS-07.
   - Das `Redelivered`-Outcome aus E-008 für das Zurückstellen bei FS-03.
4. **Mischbetrieb je Fix als Grenze aufschreiben:**
   - FS-03: Alte Worker bestätigen weiterhin.
   - FS-14: Ein alter Leader ignoriert das neue Feld.
   - FS-10: Alte Worker lassen die Header stehen.
   - Lua-Änderungen behalten Rückgabecodes und Marker-Werte.
   - Structs nur am Ende erweitern, ohne neue `Literal`-Variante.
5. **Tests mit falscher Prämisse ohne Marker:** `test_schedfire.py::test_duplicate_when_inflight_claim_lost` und `test_recycling.py:84`. Diese dürfen angepasst werden. Alle Tests mit `@pytest.mark.entscheidung` (96 Dateien) bleiben unverändert.
6. **FS-04 nicht über das Thema „Ack vor DI-Scope-Ende“ lösen**, das ist offen. Lösen über verschachteltes try/finally bzw. einen Finalizer-Fehler, der nur geloggt wird.
7. **Learning-Pflege nach dem Fix:**
   - `worker-pipeline-stage-ack-ordering` Rule 3 zeigt die fehlerhafte `int()`-Form (FS-19).
   - Das Dishka-Learning nennt „close failure logged, not raised“ nur für APP und sollte den REQUEST-Scope ergänzen.
   - Der Toxiproxy-Spike sollte den Hinweis bekommen, dass die Retry-Politik idempotente Skripte voraussetzt.
