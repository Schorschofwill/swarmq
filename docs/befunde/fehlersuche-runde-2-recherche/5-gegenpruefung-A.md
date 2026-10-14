# Gegenprüfung A: FS-01 bis FS-05, FS-17 bis FS-21

Stand: Worktree `swarmq-fehlersuche`, Branch `test/fehlersuche-runde-2` (ddeca96), 2026-10-13. Ich habe nur gelesen und Unit-Tests ausgeführt. Alle Nachweise von FS-01 bis FS-05 sind mit `--runxfail` rot, und zwar aus dem behaupteten Grund: 7× FS-01, 4× FS-02, 2× FS-03 und je 2 bzw. 4 bei FS-04/FS-05.

---

## FS-01: Drain-Abbruch wird zum Fehlerpfad statt zur Neuzustellung

**Einstufung: ENTSCHEIDUNG für den Kern (SIGTERM-Drain, IO und CPU). Mit neuem Argument sollte sie überdacht werden.** Der Recycling-Teil (MB-2) ist dagegen ein **FEHLER** und gehört als eigener Befund abgetrennt.

### Belege für eine Entscheidung

- `docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:258`, KTD2: „Auch der Abbruch des Handlers am Drain-Ende wird so abgebildet (Kill, Tod bestätigen, Fehlerergebnis), nie als propagierter `CancelledError` … Damit laufen Retry, DLQ und Signale ohne neuen Code durch `_resolve_error_path` und `RetryHandler.evaluate`. (session-settled: user-approved …) „Fehlerpfad wie heute“ …“.
  - Mit „wie heute“ ist das IO-Verhalten als Vorbild gemeint. Die IO-Abbildung ist damit mitbestätigt.
- Gleicher Plan `:564` (U6, Schritt 5): „Handler-Cancel während des Wartens (Drain-Ende): … dasselbe Fehlerergebnis wie beim Timeout liefern“.
- `src/swarmq/worker/execution.py:196`: „"timeout" or None (bare CancelledError): hard-timeout path.“ Eingeführt mit 44d99ca (2026-04-16, Phase 3b.3). Der Commit-Text sagt „else → existing hard-timeout path (RuntimeError, retry-eligible)“.
- Bestehende Tests schreiben das Verhalten fest: `tests/unit/worker/compute/test_executor.py:190` (`test_outer_cancel_during_drain_kills_child_and_returns_error_result`) und `:479` (`test_hard_timeout_and_drain_errors_have_no_error_class`, prüft `"drain" in error_message`).
- Die neueste Nutzer-Doku (0.12.0, P9) beschreibt Drain-Fehler als Fehlerergebnis:
  - `website/reference/agent/message-formats.md:112` und `website/reference/errors.md:258`: „failures the worker generates itself (hard timeout, drain, lost compute child …)“.
  - Changelog 0.12.0rc2 `website/reference/changelog.md:42` und `:196`.
  - Plan `docs/plans/2026-10-12-1105-feat-phoenix-kleinpunkte-plan.md:101, 283, 564`.

### Belege dagegen (Zusagen)

- `website/explanation/delivery-semantics.md:267-268` (DE `.de.md:286`): „A task that runs longer than the drain is cancelled and redelivered (see above)“. Der Satz kam mit fd71b31 (BZ-18, 2026-10-11) und widerspricht KTD2 und P9.
- CPU-Plan `:603` (U7-Testszenario): „Drain wartet `shutdown_timeout`, danach sind Kinder tot und Nachrichten redelivered“. Auch das widerspricht KTD2 im selben Plan.
- Ursprünglicher Lifecycle-Entwurf `docs/recherche-deepening-broker-lifecycle.md:716, 736`: SHUTTING_DOWN = „Task.cancel(), nack prefetched“.

### Neues Argument gegen KTD2

KTD2 setzt voraus, dass Retry und DLQ nach dem Abbruch „ohne neuen Code“ durchlaufen. Am Drain-Ende ist die Publish-Endfrist aber per Konstruktion verstrichen: `shutdown.py:271` setzt sie auf `drain_until`, und der Abbruch kommt genau zu diesem Zeitpunkt.

- Retry- und DLQ-Publish laufen deshalb mit Budget 0. Über FS-02 gehen sie trotzdem raus und melden `PublishIndeterminate`, danach folgt `redeliver`.
- Ergebnis bei `max_retries=0`:
  - Fehlerergebnis plus `notify`,
  - Workflow FAILED (`retry.py:276-301` läuft vor dem Publish),
  - DLQ-Kopie
  - **und** erneuter Lauf.
- Das ist schlechter als jede der beiden Optionen. KTD2 hat genau diesen Fall nicht betrachtet.
- Das Problem, das KTD2 lösen sollte (ein propagierter Abbruch trifft den Terminal-Default des `finally`), lässt sich auch mit einem eigenen nicht-terminalen Outcome lösen.

### Recycling-Teil (MB-2): FEHLER

- Belege:
  - R22 (`:115`): „Ein Recycling verbraucht keinen Retry eines laufenden Tasks.“
  - KTD11 (`:267`): „drainiert ohne erzwungenen Abbruch“.
  - Changelog `:2293-2296`.
- `recycling.py:78-86` fällt ohne registrierten `hard_timeout` (Default `None`) auf `shutdown_timeout` zurück. Der Docstring („sonst `shutdown_timeout`“) und `test_recycling.py:84` schreiben den Rückfall zwar fest.
- Eine Begründung oder Plan-Entscheidung dafür fand ich nicht. Der Plan nennt nur „größter registrierter `hard_timeout` plus Puffer“ und bedenkt den leeren Fall nicht. Das ist eine Umsetzungslücke gegen eine klare Zusage.

### Realismus

- IO-Test: echter `_on_message`/`_execute_shutdown`, Broker und Backend als Mock. Das gibt den Pfad korrekt wieder; der Mock-`dead_letter` gelingt sogar, im Betrieb ist es noch schlimmer (siehe oben).
- Der Auslöser kommt bei jedem Deploy vor, wenn ein Task länger als etwa 23 s läuft (Supervisor) bzw. 25 s (eingebettet). Die Doku empfiehlt dafür, `SWARMQ_SHUTDOWN_TIMEOUT` anzuheben.
- MB-5 (1-s-Nachlauf im CPU-Modus) ist nicht belegt; Konfidenz niedrig, P3.

### Priorität

- Kern: P1 als Frage „Entscheidung überdenken“ (Folgen siehe oben). Unabhängig davon ist der Doku-Widerspruch zu beheben.
- Recycling: P2 (Opt-in-Funktion, nur Tasks ohne `hard_timeout`).

### Test-Anpassung

- `test_befund_fs01_drain_abort.py` und `compute/test_befund_fs01_cpu_drain_abort.py` prüfen eine Option, die KTD2 ausdrücklich verworfen hat. Als „Fehler“-Nachweis taugen sie erst nach einer Produktentscheidung.
- Vorschlag: durch einen entscheidungsneutralen Invariantentest ersetzen oder ergänzen: „nach Drain-Abbruch nie zugleich Fehlerergebnis/Workflow FAILED und Neuzustellung“. Dazu einen echten `RabbitMQBroker` mit `FakeTransport` verwenden, damit die verstrichene Endfrist wirkt.
- `test_befund_fs01_recycling_drain.py` ist in Ordnung und gehört zum abgetrennten Recycling-Befund.

**Konfidenz:** hoch für Ablauf und Belege; die Einstufung des Kerns hängt an Georgs Bewertung von KTD2.

---

## FS-02: Publish nach der Endfrist geht raus und meldet `PublishIndeterminate`; Recycling schneidet nach etwa 23 s ab

**Einstufung: FEHLER**, mit Einschränkung beim Teil (a).

### Belege

- **Zusage (a):** Docstring `broker/rabbitmq/connection.py:463-466`: „eine bereits verstrichene Endfrist ergibt 0.0 und lässt den Publish sofort scheitern“. Gemeint ist: nicht senden.
- **Nachbarpfad (a):** `delivery.py:187-193`. Bei `BrokerPublishBlocked` unterscheidet swarmq schon „nichts gesendet, sicher wiederholbar“ von „unbestimmt“.
- **Teilweise akzeptiert (Gegenbeleg zu a):** Plan `docs/plans/2026-08-28-001-fix-publish-deadline-port-guard-logfix-plan.md`:
  - KD2 (`:48`): „der Preis ist ein unbestimmter Ausgang, wenn die Deadline **mitten in einem Versuch** reißt“.
  - AE2 (`:82`): „Der bewusst getragene Preis ist … mindestens eine zusätzliche Ausführung, möglicherweise neben einem Eintrag in der Dead-Letter-Queue“.
  - R11 (`:63`): ein Deadline-Fehler heißt „möglicherweise zugestellt“.
  - Doppelausführung nach einem Indeterminate ist damit grundsätzlich entschieden. Neu ist: Bei einer **schon vor Beginn** verstrichenen Frist ist der Ausgang nicht unbestimmt, sondern vermeidbar. Und er tritt im Shutdown systematisch auf, nicht nur selten.
- **Zusage (b, Recycling):**
  - BZ-18-Entscheidung im Plan `2026-10-09-2314-…:603`: „Der Recycling-Drain ist ausgenommen“.
  - Changelog 0.12.0rc1 `changelog.md:673-674`: „The publish deadline in the shutdown is the end of the drain.“ Beim Recycling ist das Drain-Ende `max(hard_timeout)+30`, die Endfrist liegt aber bei `drain_until` (`shutdown.py:271`).
  - Dazu R22/KTD11 (siehe FS-01).
  - Das Problem besteht schon seit 5ef56b7 (0.5.0, damals `_shutdown_publish_budget`), ist also keine 0.12-Regression. Eine Entscheidung dazu fand ich nicht.

### Realismus

- Test (a) nutzt den echten Code von `aiormq.Channel.basic_publish` und `aio_pika.Exchange` mit `FakeTransport` aus `tests/support`, das es seit B24 gibt. Ich habe im `.venv` geprüft: In `aiormq/channel.py:618-680` gibt es auf dem Fast-Path vor dem Einreihen der Frames keinen Suspendierungspunkt. Der Ablauf ist realistisch.
- Test (b): skalierte Zeiten, echter Broker-Code. Realistisch.
- Häufigkeit:
  - (a) jeder Publish nach dem Drain-Ende, also zwangsläufig im FS-01-Fehlerpfad und beim Leader-Poll vor `scheduler.stop()`.
  - (b) nur mit aktivem Recycling (Opt-in), dort aber systematisch für Retry, DLQ, Workflow-Advance und einen recycelnden Leader. Ein reines Erfolgs-Ack ist nicht betroffen, weil die Frist nur für Publishes gilt (`delivery.py:171`, `redeclare.py:61`).

### Priorität

- (b) P1 für Betreiber mit Recycling.
- (a) allein P2: Folgen wie AE2 akzeptiert, aber systematisch statt selten.
- Gesamt bleibt P1.

### Test-Anpassung

- Den Register-Hinweis „Invariante nie gescheitert und zugleich gesendet“ auf „Frist **vor Beginn** verstrichen“ einschränken. Allgemein widerspricht die Invariante R11/AE2.
- Der Test selbst prüft nur diesen Fall und ist deshalb in Ordnung.

**Konfidenz:** hoch.

---

## FS-03: Schedfire-Gate bestätigt eine Broker-Neuzustellung mit fremdem Claim (Verlust), Semaphor-Warten außerhalb von try/finally

**Einstufung: FEHLER.**

### Belege

- **Ziel der Zwei-Marker-Lösung:** Plan `docs/plans/2026-04-17-003-fix-phase3c-review-followup-plan.md`, Unit 1 (H-2): „Ziel: at-least-once-Garantie bei SETNX-winner-crash-before-ack wieder herstellen.“
  - Geplanter Test: „simulate SETNX-winner-crash → redelivery nach TTL-Expiry → neue Worker-Instanz kann den Task erfolgreich ausführen“.
  - Dieser Test fehlt in `tests/integration/test_worker_schedfire_dedup.py`.
- **Falsche Prämisse in Code und Test:**
  - Gate-Docstring `schedfire.py:113-116`: „if that worker crashes … the in-flight key expires and the next redelivery re-runs“.
  - Ebenso `test_schedfire.py:215-219`. Nach dem Ack gibt es aber keine nächste Zustellung.
  - Der Test, der „fremder Claim → Ack“ festschreibt, beruht also auf einer falschen Annahme. Eine Entscheidung für den Verlust ist er nicht.
- **Lösungsdoku:** `docs/solutions/logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md`. Stiller Drop eines Fires gilt dort als `severity: critical`, und „ein versehentliches True würde den stillen Drop reproduzieren“.
- **Nachbarpfad:** Der Ausführungsschutz stellt bei „running elsewhere“ zurück, statt zu bestätigen (`delivery-semantics.md:331-335`).
- **K-5:**
  - Der Docstring von `_pre_pipeline_gates` (`pipeline.py:424-427`) verlangt ausdrücklich: Endet die Zustellung vor dem `try/finally` nach gewonnenem Claim, wird der Claim freigegeben, „sonst sähe die Kopie ihn als Duplikat“.
  - `await self._semaphore.acquire()` (`pipeline.py:575`) liegt in derselben Lücke, ist aber nicht abgesichert. Die Invariante ist im Nachbarpfad also schon durchgesetzt.
- **Gegenbelege (teilweise):**
  - `src/swarmq/lua/sched_fire_claim.lua:9-11` und `schedfire.py:157-158` (d93d3ab, 0.9.0): „eine Broker-Redelivery derselben Kopie (altes Token) bleibt ein Duplikat“. Das richtet sich gegen Parallelausführung bei Kanalverlust, ohne den Absturzfall abzuwägen.
  - `delivery-semantics.md:178-184` nimmt Fire-Verlust nur im Doppelausfall (Valkey und Broker) ausdrücklich hin, nicht beim Absturz.
  - Ein Fix muss den Kanalverlust-Fall weiter vor Parallelausführung schützen. Das Zurückstellen statt Bestätigen leistet das.

### Realismus

- Test 1 setzt `redelivered=True` und `x-delivery-count=1` und mockt `claim_fire` mit dem Ergebnis 0. Das entspricht dem echten Ablauf nach SIGKILL: Die TTL des Claims beträgt mindestens 300 s, die Neuzustellung kommt sofort.
- Test 2 nutzt die echte Pipeline mit `concurrency=1`. Realistisch.
- Kleinigkeit: `_SLOW_STARTED`/`_NEVER` sind modulweite `asyncio.Event` und werden je Loop gebunden. Das ist heute unschädlich, wird aber fragil, sobald ein zweiter Test sie nutzt.

### Priorität

P1 bleibt: K-5 tritt bei jedem Deploy mit vollen Slots deterministisch auf, K-2 bei OOM/SIGKILL.

### Test-Anpassung

Keine. Ich empfehle, den Docstring von `test_schedfire.py::test_duplicate_when_inflight_claim_lost` beim Fix zu korrigieren.

**Konfidenz:** hoch.

---

## FS-04: Ein scheiternder Dishka-Finalizer überspringt Lock-Freigabe und Semaphor-Rückgabe

**Einstufung: FEHLER.**

### Belege

- **Invariante, die swarmq sonst durchsetzt:**
  - `tests/unit/worker/test_worker.py:418-424`: „Semaphore … released in finally“ / „released even when the task raises“.
  - Ursprung: `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`.
  - Locks gibt nur der Besitzer frei (`docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md:48`). Dabei geht es um Freigabe, nicht um Verlängerung auf Lebenszeit.
- **Der Auslöser ist ein empfohlenes Muster:**
  - DI-Plan `docs/plans/2026-05-31-001-feat-dishka-di-tasks-plan.md:39, 96` (R13): „Commit/Rollback gehört in den Finalizer“.
  - Brainstorm `docs/brainstorms/2026-05-31-dishka-di-tasks-requirements.md:48`.
  - Ein scheiternder Commit (Constraint-Verletzung, DB-Verbindung weg) ist damit ein normaler Fall.
- Eine Entscheidung, Finalizer-Fehler so zu behandeln, gibt es nicht.
- Dishka 1.10 wirft tatsächlich `ExitError` (`.venv/.../dishka/async_container.py:340`). `_detached` reicht ihn über `asyncio.shield` durch (`pipeline.py:330-336`), und der Rest des `finally` (`:712-715`) entfällt.

### Realismus

- Echter Dishka-Container, echter `InMemoryLockStore`. Der Ablauf ist realistisch.
- Nur der IO-Modus ist betroffen. Im CPU-Modus liegt der REQUEST-Scope im Kind.

### Priorität und Test

- P1 bleibt für den Lock-Fall (clusterweiter Stillstand eines Lock-Namens). Ohne Lock schleichendes Aushungern, P2.
- Keine Test-Anpassung.

### Nebenbefund (nicht gemeldet)

Plan `:118` sagt: „Retry-Entscheidung und Result-Storage laufen **nach** dem Scope-Exit“. Tatsächlich bestätigt der Ergebnis-Tail **vor** dem Scope-Exit: Ein scheiternder Commit kommt nach dem Ack. Ob das gewollt ist, sollte Georg klären.

**Konfidenz:** hoch.

---

## FS-05: Eine eigene `TimeoutError` des Tasks wird zu „SoftTimeoutError after None s“

**Einstufung: FEHLER.**

### Belege

- **Plan** `docs/plans/2026-04-15-003-feat-phase2-production-core-plan.md:312`: „Wenn `task_info.soft_timeout` gesetzt: `await asyncio.wait_for(…)`. Fängt `asyncio.TimeoutError`, wirft `SoftTimeoutError`.“ Die Umwandlung war nur für den Soft-Timeout gedacht.
- **Doku** `website/reference/errors.md:415-416`: „When raised. The task body is still running when `soft_timeout` elapses.“
- `website/reference/configuration.md:451`: `retry_exceptions` ist eine „Whitelist — only these exceptions trigger a retry“.
- Changelog/P9: `error_class` = „the fully-qualified class of the exception“. Gespeichert wird aber `SoftTimeoutError`.
- Keine Gegenentscheidung. `git log -S 'soft timeout after'` zeigt nur 0ccb048, 7e54a8e, c8e3b1f und 374117b ohne Begründung für das unbedingte `except`.

### Realismus

- Python 3.14: Eine aus `run()` geworfene `TimeoutError` geht unverändert durch `wait_for` bzw. `await run_task` und landet in `execution.py:185`.
- CPU: `child.py:193`.
- Häufig: `aiohttp.ServerTimeoutError` erbt von `asyncio.TimeoutError`, dazu `asyncio.timeout` und `socket.timeout`.
- Spürbare Wirkung nur bei `retry_exceptions` mit `TimeoutError` (dann sofort DLQ) sowie bei Meldung und `error_class`.

### Priorität und Test

- P2 bleibt.
- Keine Test-Anpassung.

**Konfidenz:** hoch.

---

## FS-17: Expiry-Gate wertet gescheiterten Ack nach liegender DLQ-Kopie als Fehlschlag

**Einstufung: FEHLER (P3)**, Folgen im Register zu korrigieren.

### Belege

- Konvention im Nachbarpfad: `worker/gates/__init__.py:306-316` (`dead_letter_copy`): „`SettleFailedAfterPublish` … ist terminal wie ein Erfolg“.
- Doku `delivery-semantics.md:317-320`.
- `expiry.py:118-132` fängt dagegen alles als `BrokerConnectionError` und gibt `never_release` zurück. Eine Entscheidung dafür gibt es nicht. Der Kommentar `:108-116` stammt aus der Zeit vor `SettleFailedAfterPublish`.

### Korrektur der Folgen

- Mit dem Ausführungsschutz (Default an) setzt `GuardedBroker.dead_letter` bei `SettleFailedAfterPublish` den Done-Marker (`guarded_broker.py:51-61`). Die Neuzustellung wird als Duplikat übersprungen.
- **Keine** zweite DLQ-Kopie und **kein** zweites `TASK_EXPIRED`.
- Es bleibt:
  - Das Unique-Flag hängt bis `unique_for`, weil die übersprungene Neuzustellung das Expiry-Gate nie mehr erreicht. Das ist ein BZ-22-Rest.
  - Irreführende Logzeilen `task_expired_dlq_publish_failed` bzw. `…_nack_also_failed`.
- Doppelkopie und doppeltes Signal gibt es nur mit `SWARMQ_EXECUTION_GUARD=false`.

### Realismus und Test

- Selten, weil ein Kanalverlust genau zwischen Confirm und Ack nötig ist.
- P3 bleibt.
- Test (falls einer geschrieben wird): Erwartung `release_only` und Unique-Freigabe, nicht „kein zweites `TASK_EXPIRED`“ allein.

**Konfidenz:** hoch.

---

## FS-18: Expiry-Gate speichert ein Fehlerergebnis trotz `result_ttl=0` bzw. FAF

**Einstufung: ENTSCHEIDUNG.** Gemessen an Nutzer-Doku und Nachbarpfad ist das Verhalten richtig; falsch ist nur der Kommentar.

### Belege

- **Nutzer-Doku** `website/guides/dead-letter-queues.md:51`: „Dead-lettering by the worker — retries exhausted, `NoRetry`, `max_age`, … | … an error result is stored“. Keine Ausnahme für `result_ttl=0`.
- **Nachbarpfad, bewusst entschieden:**
  - `worker/retry.py:288-292` (ad81f71, Plan 2026-10-08-1541 U9): „Anders als der Erfolgspfad: kein FIRE_AND_FORGET-Zweig, und 0 fällt hier auf DEFAULT_RESULT_TTL_S statt "nicht speichern".“
  - Der Ablauf ist ebenfalls ein terminaler DLQ-Pfad und verhält sich gleich.
- **Herkunft des Kommentars** `expiry.py:81-83`:
  - 111983a, Härtung R2 P2, Plan `docs/plans/2026-06-03-001-fix-hardening-round-2-plan.md:89`: „`store_result` auf dem Expired-Pfad mit `ttl_ms=0` → Skip statt `SET PX 0`“.
  - Ziel war nur, den Valkey-Fehler bei `PX 0` zu vermeiden. „mirrors the main-path semantics“ beschreibt den Code nicht.

### Priorität und Test

- Keine Priorität als Fehler. Nur den Kommentar korrigieren.
- Kleiner Rest: Ablauf nutzt `config.result_ttl`, `dead_letter` nutzt `DEFAULT_RESULT_TTL_S`. Bei Default-Konfiguration ist das gleich.

**Konfidenz:** mittel-hoch.

---

## FS-19: Nicht ganzzahliger Zähler-Header lässt die Nachricht unquittiert

**Einstufung: FEHLER (P3).**

### Belege

Die Invariante ist an drei Stellen ausdrücklich formuliert: „ein kaputter Header zählt wie keiner“.
- `worker/gates/lock.py:118-124` (`_as_int`)
- `worker/gates/__init__.py:204-210` (`_as_count`)
- `worker/gates/execution_guard.py:57-60` (`retry_count_of`)

`rate_limit.py:86` und `retry.py:116-119` lassen sie aus. Eine Gegenentscheidung gibt es nicht.

### Präzisierung

- `int("3")` gelingt. Das Rate-Limit-Gate scheitert deshalb nur an nicht numerischen Werten.
- Der Retry-Pfad (`retry_count >= max_retries`) scheitert schon an der Zeichenkette `"3"` (TypeError). Das ist der realistische Fall: manuelles Neu-Publizieren über die RabbitMQ-UI, deren Header Zeichenketten sind. „Move messages“ bzw. Shovel erhalten die Typen.
- Folge im Retry-Pfad: Der Task ist schon gelaufen. Bis zum Delivery-Limit folgt je `consumer_timeout` ein weiterer Lauf.

### Priorität und Test

- P3 bleibt.
- Für einen Test: Retry-Fall mit `"1"` (Zeichenkette mit Ziffern) prüfen, Rate-Limit-Fall mit einem nicht numerischen Wert.

**Konfidenz:** hoch.

---

## FS-20: Doku: `SoftTimeoutError` erreicht `run()` nie

**Einstufung: FEHLER (Doku).**

### Belege

- **Zusagen:**
  - `website/guides/scheduling-and-retries.md:674-675` (DE entsprechend): „The worker raises `SoftTimeoutError` inside `run()` so your task can checkpoint“.
  - `website/reference/errors.md:422-441` mit Codebeispiel `except SoftTimeoutError: await checkpoint()`, das nie greift.
- **Ursprung der Anforderung:** `docs/brainstorms/implementation-order-requirements.md:70` (R2.4: „SoftTimeoutError (fangbar …)“).
- **Umsetzungsentscheidung:** `docs/plans/2026-04-15-003-…:76, 312`: Soft-Timeout über `asyncio.wait_for`. Das bricht das innere Awaitable ab (CancelledError im Task) und kann eine fangbare Ausnahme technisch gar nicht liefern.
- Die Doku beschreibt also die verworfene Anforderung. Kein Beleg, dass die Abweichung bewusst in die Doku übernommen wurde.

### Priorität und Test

- P3. Ich würde zu P2 tendieren, weil das Codebeispiel aktiv in die Irre führt: Ein Checkpoint läuft nie, und ein Task über `soft_timeout` startet bei jedem Retry neu.
- Kein Test nötig. Ein Doku-Test oder ein Unit-Test „`run()` sieht `CancelledError`“ wäre möglich.

**Konfidenz:** hoch.

---

## FS-21: Backoff am Deckel nicht monoton, gekappte Retries warten höchstens 0,8 × `retry_max_delay`

**Einstufung: ENTSCHEIDUNG.**

### Belege

- Register `docs/befunde/betrieb-ueber-zeit-2026-10.md:140` (BZ-26): „Nur Deckel-Jitter und Overflow werden behoben.“
- Plan `docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:290`: „Oberhalb von `max_delay` gilt `max_delay·(0,6+0,2u)`“.
- Nutzer-Doku `website/guides/scheduling-and-retries.md:590-592`: „A delay above the cap becomes 0.6 to 0.8 × `retry_max_delay`“.
- Changelog `:363-365` und `:766-768`.
- Produktfrage 4 („Obergrenze oder Mittelwert?“) ist damit beantwortet: Es ist eine Obergrenze, gekappte Werte liegen bei 0,6 bis 0,8.
- Die fehlende Monotonie an der Deckelgrenze folgt direkt aus der dokumentierten Formel.

### Mögliches neues Argument

Wer das Fenster `max_retries × retry_max_delay` bemessen hat, bekommt 20–40 % weniger. Die Doku nennt die Formel, aber nicht diese Folge. Das ist ein schwaches Argument, höchstens ein Doku-Satz.

### Priorität und Test

- Keine Priorität als Fehler. Produktfrage 4 streichen.
- Kein Nachweis-Test.

**Konfidenz:** hoch.

---

## Übersicht

| ID | Einstufung | Prio alt→neu | Kernbeleg |
|---|---|---|---|
| FS-01 | ENTSCHEIDUNG (Kern), Recycling-Teil FEHLER | P1 → P1 (Entscheidung überdenken); Recycling-Teil abtrennen, P2 | KTD2 `2026-09-03-001-…:258` (user-approved) vs. `delivery-semantics.md:267-268`; Recycling: R22 `:115` |
| FS-02 | FEHLER | P1 → P1 ((b) Recycling); (a) allein P2 | Docstring `connection.py:463-466`; BZ-18 „Recycling-Drain ausgenommen“ (`2026-10-09-2314-…:603`); Changelog `:673-674`; Gegenbeleg AE2 `2026-08-28-001-…:82` |
| FS-03 | FEHLER | P1 → P1 | H-2-Ziel `2026-04-17-003-…` Unit 1; `pipeline.py:424-427` (Freigabe vor `try/finally`) |
| FS-04 | FEHLER | P1 → P1 (mit Lock), P2 ohne | `test_worker.py:418-424` (Semaphor im finally); DI-Plan R13 (Commit im Finalizer) |
| FS-05 | FEHLER | P2 → P2 | `2026-04-15-003-…:312` (nur bei gesetztem `soft_timeout`); `errors.md:415-416` |
| FS-17 | FEHLER | P3 → P3 (Folgen korrigieren) | `gates/__init__.py:306-316` (`dead_letter_copy`) |
| FS-18 | ENTSCHEIDUNG | P3 → kein Fehler (Kommentar korrigieren) | `dead-letter-queues.md:51`; `retry.py:288-292` |
| FS-19 | FEHLER | P3 → P3 | `lock.py:118`, `gates/__init__.py:204`, `execution_guard.py:57` (kaputter Header zählt wie keiner) |
| FS-20 | FEHLER (Doku) | P3 → P3 (eher P2) | `scheduling-and-retries.md:674-675`, `errors.md:422-441` vs. `wait_for`-Entscheidung `2026-04-15-003-…:76` |
| FS-21 | ENTSCHEIDUNG | P3 → kein Fehler | BZ-26 `betrieb-ueber-zeit-2026-10.md:140`; Plan `2026-10-09-2314-…:290`; `scheduling-and-retries.md:590-592` |

## Entscheidungsquellen außerhalb von `docs/befunde` (für ein künftiges Register)

### Pläne

- `docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md`:
  - KTD2 `:258`: Drain-Abbruch, Timeout und Kindtod werden zum Fehlerergebnis; user-approved.
  - R22 `:115` und KTD11 `:267`: Recycling ohne Abbruch, kein Retry-Verbrauch.
  - U6 Schritt 5 `:564`.
  - Testszenarien `:603` (widerspricht KTD2) und `:673`.
- `docs/plans/2026-08-28-001-fix-publish-deadline-port-guard-logfix-plan.md`:
  - KD2 `:48`, KTD1 `:179`: Gesamtfrist mit unbestimmtem Ausgang.
  - R4 `:61`, R11 `:63`, AE2 `:82`: Zusatzlauf und DLQ-Eintrag bewusst getragen.
  - KTD9 `:184`.
- `docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md`:
  - MR 17 `:599-603`: Recycling-Drain vom Budget ausgenommen.
  - MR 2 `:285-291`: Backoff-Formel am Deckel.
- `docs/plans/2026-04-17-003-fix-phase3c-review-followup-plan.md`, Unit 1 (H-2): at-least-once beim Absturz des Claim-Gewinners.
- `docs/plans/2026-04-15-003-feat-phase2-production-core-plan.md:76, 312`: Soft-Timeout über `wait_for`, Umwandlung nur bei gesetztem `soft_timeout`.
- `docs/plans/2026-05-31-001-feat-dishka-di-tasks-plan.md:39, 94, 96, 118`: Commit im Finalizer; Scope-Exit geshieldet.
- `docs/plans/2026-06-03-001-fix-hardening-round-2-plan.md:89`: Expiry ohne `PX 0`.
- `docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md`, U9: DLQ-Ergebnis-TTL, im Code `retry.py:288-292`.
- `docs/plans/2026-10-12-1105-feat-phoenix-kleinpunkte-plan.md:101, 283, 564`: Drain-Abbruch als vom Worker erzeugter Fehler ohne `error_class`.
- `docs/plans/2026-04-16-009-feat-phase3b3-task-cancellation-plan.md:1016`: Detached-Release-Set für Outer-Cancel.

### Brainstorm und Recherche

- `docs/brainstorms/implementation-order-requirements.md:70` (R2.4: fangbarer `SoftTimeoutError`, später durch `wait_for` ersetzt).
- `docs/recherche-deepening-broker-lifecycle.md:716, 736` (SHUTTING_DOWN: cancel + nack).

### Lösungsdoku

- `docs/solutions/logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md`: Stiller Fire-Drop ist kritisch; im Zweifel überfreigeben.
- `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`.

### Code-Kommentare mit Festlegungscharakter

- `src/swarmq/worker/execution.py:196`: bloßes `CancelledError` → Hard-Timeout.
- `src/swarmq/worker/recycling.py:78`: „sonst `shutdown_timeout`“.
- `src/swarmq/lua/sched_fire_claim.lua:9-11` und `src/swarmq/worker/gates/schedfire.py:157-158`: „Broker-Redelivery derselben Kopie bleibt ein Duplikat“.
- `src/swarmq/worker/gates/schedfire.py:113-116`: falsche Prämisse „next redelivery re-runs“.
- `src/swarmq/worker/gates/__init__.py:306-316`: `SettleFailedAfterPublish` gilt als terminal.
- `lock.py:118`, `gates/__init__.py:204`, `execution_guard.py:57`: Regel zu kaputten Headern.
- `src/swarmq/broker/rabbitmq/delivery.py:187-193`: „nichts gesendet“ ≠ Indeterminate.
- `src/swarmq/worker/pipeline.py:424-427`: Claim-Freigabe vor `try/finally`.

### Nutzer-Doku und Changelog

- `website/explanation/delivery-semantics.md`:
  - `:178-184`: akzeptierter Fire-Verlust nur im Doppelausfall.
  - `:267-268`: „cancelled and redelivered“, widerspricht KTD2.
- `website/guides/dead-letter-queues.md:51`: Ablauf speichert ein Fehlerergebnis.
- `website/guides/scheduling-and-retries.md:590-592`: Backoff am Deckel.
- `website/reference/errors.md:258` und `website/reference/agent/message-formats.md:112`: Drain-Fehler.
- `website/reference/changelog.md`:
  - `:663-681`: BZ-18, „publish deadline … is the end of the drain“, „recycling drain is unchanged“.
  - `:2293-2296`: Recycling „without forced cancellation“.

### Tests, die Verhalten festschreiben

- `tests/unit/worker/compute/test_executor.py:190, 479`: Drain-Abbruch wird zum Fehlerergebnis.
- `tests/unit/worker/test_recycling.py:84`: Rückfall auf `shutdown_timeout`.
- `tests/unit/worker/gates/test_schedfire.py:215`: fremder Claim führt zum Ack, Prämisse falsch.
