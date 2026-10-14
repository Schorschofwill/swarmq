# Fix-Recherche Broker/Publish: FS-02, FS-08, FS-09, FS-10

Stand: Worktree `swarmq-fehlersuche` (v0.12.0 + Nachweise, 906afac), Register `swarmq-entscheidungen` (45c6336). Nur gelesen. Ausgeführt habe ich die Unit-Nachweise mit `--runxfail`: Alle scheitern aus dem im Register genannten Grund. Bibliotheken laut `.venv`: aiormq 7.0.0, aio-pika 10.0.1, pamqp 4.0.1, Python 3.14.4. Integration habe ich nicht ausgeführt.

Methodik nach repo-research-analyst (Scope: patterns). Je Befund folgen Ursache, Optionen mit Folgefehler-Analyse, Empfehlung, Tests und Aufwand/Risiko.

---

## FS-02 (P1): Publish nach verstrichener Endfrist geht raus und meldet `PublishIndeterminate`

### Ursache

**Teil a (Endfrist vor Beginn verstrichen).**
- `broker/rabbitmq/connection.py:460-471` `_effective_publish_timeout()` liefert `max(0.0, …)`, also `0.0`. Der Docstring sagt „lässt den Publish sofort scheitern“.
- `delivery.py:171-184`: `async with asyncio.timeout(0.0)`. asyncio plant den Abbruch beim Eintritt nur per `call_soon` ein.
- Bis zum ersten Suspendieren läuft alles synchron durch:
  - `_wait_while_blocked`: unblockiert kein `await`.
  - `_ensure_publish_channel` (`connection.py:578-590`): Fast-Path ohne Sperre.
  - `aio_pika/exchange.py:197-210`: Bei `Channel.get_underlay_channel` suspendiert nichts, auch `RobustChannel.get_underlay_channel` → `connection.ready()` → `Event.wait()` nicht, wenn das Event gesetzt ist.
  - `aiormq/channel.py:652-688`: Sperre frei, `write_queue.put` auf eine unbegrenzte Queue.
- Erstes Suspendieren ist `await drain_future` (`channel.py:688`). Der Frame liegt dann schon in der `write_queue`, der Writer schreibt ihn.
- Der Abbruch trifft `drain_future`, aus `TimeoutError` wird `PublishIndeterminate` (`delivery.py:187-211`), dazu kommt `_discard_publish_channel`.
- Der Nachweis zeigt `ChannelFrame … cancelled` auf der Leitung. **Wichtig für alle Optionen:** Ein abgebrochenes `drain_future` holt den Frame bei lebender Verbindung **nicht** aus der Queue.
- Aufrufer werten das als gescheitert und stellen das Original zurück:
  - `worker/retry.py:157-165` → `redeliver_in_place`
  - `retry.py:311-336` (`dlq_publish_failed`) → `redeliver_in_place`
  - `gates/__init__.py:281-285`, `results.py:258-266` (Advance)
  - `scheduler/fire.py:299-308`: kein Commit, Claim bleibt stehen
- Ergebnis: Kopie in DQ/XQ **und** Requeue, also Doppelausführung.

**Teil b (Recycling).**
- `worker/recycling.py:113` setzt `_drain_timeout_override = recycle_drain_timeout()`, das ist max(hard_timeout) + 30 bzw. `shutdown_timeout`.
- `worker/shutdown.py:103-111`: Der Drain nutzt den Override.
- `shutdown.py:269-272` setzt die Publish-Endfrist aber unverändert auf `started_at + budget.drain_until`. Unter dem Supervisor sind das bei `shutdown_timeout=30` 23 s (`shutdown_budget.py:53-56`).
- `_shutdown_end` (`:270`) bleibt ebenfalls bei `finish_by`. Nach einem langen Recycling-Drain bekommt deshalb jeder Aufräumschritt nur noch `_SHUTDOWN_STEP_MIN_S` = 1 s. Das ist Nachbarschaft, nicht Teil des Befunds.
- Jeder Retry-, DLQ-, Advance- und Fire-Publish eines Langläufers nach 23 s endet über Teil a.
- `_request_shutdown` (`shutdown.py:370-378`) ignoriert ein SIGTERM während des Recycling-Drains (Zustand `DRAINING`): weder Drain noch Frist werden verkürzt. Das ist Bestand (E-018 „Recycling-Drain ausgenommen“), siehe Schnittstelle FS-30.

### Optionen Teil a

**a1: Vor dem Publish prüfen, nichts senden, `BrokerConnectionError` (empfohlen).**
- In `_publish_with_retry` vor `asyncio.timeout`: Ist `_publish_deadline_at` gesetzt und `loop.time() >= _publish_deadline_at`, wirft der Code `BrokerConnectionError("publish to … not sent: the shutdown publish deadline has passed")`, ohne Kanal, ohne Verwurf, ohne Log-WARNING `publish_deadline_exceeded`.
- `_effective_publish_timeout` bleibt. `test_shutdown_deadline.py::test_elapsed_shutdown_deadline_fails_immediately` prüft `== 0.0` und `BrokerConnectionError` und bleibt grün.
- `redeclare.py:61` (Redeclare unter `asyncio.timeout(0)`) wird nicht mehr erreicht, weil der erste Publish schon scheitert. Gleiche Prüfung dort optional.
- **Aufrufer:**
  - Retry/DLQ/Gates/Advance führen zu `redeliver_in_place` wie bei jedem Nicht-Indeterminate-Fehler. Nur das Original kehrt zurück, keine Kopie.
  - `redeliver_in_place` gibt einen Schedfire-Claim **vorher** frei (`gates/__init__.py:251-276`), die Neuzustellung läuft also.
  - Scheitert die Freigabe, kommt der Rückfall `retry_later` (ebenfalls „nicht gesendet“) und dann `nack`. Der Claim bleibt, und FS-03 entscheidet über Verlust. Randfall, heute ebenso.
  - Scheduler (`fire.py:304-307`): kein `PublishIndeterminate`, deshalb `_release_own_claim`. Richtig, weil nichts gesendet wurde. Der nächste Leader feuert.
  - Client (`client/schedule.py:294-305`): Rollback der Unique-Sperre (richtig, E-041 gilt nur für Indeterminate).
  - Workflow-Advance: AT_LEAST_ONCE (E-039) → `redeliver`.
- **Entscheidungen:**
  - E-042 wird erfüllt („nachweislich nicht gesendet → `BrokerConnectionError`“). Die drei E-042-Tests laufen ohne Endfrist und sind unberührt.
  - E-041: Der Test `test_dead_letter_losing_the_copy_confirm_is_indeterminate…` ist unberührt. E-041 nennt Teil a ausdrücklich als „nicht gedeckt“.
  - E-025: `test_shutdown_deadline_cuts_the_wait_short` setzt die Frist erst während des Wartens und bleibt `BrokerPublishBlocked`. Bei Blockade **und** schon verstrichener Frist käme künftig `BrokerConnectionError` statt `BrokerPublishBlocked`. Beides heißt „nicht gesendet“; nur die Rücktrittszählung unterscheidet sich (s. u.).
  - **E-013:** Plain `BrokerConnectionError` zählt für den Leader-Rücktritt, `PublishIndeterminate` nicht. Ein Leader im Shutdown, der zwischen `drain_until` und `scheduler.stop()` feuert, zählt jetzt. Ein Rücktritt braucht ≥ 2 Fehler über ≥ `scheduler_publish_stepdown_s` (Default Lock-TTL). Im Shutdown-Fenster von etwa 5 s passiert das nur, wenn die Serie schon vorher lief. Folge wäre ein Lock-Release ohne Handover-Marker kurz vor dem ohnehin folgenden `stop()`, also kosmetisch (FAILOVER- statt LEADER-Signal).
  - **E-001 (Folgen-Text):** Ein Drain-Abbruch bei SIGTERM „verbraucht einen Retry / landet in der DLQ“. Mit a1 wird stattdessen das Original zurückgestellt: Fehlerergebnis und Workflow-FAILED laufen wie heute vorher, dann folgt ein erneuter Lauf **ohne** DLQ-Kopie und ohne Retry-Verbrauch. Das ist weniger falsch als heute (DLQ-Kopie **und** erneuter Lauf), weicht aber vom Folgen-Text in E-001 ab. Die E-001-Tests (`compute/test_executor.py`) prüfen nur das Ergebnisobjekt und bleiben grün. Das ist eine Abhängigkeit zur offenen Produktfrage FS-01 und keine Entscheidung im Fix. Der E-001-Text braucht einen Nachtrag „bis zur FS-01-Klärung: Drain-Abbruch → Neuzustellung statt DLQ, weil die Frist verstrichen ist“.
- **Mischbetrieb:** keine Wire-, Lua-, Header- oder Key-Änderung. Ein 0.12.0-Worker neben einem 0.12.1-Worker verhält sich je Prozess wie seine Version. Rolling Deploy in beide Richtungen unkritisch.
- **Neue Fehlermodi:** keine Suspendierung, kein Cancel-Pfad. Einzige Verhaltensänderung: ein eingebetteter Producer im selben Prozess bekommt nach der Frist einen sofortigen, sicher wiederholbaren Fehler statt Indeterminate.

**a2: Untergrenze statt Null (Fehlerpfad-Publishes bekommen die Aufräumreserve).**
- Endfrist z. B. `drain_until + min(1 s, Reserve)`. In genau diesem Fenster wartet `_drain_in_flight` auf die abgebrochenen Handler (`shutdown.py:118`).
- Dann laufen Retry- und DLQ-Kopie des abgebrochenen Handlers wie in E-001/KTD2 gedacht (Kopie + Ack, kein erneuter Lauf).
- Ändert aber die in 0.12 dokumentierte Zusage „Publish-Endfrist = Drain-Ende“ (Changelog rc1 `changelog.md:673-674`, `ShutdownBudget.drain_until`-Docstring, Plan 2026-08-28 R12/KTD9). Sie steht nicht im Register, verbraucht aber Reserve aus E-018.
- Braucht a1 trotzdem für alles nach der verlängerten Frist.
- **Nur sinnvoll, wenn FS-01/E-001 bleibt.** Bei Wechsel auf „Drain-Abbruch = Neuzustellung“ ist a2 überflüssig. Deshalb nicht im P1-Fix.

**a3 (Ergänzung, P2-Welle mit FS-08): Fristbruch vor dem Senden nicht als Indeterminate.**
- Ein Flag `maybe_sent` je Vorgang: `True` unmittelbar vor `default_exchange.publish`, `False` nach einem als „nicht gesendet“ klassifizierten Versuch.
- Reißt die Frist bei `maybe_sent == False` (Warten auf `_ensure_channel_tracked`/`connection.ready()` bis 10 s, Backoff-Sleep zwischen Versuchen), folgt `BrokerConnectionError` statt `PublishIndeterminate`.
- **Nicht** über `sent_before_failure` auf dem `TimeoutError` lösen: Bei eigenem Abbruch ist `drain_future` „cancelled“, der Frame wird aber trotzdem geschrieben (siehe Nachweis a). Die Klassifikation „cancelled → nicht gesendet“ wäre falsch.
- a3 allein löst Teil a nicht (`maybe_sent` ist beim Fast-Path schon `True`).
- Berührt E-013 wie a1. E-041 („mitten im Versuch“) bleibt für `maybe_sent == True` unverändert.

### Optionen Teil b

**b1: Publish-Endfrist = tatsächliches Drain-Ende (empfohlen).**
- In `_execute_shutdown` eine Quelle für das Drain-Ende (Loop-Zeit):
  - ohne Override `started_at + budget.drain_until`
  - mit Override `started_at + override`
  - bei unbegrenztem Recycling-Drain (falls FS-30 das einführt) `None`
- Diesen Wert an `_set_publish_deadline(...)` **und** an `_drain_in_flight(drain_until)` geben. Heute rechnet `_drain_in_flight` den Override relativ zum eigenen Start (nach `cancel_consume`). Die Vereinheitlichung verschiebt das Drain-Ende um die Dauer von `cancel_consume`, das ist vernachlässigbar.
- Optional `_shutdown_end = drain_end + (finish_by − drain_until)`, damit die Aufräumschritte nach einem Recycling nicht auf 1 s fallen. Das ist Verhalten außerhalb des Befunds, gehört eher zu FS-30 und ist dort zu entscheiden.
- **Aufrufer:** Ein recycelnder Leader feuert während des Drains wieder normal, das Sekunden-Refire endet. Workflow-Advances, Retry und DLQ der Langläufer laufen normal.
- **Entscheidungen:**
  - E-018: Der Recycling-Drain ist ausdrücklich ausgenommen, b1 setzt das für die Endfrist konsequent um. Die Tests `test_supervised_drain_ends_before_the_supervisor_kills` (ohne Override) und `test_recycling_drain_is_exempt_from_the_budget` (Override 2.0, Task endet nach 0,06 s) bleiben grün, ebenso `test_shutdown_publish_deadline.py` (ohne Override).
  - E-038 wird für Teil b erfüllt.
- **Mischbetrieb:** Supervisor und Worker stammen aus demselben Paket, kein Mischfall. Keine Wire-Änderung.
- **Neue Fehlermodi:** Ein SIGTERM während des Recycling-Drains verkürzt die Frist weiterhin nicht. Der Supervisor tötet bei `sigkill_at`, und laufende Publishes enden mit dem Prozess (at-least-once über Broker-Redelivery). Das ist Bestand, nicht durch b1 verursacht.

**b2: Keine Endfrist im Recycling (`set_publish_deadline(None)`).**
- Einfacher. Ein Publish ist dann nur durch `publish_timeout_s` (120 s) begrenzt.
- Nach dem Ende des Recycling-Drains (abgebrochene Handler) hängen die Fehlerpfad-Publishes bis zu 120 s bei kaputtem Broker. `_drain_in_flight` wartet zwar nur 1 s, die Tasks laufen danach aber neben dem Aufräumen weiter.
- Der Nachweis b prüft als Voraussetzung `broker._publish_deadline_at is not None` und bräche.
- Nicht empfohlen.

### Schnittstelle zu FS-30 (Recycling-Drainfrist, andere Recherche)

- FS-30 ändert **wie lang** der Recycling-Drain ist (`recycle_drain_timeout`, Rückfall auf `shutdown_timeout` ohne `hard_timeout`). FS-02b ändert, **dass die Publish-Endfrist demselben Drain-Ende folgt**.
- Gemeinsame Naht ist der Wert in `worker._drain_timeout_override` und seine Auswertung in `shutdown.py:103-107`/`:269-272`.
- Vorschlag: FS-02b führt eine einzige Funktion „Drain-Ende ab `started_at`“ ein (Rückgabe `float | None`). FS-30 ändert danach nur `recycle_drain_timeout`, also endlich, `math.inf` oder ein Sentinel.
- **Vertrag für FS-30:** `0`/`None` bedeutet heute „kein Override“ (`if not drain_timeout`). Ein „unbegrenzt“ muss ein eigener Wert sein (`math.inf`), und die Drain-Ende-Funktion bildet ihn auf eine Publish-Endfrist `None` ab.
- Reihenfolge: FS-02b zuerst (P1, 0.12.1), FS-30 danach in der P2-Welle.
- Offen für FS-30, nicht für FS-02: `_shutdown_end` nach langem Recycling-Drain und SIGTERM während des Recycling-Drains.

### Empfehlung

- **a1 + b1** als 0.12.1. a3 in der P2-Welle zusammen mit FS-08 (gleicher Code).
- **Zu ändernde Tests:**
  - `tests/unit/broker/test_befund_fs02_publish_after_deadline.py`: Marker entfernen, sonst unverändert.
  - `tests/unit/worker/test_befund_fs02_recycling_publish_deadline.py` **muss angepasst werden**. Er liest `broker._publish_deadline_at` beim Drain-Start und publiziert 5 ms danach. Mit b1 ist das die Recycling-Frist (2,0 s): Der Task liefe nach dem Drain-Ende, und `wait_for(…, 2.0)` risse.
  - Anpassung: die „normale“ Frist aus `shutdown_budget(0.05, supervised=False).drain_until + started_at` berechnen, nicht aus dem Broker lesen. Die Semantik bleibt „Publish nach der normalen Frist, innerhalb der Recycling-Frist gelingt“. Zulässig, weil es ein befund-Test ist und kein entscheidung-Test.
- **Neue Tests:**
  - (1) `retry_later`/`dead_letter` nach der Frist: Original wird per `redeliver` zurückgestellt, `sent == []`. Auf Worker-Ebene ohne Kopie, Schedfire-Claim vor dem nack freigegeben.
  - (2) Scheduler-Fire nach der Frist: `_release_own_claim` wird aufgerufen, kein Commit.
  - (3) Client `schedule()` nach der Frist: Unique-Rollback, `BrokerConnectionError` und **nicht** `PublishIndeterminate`.
  - (4) b1: Drain-Ende mit Override, die Frist folgt (Unit, skalierte Zeit).
  - (5) b1 + SIGTERM-Pfad unverändert (bestehende E-018-Tests reichen).
  - (6) E-013: Fehler nach der Frist zählen wie `BrokerConnectionError`. Einen Test schreibt fest, was gewollt ist.
- **Dateien:** `broker/rabbitmq/delivery.py`, `broker/rabbitmq/connection.py` (Docstring), `worker/shutdown.py`, ggf. `broker/rabbitmq/redeclare.py`. Doku: `errors(.de).md` (Publish nach Endfrist), `delivery-semantics(.de).md`, Changelog, Register E-001/E-038/E-041/E-042 („offen“ auf „behoben“, E-001-Nachtrag).
- **Aufwand:** S (a1) + S (b1), a3 S. **Risiko:** niedrig. E-001-Folgen-Text und E-013-Zählung sind zu benennen.

---

## FS-08 (P2): Publish in aiormqs Kanal-Sperre hängt nach Abriss bis zur Frist und endet als `PublishIndeterminate`

### Ursache

- `aiormq/channel.py:143-147`: Die Property `lock` prüft `is_closed` nur beim Zugriff.
- `:652` `async with countdown.enter_context(self.lock)`: Der Wartende hält danach die Referenz.
- Abriss: `FutureStore.reject_all` (`aiormq/base.py:47-63`) leert den Store und lehnt `drain_future`/Confirm des Halters ab. Der Halter gibt die Sperre frei.
- Der Wartende legt **neue** Futures an (`:666-680`). Sie stehen im schon geleerten Store, niemand lehnt sie mehr ab.
- Er reiht seinen Frame in die `write_queue` der toten Verbindung (kein Writer) und wartet ewig auf `drain_future` (`:688`).
- swarmq begrenzt nur mit `asyncio.timeout(publish_timeout_s)` (`delivery.py:178`), deshalb nach 120 s `PublishIndeterminate`, obwohl nichts gesendet wurde. Das verletzt E-042.
- Im Worker belegt der hängende Publish einen Semaphor-Platz, im Leader den `_fire_lock`.

### Optionen

**A: Je Versuch Publish gegen `underlay.closing` laufen lassen (empfohlen).**
- In `_publish_attempts` je Versuch:
  1. `underlay = await pub.get_underlay_channel()`. aio-pika: Basis synchron, Robust nur `connected.wait()`, das nicht suspendiert, wenn das Event gesetzt ist (`aio_pika/connection.py:230-231`, `robust_channel.py:81-83`).
  2. **Synchron** prüfen: Ist `closing` kein `asyncio.Future` (Mocks, `FakeTransport`), gilt der heutige Pfad (fail-open wie `blocked.py`). Ist `underlay.is_closed`, gilt der transiente Pfad (verwerfen, nächster Versuch).
  3. `child = create_task(pub.default_exchange.publish(...))`, dann `await asyncio.wait({child, underlay.closing}, FIRST_COMPLETED)`.
  4. Gewinnt `closing`, wird `child` abgebrochen und abgewartet. Klassifikation: `drain_future` erfüllt → gesendet → `PublishIndeterminate` (E-042). Sonst nicht gesendet → verwerfen, nächster Versuch, am Ende `BrokerConnectionError`.
  5. Im `finally` das Kind immer abbrechen und abwarten. Ein Abbruch der eigenen Task bzw. der Frist geht unverändert durch (E-041: Frist mitten im Versuch → Indeterminate).
- **Pflicht-Ergänzung in `publish_phase.sent_before_failure`:** Steht `drain_future` in `frame.f_code.co_varnames`, ist aber nicht gebunden (nicht in `f_locals`), heißt das „nicht gesendet“. Erst nach `self.create_future()` folgt synchron `write_queue.put` (`channel.py:666-686`).
  - Ohne diese Ergänzung würde ein Kind, das beim Abbruch noch **in der Sperren-Warteschlange** steht, als gesendet gewertet. Die Reihenfolge der Wakeups nach `reject_all` ist nicht deterministisch (Set-Pop-Reihenfolge).
  - In Python 3.14.4 gegengeprüft: ungebundener Local → `'drain_future' in f_locals == False`, `in co_varnames == True`.
  - `test_publish_phase.py::test_an_unreadable_drain_future_counts_as_sent` (kein `drain_future` in `co_varnames`) bleibt `True`.
- **Warum die Klassifikation hier stimmt (anders als bei FS-02):** Abgebrochen wird nur, wenn **genau der** Underlay geschlossen ist, auf dem das Kind publiziert.
  - Bei Verbindungsabriss ist der Writer tot.
  - Bei reinem Channel-Close durch den Broker (403/406) schreibt der Writer zwar weiter. Der Broker verwirft Frames auf einem geschlossenen Kanal aber (bzw. schließt die Verbindung mit CHANNEL_ERROR, Bestand in aiormq). Zugestellt wird nichts.
- **Identität des Underlays:** Durch die synchrone Prüfung vor `create_task` und den suspendierungsfreien Fast-Path in `Exchange.publish` → `get_underlay_channel` → `basic_publish` erhält das Kind denselben Underlay. Ein Wechsel auf einen neuen Underlay binnen eines Loop-Durchlaufs ist praktisch ausgeschlossen. Für den Fall, dass `connected` gerade nicht gesetzt ist, wartet das Kind in `ready()` ohne `basic_publish`-Rahmen, und das ergibt korrekt „nicht gesendet“.
- `aiormq…closing` ist ein öffentliches Attribut (`base.py:83-96`), aio-pika nutzt es selbst (`aio_pika/abc.py:523-542`).
- **Aufrufer:** alle Publishes über `_publish_with_retry`: Client, Retry/DLQ, Workflow, Scheduler, `dlq retry` (`admin.py`).
- **Entscheidungen:**
  - E-042 wird erfüllt. Die E-042- und E-041-Tests nutzen `FakeTransport` ohne `closing`-Future und laufen deshalb im heutigen Pfad: unverändert grün.
  - E-013: Statt Indeterminate (nicht gezählt) kommt nach einem Abriss Erfolg auf neuem Kanal, oder nach 5 Versuchen `BrokerConnectionError` (gezählt). Genau so beschreibt E-013 Verbindungsfehler, also konsistenter als heute.
  - E-025: unberührt (Warten vor den Versuchen).
- **Mischbetrieb:** keiner (prozesslokal).
- **Wechselwirkung:** gleiche Funktion wie FS-02 (a1 vor dem Deckel, A in der Versuchsschleife) und a3 (`maybe_sent`). Reihenfolge: FS-02 in 0.12.1, FS-08 + a3 gemeinsam in der P2-Welle.
- **Neue Fehlermodi:**
  - eine Task je Versuch (Kosten im µs-Bereich; bei der Loadsuite etwa 510/s unkritisch, im Loadsuite-Lauf messen)
  - `asyncio.wait` entfernt seine Callbacks vom langlebigen `closing`-Future selbst, kein Leck
  - contextvars werden ins Kind kopiert (structlog-Bindungen bleiben)
  - `current_task().cancelling()`-Prüfungen in `_publish_attempts` beziehen sich auf die Eltern-Task, ein eigener Abbruch des Kinds muss vor dem generischen `CancelledError`-Zweig behandelt werden
  - ein Publish, den der Aufrufer abbricht, kann wie heute noch geschrieben werden

**B: aiormq upstream fixen (nach dem Erwerb der Sperre `is_closed` prüfen bzw. Futures an den Closing-Zustand koppeln).**
- Richtig an der Wurzel. Zeitpunkt ungewiss.
- swarmq pinnt aiormq gar nicht (`pyproject.toml:33` nur `aio-pika>=9`), deshalb wäre zusätzlich eine Untergrenze nötig.
- Als Begleitmaßnahme melden. Allein nicht ausreichend für 0.13.

**C: Eigene Sperre um den ganzen Publish.**
- Verworfen. Sie serialisierte auch das Confirm-Warten (aiormq gibt die Sperre vor `await confirmation` frei, `channel.py:690-695`), der Durchsatz bräche ein.

### Empfehlung

- A plus `sent_before_failure`-Ergänzung, Upstream-Meldung als B.
- **Tests:**
  - Nachweis `test_befund_fs08_publish_lock_waiter.py`: Marker entfernen.
  - Neu:
    - (1) Kind wird in der Sperren-Warteschlange abgebrochen → nicht gesendet, beide Wakeup-Reihenfolgen. Deterministisch über den echten aiormq-Channel wie im Nachweis.
    - (2) `drain_future` erfüllt, dann Close → `PublishIndeterminate`, kein erneutes Senden (E-042).
    - (3) Channel-Close durch den Broker (nicht Verbindung) während des Wartens → neuer Kanal.
    - (4) Frist reißt während `asyncio.wait` → Kind abgebrochen und abgeholt, kein „Task was destroyed but it is pending“/„never retrieved“.
    - (5) eigener Abbruch geht durch.
    - (6) `sent_before_failure` mit ungebundenem `drain_future` → `False`.
  - Optional Toxiproxy-Integration (Abriss bei laufendem Publish-Strom), ohne Timing-Glück nicht deterministisch, deshalb nur als Smoke.
- **Dateien:** `broker/rabbitmq/delivery.py`, `broker/rabbitmq/publish_phase.py`, `tests/support/aiormq_transport.py` (ggf. `closing` am FakeTransport für neue Tests).
- **Aufwand:** M. **Risiko:** mittel (Hot-Path, Cancel-Semantik).

---

## FS-09 (P3): Consumer-Wächter bricht `basic.cancel`/`basic.consume` nach 2 s ab, aiormq schließt den Consume-Channel

### Ursache

- `consumer_watch.py:244` (`wait_for(queue.cancel(tag), 2 s)`) und `:264` (`wait_for(queue.consume(cb), 2 s)`).
- `aiormq/channel.py:157-213`: `rpc` läuft als `@task` in eigener Task. Der Abbruch der wartenden Task bricht die innere ab (TaskWrapper `abc.py:28-58`). `rpc` schickt bei `CancelledError`/`TimeoutError` selbst `Channel.Close(504)` und setzt `__close_event`.
- Ein gemeinsamer Consume-Kanal je Worker (BZ-20/E-019), deshalb kommen alle unbestätigten Zustellungen zurück, während sie hier noch laufen.
- Plan 2026-10-08-1541 `:182` begrenzt nur `get_underlay_channel`. Der Kommentar `consumer_watch.py:46-49` hält einen Abbruch mitten im RPC selbst für unerwünscht.
- **Nachbarpfad mit derselben Wirkung:** `stop()` (`:115-131`) bricht einen noch laufenden Takt nach `_STOP_TIMEOUT_S = 5 s` ab. Das trifft ein laufendes RPC, und der Consume-Kanal schließt zu Beginn des Shutdown-Drains (`cancel_consume`). Die laufenden Handler werden dann an andere Worker zurückgegeben.
- `cancel_all` (`:140-152`) ruft `queue.cancel` ohne Frist. Hält ein hängendes RPC die aiormq-Kanalsperre (`rpc` hält `lock` bis zur Antwort), wartet es dahinter. Acks laufen ohne Sperre (`channel.py:537-556`), Settles hängen also nicht.

### Optionen

**O1: RPC nicht abbrechen, nur das Warten des Takts begrenzen.**
- RPC als eigene Task; der Takt wartet `asyncio.wait({rpc}, timeout)`.
- Bei Überschreitung WARNING `consumer_watch_rpc_slow`, Eintrag merkt sich `pending_rpc`. Der nächste Takt überspringt bzw. wertet aus (Cancel ok → `_mark_cancelled` + consume).
- `stop()` bricht laufende RPC-Tasks nie ab.
- Vorteil: Der Takt läuft weiter.
- Da aber alle Consumer auf **einem** Kanal liegen und `rpc` die Kanalsperre bis zur Antwort hält, blockiert ein hängendes RPC jedes weitere ohnehin. Parallelität bringt hier nichts, nur Zustandsautomat und Komplexität.

**O2: Großzügige, aiormq-eigene Frist (`queue.cancel(tag, timeout=…)`, `consume(…, timeout=…)`, z. B. 30–60 s) statt `wait_for`.**
- Schließt bei Ablauf ebenfalls (`rpc` sendet Close(504)), aber erst, wenn der Broker sicher hängt.
- Ein so lange stummer Broker hat den Kanal praktisch verloren.

**O3: Kein Zeitlimit am RPC, nur an `get_underlay_channel`, wie im Plan. Plus O2 als Notbremse (empfohlen: O3 + O2).**
- Der Takt wartet auf die Antwort, ein Abriss lehnt das RPC ab (Fehler → nächster Takt).
- Dazu `stop()`: nach `_STOP_TIMEOUT_S` den Takt nicht abbrechen, sondern abkoppeln (Referenz halten; `broker.close()` schließt den Kanal und beendet ihn).
- Dann wartet `cancel_all` hinter der Kanalsperre auf das hängende RPC. Den Shutdown begrenzt das nicht. Bestand ist: `cancel_consume` hat heute keine Frist (`shutdown.py:89-92`).
- Deshalb in `_drain_in_flight` `cancel_consume` mit `_bounded_step` begrenzen, aber ohne das RPC abzubrechen (`asyncio.shield`). Sonst entsteht beim Shutdown genau der Kanalverlust.
- Folge: Ein Consumer, dessen Cancel nicht durchkommt, bekommt während des Drains evtl. noch Zustellungen. Sie bleiben unbestätigt und kehren beim Verbindungsschluss zurück: at-least-once, Ausführungsschutz.

**Aufrufer/Nachbarn:**
- `_may_settle`/`wait_cancelled` (`delivery.py:392-434`) wartet `settle_cancel_wait_s = 10 s` auf den eigenen Cancel. Ein langsames RPC (> 10 s) führt dazu, dass das Settle unterbleibt (Broker stellt neu zu). Das ist der vorhandene Rückfall, keine neue Gefahr. Kommentar `:95-97` („je 2 s“) anpassen.
- `/ready` (`gaps()`) misst ab `missing_since` und ist unverändert.
- `consumer_watch_channel_timeout` (Wartezeit auf den Kanal) bleibt bei 2 s.

**Entscheidungen:** Kein E-Eintrag regelt den Wächter-RPC-Abbruch. E-019 (gemeinsamer Kanal, Prefetch je Consumer) ist Voraussetzung, nicht berührt. E-046/FS-03: Weniger selbst verursachte Kanalverluste heißen weniger Neuzustellungen laufender Schedfires (die `sched_fire_claim.lua`-Duplikatregel greift seltener). Die Wechselwirkung ist positiv.

**Mischbetrieb:** keiner.

**Neue Fehlermodi:** Ein hängender Takt meldet keine weiteren Verluste. Die anderen Consumer liegen auf demselben, gesperrten Kanal und wären ohnehin blockiert. Kein Abbruch mehr in `stop()` heißt, dass eine Task über `stop()` hinaus läuft. Sie muss beim `close()` enden; zu prüfen mit einem Test ohne „Task was destroyed but it is pending“.

### Empfehlung

- O3 + O2-Notbremse (Konstante etwa 60 s), `stop()` koppelt ab statt abzubrechen, `cancel_consume` im Shutdown begrenzt und geschützt (shield).
- **Tests ändern (zulässig, keine entscheidung-Tests):** `tests/unit/broker/test_consumer_watch.py::TestHangingRpc::test_hanging_own_cancel_gives_up_after_timeout` und `::test_hanging_reregistration_gives_up_after_timeout`. Sie schreiben das Aufgeben nach 0,01 s mit `TimeoutError` fest. Neu formulieren: Der Takt wartet weiter bzw. endet erst an der Notbremse, kein Close.
- **Achtung Nachweis:** `Fs09Queue.cancel(tag)`/`consume(callback)` im Nachweis nehmen **kein** `timeout=`. Übergibt der Fix `timeout=`, endet der Aufruf mit `TypeError`, wird geloggt, es gibt kein Close, und der Test wird grün, **ohne** den Fix zu prüfen. Fake-Signatur beim Marker-Entfernen um `timeout: float | None = None` ergänzen.
- **Neue Tests:**
  - (1) `stop()` während laufendem RPC sendet kein `Channel.Close`.
  - (2) Ein langsames Cancel, das nach 3 s antwortet, führt zu `_mark_cancelled` und Neuanmeldung.
  - (3) Die Notbremse sendet Close erst nach der großen Frist (über Konstante skaliert).
  - (4) `cancel_consume` im Shutdown ist begrenzt, ohne das RPC abzubrechen.
- **Dateien:** `broker/rabbitmq/consumer_watch.py`, `broker/rabbitmq/delivery.py` (Kommentar/`settle_cancel_wait_s`), `worker/shutdown.py` (`cancel_consume` begrenzen).
- **Aufwand:** S–M. **Risiko:** niedrig-mittel (Shutdown-Reihenfolge).

---

## FS-10 (P2): DLQ-Kopie trägt `x-delivery-count`/`x-acquired-count`

### Ursache

- `worker/retry.py:306` (`headers = {**dict(delivery.headers)}`), `gates/reject.py:33` und `gates/expiry.py:119` kopieren alle Header.
- `RabbitMQDelivery.dead_letter` (`delivery.py:474-485`) → `identity.dead_letter_generation` (`identity.py:141-153`) → `copy_headers` (`:83-93`) entfernt nur `x-death`/Base. `_publish_to_dead_letter_queue` (`delivery.py:143`) entfernt nur `LOOP_STATE_HEADERS`.
- Die Gegenstücke filtern bereits: `republish_headers` (`gates/__init__.py:170-191`, `BROKER_DELIVERY_COUNTERS`) und `admin.py:78/329` (`dlq list`/`retry`).
- RabbitMQ 4.3.6 liefert einen vom Publisher gesetzten Zähler bei der ersten Zustellung unverändert aus (Integrationsnachweis). Das DeliveryCountGate (`gates/delivery_count.py:84-97`) dead-lettert das Replay vor der Ausführung.

### Optionen

**A: Zähler zentral aus jeder von swarmq publizierten Kopie entfernen (empfohlen).**
- Das geht in `identity.copy_headers` (Docstring: „Header einer Kopie, die swarmq selbst publiziert (Retry, Zurückstellung, DLQ-Stempel, dlq retry)“).
- `BROKER_DELIVERY_COUNTERS` nach `swarmq/headers.py` verschieben, damit es keinen Zyklus identity ↔ gates gibt. Duplikat `admin.py:78` darauf umstellen.
- Wirkung je Aufrufer:
  - `republish_headers`: filtert schon, unverändert.
  - `admin.py:396` (`dlq retry` forward): filtert schon über `:329`, unverändert.
  - `dead_letter_generation`: neu ohne Zähler. Deckt alle vier Worker-Pfade ab (Gate, Retry erschöpft, Expiry, Ablehnung), weil alle über `broker.dead_letter` laufen.
- **Gleichwertige Alternative:** Filter in `_publish_to_dead_letter_queue` neben `LOOP_STATE_HEADERS`. Das ist enger, lässt aber die Invariante dezentral.
- **Identität:** `execution_identity` liest die Original-Header (`dead_letter_generation` übergibt `headers`, nicht die Kopie). `x-swarmq-replay-id`/Generation bleiben bitgleich, E-009 bleibt unberührt (Tests `test_identity_sequences.py`, `test_befund_bz08_replay_without_id.py`).
- **Diagnose:** Der Zählerstand steht beim Gate-Pfad schon im Fehlertext („delivered 19 times …“ → `x-swarmq-traceback`). Kein neuer Header nötig. Optional ein eigener Name (`x-swarmq-dlq-delivery-count`), aber nicht empfohlen (zusätzliches Header-Format).
- **Entscheidungen:**
  - E-009 unberührt.
  - E-043 (`dead_letter_copy`, `SettleFailedAfterPublish`) unberührt.
  - Der Contract-Test `test_dead_letter_is_inspectable_with_all_original_headers` (KTD9) prüft nur swarmq-Header und bleibt grün.
  - Kein E-Eintrag regelt die Zähler.
- **Mischbetrieb:**
  - 0.12.1-Kopien ohne Zähler, 0.12.0-Kopien mit. Ein 0.12.0-Worker, der ein 0.12.1-Replay konsumiert, sieht keine Zähler (korrekt).
  - **Bestand:** DLQ-Nachrichten, die 0.12.0-Worker schon in die XQ gelegt haben, bleiben per UI/Shovel nicht replaybar. Für sie gilt weiter: `swarmq dlq retry` nutzen. Das gehört in Changelog und Upgrade-Hinweis.
  - Dasselbe gilt für Kopien über den Broker-DLX-Rückfall (`route_via_dlx`). swarmq kann sie nicht filtern. Ob RabbitMQ beim DLX-Dead-Lettering den Zähler mitgibt, ist **nicht** belegt; per Integrationsvariante prüfen.
- **Neue Fehlermodi:** keine. `broker_redelivered()` ist für ein Replay jetzt korrekt `False` (Lock-Wartezeit wird nicht mehr fälschlich zurückgesetzt).

**B: Gate-seitige Toleranz: Zähler nur werten, wenn die AMQP-Zustellung `redelivered` ist.**
- Repariert auch den Bestand und DLX-Kopien.
- Braucht aber `redelivered` am `Delivery`-Vertrag (Drittbroker) oder einen RabbitMQ-spezifischen Zugriff im broker-agnostischen Gate.
- Unbelegt ist, ob RabbitMQ den Publisher-Wert bei der zweiten Zustellung überschreibt oder hochzählt (der Spike `fs10spike/spike_fs10_counter.py` gibt das aus, das Ergebnis ist nicht festgehalten). Davon hängt ab, ob nach einem Replay der Hängeschutz noch greift.
- Berührt BZ-29-Tests (`test_delivery_count.py`, SimpleNamespace ohne `redelivered`).
- Als Folgearbeit, falls der Bestand relevant ist. Nicht für die P2-Welle.

### Empfehlung

- A (zentral in `copy_headers`), Doku/Changelog mit Bestandshinweis.
- **Tests:**
  - Unit-Nachweis `test_befund_fs10_dlq_copy_counters.py` (4 Fälle): Marker entfernen.
  - Integrationsnachweis `test_befund_fs10_dlq_replay_counters.py` (maßgeblich, über `itest.sh`): Marker entfernen.
  - Neu:
    - (1) `copy_headers` entfernt die Zähler (Unit).
    - (2) Generations-ID bzw. Replay-ID mit und ohne Zähler im Original identisch (Mischbetrieb-Garantie).
    - (3) Mischbetrieb-Suite `tests/compat`: Replay einer 0.12.1-DLQ-Kopie an einen 0.12.0-Worker läuft.
    - (4) Integrationsvariante: `route_via_dlx` + UI-Move, um das Broker-Verhalten festzuhalten (Spike/xfail mit Begründung, falls RabbitMQ den Zähler trägt).
- **Dateien:** `identity.py`, `headers.py`, `worker/gates/__init__.py` (Import), `broker/rabbitmq/admin.py`, Doku `delivery-semantics(.de).md:172-175` (Liste um DLQ-Kopie ergänzen), Changelog.
- **Aufwand:** S. **Risiko:** niedrig.

---

## Wechselwirkungen zwischen den vier Befunden

- FS-02 a1/a3 und FS-08 ändern dieselbe Funktion (`delivery.py:156-313`). Reihenfolge: a1 + b1 in 0.12.1. FS-08 + a3 + `publish_phase`-Ergänzung gemeinsam in der P2-Welle, mit einem gemeinsamen Satz Phasen-Tests.
- FS-09 und FS-02: Ein Abbruch des Wächter-Takts im Shutdown (`stop()`) schließt den Consume-Kanal vor dem Drain. Die Neuzustellungen laufen dann parallel zu Handlern, deren Retry-/DLQ-Publishes nach FS-02 a1 sauber „nicht gesendet“ melden. Beide Fixes verringern unabhängig voneinander Doppelläufe.
- FS-10 und FS-02: unabhängig. Die DLQ-Kopie ohne Zähler ändert die Generation nicht.

## Befunde, deren Fix eine Entscheidung oder offene Produktfrage berührt

- **FS-02 Teil a → E-001 (offene Produktfrage FS-01):** Mit a1 endet ein Drain-Abbruch bei SIGTERM als Fehlerergebnis + Workflow-FAILED + **Neuzustellung**, nicht wie im E-001-Folgen-Text als DLQ-Eintrag bzw. Retry-Verbrauch. Die E-001-Tests bleiben grün. Der Text braucht einen Nachtrag; die Klärung bleibt FS-01 überlassen. a2 (Fehlerpfad-Publishes in der Reserve) wäre nur unter „E-001 bleibt“ sinnvoll.
- **FS-02 Teil a (und FS-08) → E-013:** „Nicht gesendet“ wird `BrokerConnectionError` und zählt für den Leader-Rücktritt, `PublishIndeterminate` nicht. Im Shutdown-Fenster praktisch folgenlos, aber in E-013 festzuhalten (oder eigene, nicht gezählte Unterklasse, das wäre ein E-013-Nachtrag).
- **FS-02 Teil b → E-018/E-038 + FS-30:** b1 setzt „Recycling ausgenommen“ für die Endfrist um. Offen und im Zuständigkeitsbereich von FS-30: unbegrenzter Drain als eigener Wert, `_shutdown_end` nach langem Recycling, SIGTERM während des Recycling-Drains (heute ignoriert).
- **FS-10 → keine Entscheidung,** aber eine Grenze: 0.12.0-DLQ-Bestand und Broker-DLX-Kopien bleiben per UI/Shovel nicht replaybar (Doku). Option B wäre eine Produktentscheidung (Vertragsfeld `redelivered`).
- FS-08, FS-09: keine Entscheidung berührt (E-042 wird erfüllt, Wächter-RPC-Abbruch war nie entschieden).
