# Locking und Deduplizierung

Du willst verhindern, dass dieselbe logische Arbeit parallel läuft
(Locking), oder dass derselbe Aufruf zweimal eingereiht wird
(Deduplizierung). SwarmQ bietet beides, jeweils an einer eigenen
Stelle.

## Was brauchst du?

- **Lock** regelt die **Ausführung**. Zwei Zustellungen, deren Lock
  denselben Namen ergibt, führen `run()` nie gleichzeitig aus (beim
  Semaphor: nie mehr als `max_concurrency` davon).
- **Unique Task** regelt das **Einplanen**. Zwei `schedule()`-Aufrufe
  mit denselben Argumenten ergeben einen Task. Der zweite Aufruf gibt
  still die Task-ID des ersten zurück.

Beides lässt sich kombinieren, ein Task kann beides deklarieren. Ein
häufiges Muster ist `unique_for` gegen Client-Retries und `lock`, um
die eigentliche Ausführung zu serialisieren.

## Locking

### Einen Lock deklarieren

```python
class SyncUser(Task):
    task_info = TaskInfo(
        name="sync_user",
        queues=["users"],
        lock="user:{user_id}",
        hard_timeout=60,
    )

    async def run(self, user_id: int) -> None:
        ...
```

Der Lock-Name kann ein Format-String sein, der Parameter von `run()`
nennt, übergeben per Keyword oder per Position. Der Worker rendert ihn
gegen die Argumente der Nachricht: `schedule("sync_user", user_id=42)`
sperrt `user:42`. Der Lock liegt in Valkey unter
`[<namensraum>:]sq:lock:<name>`.

`hard_timeout` ist mit `lock` **Pflicht**; ohne wirft `TaskInfo` beim
Import `ValueError`. Die TTL des Locks ist `hard_timeout + 30 s`. Sie
ist die Absturzfrist: Stirbt ein Worker hart, während er den Lock hält,
wartet der nächste Halter höchstens so lange.

### Mutex vs. Semaphor

Mit dem Standard `max_concurrency=1` ist der Lock ein Mutex. Setze den
Wert höher, um mehrere Ausführungen je Lock-Name zuzulassen:

```python
task_info = TaskInfo(
    name="pdf_render",
    queues=["pdf"],
    lock="pdf",          # ein Name für alle Renderings
    max_concurrency=4,   # bis zu 4 gleichzeitig
    hard_timeout=300,
)
```

`max_concurrency=4` lässt vier Renderings gleichzeitig laufen, über die
ganze Flotte hinweg, unabhängig von `--concurrency` oder `--processes`.
`max_concurrency` braucht `lock`; ohne wirft `TaskInfo` `ValueError`.

### Lock-Acquisition-Verhalten

Nimmt der Worker eine Nachricht auf, deren Lock gehalten wird:

- Die Nachricht wird **verzögert neu eingereiht** (100 ms, je Konflikt
  verdoppelt, höchstens 60 s). Sie blockiert den Worker nicht und lässt
  den Task nicht scheitern.
- Der Worker feuert bei jedem Konflikt `TASK_LOCKED`
  (`lock_conflict_count` zählt die Konflikte dieser Nachricht) und loggt
  `task_lock_conflict` (INFO, mit `lock_name`, `lock_conflict_count` und
  `waited_ms`) beim ersten Konflikt und danach etwa einmal je Minute.
- Die Nachricht wartet, solange der Lock den Halter wechselt oder sein
  Halter ihn am Leben hält. In die DLQ geht sie mit `LockWaitExceeded`
  (Fehlerergebnis, `TASK_DLQ`, ein Workflow-Schritt lässt seinen
  Workflow scheitern) erst, wenn
    - derselbe Halter den Lock länger hält als die beim letzten
      Halterwechsel gesehene TTL des Keys plus 60 s (ein hängender
      Halter, dessen Lock am Leben gehalten wird), oder
    - seit dem ersten Konflikt `SWARMQ_LOCK_WAIT_MAX_S` (Standard 6 h)
      vergangen ist.
- `max_age` bleibt die harte Grenze: Ein Task mit `max_age` läuft in
  die DLQ, sobald er älter ist, egal wie lange er gewartet hat.

Gemessen wird mit den Wanduhren der Worker
(`x-swarmq-lock-wait-since`, `x-swarmq-lock-holder-since`). Halte die
Uhren synchron; ein Versatz bis etwa 60 s wird toleriert. Ein
Lock-Store, der seine Halter nicht melden kann (ein eigener Store, dem
`observe` fehlt oder dessen `observe` keine `LockObservation` liefert),
loggt einmal `lock_observe_unsupported` und wartet bis
`SWARMQ_LOCK_WAIT_MAX_S`. Die Gauges `swarmq_lock_waits` und
`swarmq_lock_wait_oldest_seconds` zeigen je Worker, wie viele
Nachrichten er zuletzt zurückgestellt hat und wie lange die älteste
schon wartet. Eine Nachricht, die er 3 Minuten nicht wieder sieht,
fällt heraus; bei mehr als etwa 2 Minuten Rückstand in der Queue zählen
die Gauges also zu wenig.

Eine neue Wartezeit beginnt, wenn der Broker die Nachricht neu zustellt
(der Worker starb, nachdem er den Lock erworben hatte, oder gab die
Nachricht beim Herunterfahren zurück), und auf der Kopie einer späteren
Ausführung (Retry, `swarmq dlq retry`; der Header
`x-swarmq-lock-wait-gen` hält Retry-Zähler und Replay-ID der wartenden
Ausführung fest). Eine alte Wartezeit beendet eine neu zugestellte
Nachricht also nie sofort. Klassische Queues führen keinen
Zustellzähler; dort setzt eine Neuzustellung die alte Wartezeit fort.

Ist Valkey beim Acquire nicht erreichbar (oder antwortet nicht innerhalb
von `SWARMQ_BACKEND_OP_TIMEOUT`), ist das kein Konflikt. Die Nachricht
wird mit eigenem Backoff über die Delay-Queue zurückgestellt, ohne
`TASK_LOCKED` und ohne `lock_conflict_count` zu erhöhen, und der Worker
loggt `task_deferred_backend_unavailable` (`gate="lock"`, `lock_name`).
Weil der Acquire in Valkey trotzdem gelaufen sein kann, gibt der Worker
den Lock im Hintergrund frei, geprüft gegen sein eigenes Owner-Token.
Jeder andere Fehler des Acquire, auch eine Fehlerantwort von Valkey,
loggt `lock_acquire_failed` (ERROR) und reiht die Nachricht an derselben
Stelle neu ein.

Setze `max_age` bei Tasks, deren Arbeit veraltet, damit eine Nachricht
nicht stundenlang hinter einem langen Halter wartet.

### Einen hängenden Lock lösen

Ein Lock, den ein hängender Halter am Leben hält oder der seinen Halter
überlebt hat, beendet die Wartezeit der eingereihten Nachrichten mit
`LockWaitExceeded`. Die DLQ-Kopie trägt den Lock-Namen in
`x-swarmq-lock-name`, und der Fehlertext nennt den zu löschenden Key.
Entferne den Lock nur, wenn sicher ist, dass sein Halter nicht mehr
läuft:

```bash
valkey-cli DEL sq:lock:user:42   # mit Namensraum: <namensraum>:sq:lock:user:42
```

Aus Python leistet `await app.backend.locks.force_release("user:42",
max_concurrency=1)` dasselbe mit einem `DEL`; es liefert `True`, wenn ein
Lock existierte, und `False`, wenn keiner da war. Spiele die
DLQ-Nachrichten danach mit `swarmq dlq retry` wieder ein.

### Locks während der Ausführung

Der Worker hält einen Lock, den er besitzt, so lange am Leben, wie der
Handler läuft. Er beginnt ein Fenster vor der ursprünglichen Frist
`t0 + TTL` mit dem Verlängern (TTL = `hard_timeout + 30 s`; das Fenster
sind drei Takte, ein Takt ist `min(max(TTL / 3, 50 ms), 20 s)`). Davor
passiert nichts. Ab einem `hard_timeout` von 30 s ist das Fenster 60 s
lang, ein Task, der innerhalb von `hard_timeout - 30 s` fertig wird,
kostet also keinen zusätzlichen Roundtrip. Bei kürzerem `hard_timeout`
deckt das Fenster die ganze TTL: Ein Handler, der länger als etwa ein
Drittel seiner TTL läuft, wird ab dem ersten Takt verlängert, ein
Roundtrip je Takt. Jede Verlängerung gibt ein Fenster dazu, für Mutex
und Semaphor-Slot gleich. Ein hart beendeter Halter (SIGKILL, OOM)
hinterlässt den Lock deshalb bis `t0 + TTL`, oder höchstens ein Fenster
länger, wenn er beim Überziehen beendet wurde.

Ein Handler, der seinen Abbruch ignoriert (Task-Code, der
`CancelledError` schluckt), überlebt `hard_timeout + 30 s` und behält
seinen Lock; der Worker loggt einmal `task_lock_held_past_hard_timeout`
(WARNING). Ein blockierter Event-Loop behält den Lock nicht: Er
blockiert auch die Verlängerung, der Lock kann ablaufen, während der
Handler läuft, und der Worker meldet `task_lock_lost`, sobald der Loop
wieder läuft. Blockierender Code gehört in den
[CPU-Modus](cpu-mode.md), dort tötet `hard_timeout` den Kindprozess.

Findet die Verlängerung den Lock weg oder übernommen (verdrängt durch
`maxmemory-policy`, siehe
[Valkey-Anforderungen](../reference/configuration.md#valkey-anforderungen),
Valkey-Datenverlust oder ein Valkey-Ausfall von etwa zwei Takten,
sobald die Verlängerungen begonnen haben, bei einem Takt von 20 s also
etwa 40 s), loggt der Worker `task_lock_lost` (WARNING,
`stage="running"`): Ein zweiter Halter kann jetzt parallel laufen.
Dieselbe Warnung mit `stage="release"` kommt, wenn der Lock bei der
Freigabe schon weg ist. Eine gescheiterte Verlängerung loggt
`lock_extend_failed` und wird im nächsten Takt wiederholt; gelang vor
der Frist des Locks keine Verlängerung, loggt der Worker
`task_lock_lost` mit `stage="expired"`, ohne Valkey zu fragen, und
verlängert ihn nicht weiter.

Beim Herunterfahren gibt der Worker die Locks von Handlern frei, die
nach dem Drain noch laufen. Ein Handler, der seinen Abbruch ignoriert
und weiterläuft, verliert seinen Lock dann.

Grenze: Ein Erwerber mit kurzer TTL räumt Semaphor-Slots ab, die älter
als seine eigene TTL sind; gemischte `hard_timeout`-Werte unter einem
Semaphor-Namen werden daher nicht unterstützt.

### Lock-Render-Fehler

Nennt das Template einen Parameter, den die Nachricht nicht trägt (ein
Tippfehler, eine alte Nachricht eines umbenannten Tasks), scheitert das
Rendern, und die Nachricht geht mit dem Header
`x-swarmq-lock-render-error` in die DLQ: Fehlerergebnis, `TASK_DLQ`, ein
Workflow-Schritt lässt seinen Workflow scheitern, und der Worker loggt
`lock_name_render_failed` (ERROR). Neu eingereiht wird sie nicht. Ein
kaputtes Lock-Template ist ein Bug, und den willst du laut.

Dasselbe gilt für jeden anderen Render-Fehler (`"{items[0]}"` mit leerer
Liste, `"{user.id}"` auf einem Dict), für ein Template, das einen leeren
oder nur aus Leerzeichen bestehenden Namen ergibt
(`lock_name_rendered_empty`), und für einen gerenderten Namen, der mit
`{sched}` beginnt: Der ist für interne Locks von swarmq reserviert, etwa
den Scheduler-Leader. Beginne Lock-Templates mit einem Literal
(`"order:{order_id}"`, nicht `"{order_id}"`); dann kann kein Wert aus
den Nutzdaten einen reservierten oder leeren Namen ergeben.

Ein wörtliches `lock` aus `""` oder Leerzeichen würde unter dem bloßen
Key `sq:lock:` mit jedem anderen solchen Task kollidieren; `TaskInfo`
lehnt es beim Import ab.

## Deduplizierung (Unique Tasks)

### Dedup deklarieren

```python
class SendWelcomeEmail(Task):
    task_info = TaskInfo(
        name="send_welcome",
        queues=["default"],
        unique_for=3600,            # Dedup innerhalb der Stunde
        unique_until="completion",  # Flag beim Endzustand freigeben
    )

    async def run(self, user_id: int) -> None:
        ...
```

`unique_for=3600` bedeutet: 3600 Sekunden lang nach dem ersten Aufruf
gibt ein zweiter Aufruf mit denselben `(name, args, kwargs)` die Task-ID
des ersten zurück, ohne etwas einzureihen. Der Producer nimmt dazu vor
dem Publish ein Flag in Valkey (`[<namensraum>:]sq:unique:<hash>`).

`unique_for` muss größer als 0 und höchstens `315360000` (zehn Jahre)
sein; ein `float` wird auf ganze Sekunden aufgerundet. `0`, negative
Werte, `nan` und `inf` werfen `ValueError`; abschalten geht mit `None`.
`unique_until` ohne `unique_for` hat keine Wirkung.

Mit `delay=` oder `eta=` nimmt ein Producer, der `unique_for` auflöst
(aus dem Aufruf oder seiner Task-Registry), das Flag schon beim Aufruf,
für `unique_for` plus die Verzögerung: Ein gleicher Aufruf ist vom
Aufruf an bis `unique_for` nach dem Termin ein Duplikat (siehe
[sofortige vs. verzögerte Ausführung](scheduling-and-retries.md#immediate-vs-deferred)).

### Wann das Flag freigeben

`unique_until` wählt den Freigabepunkt:

- `"completion"` (Standard): Das Flag lebt, bis der Task einen
  Endzustand erreicht: Erfolg, endgültiges Scheitern (DLQ oder, bei
  abgeschaltetem Dead-Lettering, ein gespeichertes Fehlerergebnis),
  Cancel oder Ablauf über `max_age`. Das blockiert erneute Aufrufe für
  die ganze Ausführung, Retries eingeschlossen.
- `"start"`: Das Flag wird einmal freigegeben, unmittelbar bevor der
  Task startet, nach der Rate-Limit- und der Lock-Prüfung. Wird die
  Nachricht vorher zurückgestellt (Rate-Limit, Lock-Wartezeit), bleibt
  das Flag gesetzt. Endet die Nachricht vor dem Start (Cancel, Ablauf
  über `max_age`, Ende einer Lock-Wartezeit, Delivery-Limit, Task nicht
  registriert), wird das Flag ebenfalls freigegeben. Ein
  Folge-`schedule()`, während der Task läuft, reiht einen neuen ein, und
  während des Backoffs eines Retry gibt es keine Deduplizierung.

Eine Rückgabe an den Broker, die an verlorenem Kanal oder verlorener
Verbindung scheitert, gilt als erfolgt: Der Broker stellt die
unbestätigte Nachricht ohnehin neu zu, und das Flag bleibt in beiden
Modi gehalten.

Wähle `"completion"` für Arbeit, die genau einmal passieren soll
(„Willkommens-E-Mail an User 42 senden“), und `"start"` für Arbeit, bei
der sich Läufe überlappen dürfen, doppelte wartende Nachrichten aber
nicht.

### Unique-Crons

Bei einem statischen Cron verhindert `unique_for`, dass ein langer Lauf
den nächsten Termin überlappt: Der Scheduler-Leader nimmt das Flag,
bevor er einen Termin veröffentlicht, und ein Termin, dessen Flag noch
gehalten wird, wird übersprungen (`TASK_DUPLICATE_DETECTED`,
`source="scheduler.fire"`) und gilt als erledigt. Das gilt bei jedem
Leader, auch bei einem, der den Task nicht in seiner Registry hat:
Worker melden `unique_for` und `unique_until` mit ihren Crons
([Cron-Meldungen](scheduling-and-retries.md#cron-meldungen-und-leader)).

Scheitert der Publish eines Termins, nachdem der Leader das Flag
genommen hat, gibt er es wieder frei; der Versuch im nächsten Takt gilt
dann nicht als Duplikat. Das Flag bleibt, wenn der Publish beim Warten
auf das Confirm des Brokers seine Frist gerissen oder die Verbindung
verloren hat (`PublishIndeterminate`), denn die Nachricht kann
angekommen sein. Ein Flag, das `schedule(delay=|eta=)` genommen hat,
gehört dem Producer und wird bei einem solchen Fehler nie freigegeben.

### Wenn `schedule()` scheitert

Ein Claim, den `schedule()` oder `schedule_many()` genommen hat, bleibt
bis zu seinem Ablauf stehen, wenn der Ausgang unbestimmt ist:

- nach `PublishIndeterminate`,
- nach einem Backend-Ausfall beim Speichern eines `delay`/`eta`-Eintrags,
- wenn der Aufruf während des Publish oder des Speicherns eines
  `delay`/`eta`-Eintrags abgebrochen wird (ein Timeout um ihn herum,
  `task.cancel()`).

Nachricht oder Eintrag können angekommen sein, ein Wiederholversuch in
diesem Fenster gilt darum als Duplikat. Bestimmte Fehler geben den Claim
frei, ebenso ein Backend-Ausfall beim Claim selbst (`schedule()` wirft
dann `BackendConnectionError`). Auch ein Abbruch, bevor der Broker die
Nachricht gesendet haben kann, gibt ihn frei: während der Publish auf
eine Verbindung wartet, die der Broker im Ressourcenalarm blockiert hat,
den Publish-Kanal öffnet, auf die Kanal-Sperre wartet oder nach einem
Versuch, der nichts gesendet hat, zurückwartet. Der Broker wirft dann
`PublishCancelledUnsent`, eine Unterklasse von `asyncio.CancelledError`
([Fehlerkatalog](../reference/errors.md#auerhalb-der-hierarchie-publishcancelledunsent)).

### Pro Aufruf überschreiben

`unique_for` und `unique_until` lassen sich je `schedule()`-Aufruf
überschreiben:

```python
# Dedup nur für diesen Aufruf abschalten.
await app.schedule("send_welcome", user_id=42, unique_for=None)

# Kürzeres Fenster für diesen Aufruf.
await app.schedule("send_welcome", user_id=42, unique_for=60)
```

Übergib `None`, um Dedup abzuschalten, eine Zahl, um zu überschreiben.
Lass das Keyword weg, um den `TaskInfo`-Standard zu nutzen. Ein Producer
ohne Task-Registry kennt keinen Standard: Dort gilt nur das Keyword, und
der Client warnt einmal je Task-Name (`client_task_defaults_unresolved`).

### Wie Dedup berechnet wird

Der Dedup-Key ist ein SHA-256-Hash aus Task-Name und einer kanonischen
JSON-Kodierung von `args` und `kwargs`:

- Die Reihenfolge der Positionsargumente zählt.
- Die Reihenfolge der Keyword-Argumente und der Dict-Schlüssel zählt
  nicht (die Kodierung sortiert sie).
- Typen zählen, wo JSON sie unterscheidet (`1` und `"1"` sind
  verschieden). `UUID`, `datetime`, `date`, `time` und `Decimal` hashen
  wie ihre String-Form, ein `tuple` wie eine `list`, ein `set` wie seine
  sortierte Liste.
- `bytes`, `nan`, `inf`, Dict-Schlüssel, die kein `str` sind, und
  Objekte eigener Klassen (Dataclasses, `msgspec.Struct`, einfache
  `Enum`-Mitglieder) werfen beim Aufruf `TypeError`. Wandle sie vorher
  um oder schalte Dedup für den Aufruf ab.

Zwei Clients, die `schedule("sync_user", user_id=42)` aus verschiedenen
Prozessen aufrufen, landen auf demselben Key und deduplizieren korrekt.

### Dedup beobachten

Jeder Dedup-Treffer loggt `dedup_detected` (INFO) und feuert
`TASK_DUPLICATE_DETECTED`:

```python
from swarmq import Signal
from swarmq.logging import get_logger

log = get_logger("myapp.dedup")


@app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
async def on_dup(
    existing_task_id, task_name, dedup_hash, source, **kwargs,
):
    log.info(
        "myapp_dedup_hit",
        task=task_name,
        existing_task_id=existing_task_id,
        source=source,
        batch_index=kwargs.get("batch_index"),
    )
```

`source` nennt den Ursprung:

- `"client.schedule"`: sofortiger Pfad.
- `"client.schedule_deferred"`: Pfad mit `delay=` / `eta=`.
- `"client.schedule_many"`: Batch-Einreihung (mit `batch_index`).
- `"scheduler.fire"`: Der Scheduler-Leader hat einen Cron-Termin
  übersprungen, weil der vorige Lauf das Flag noch hält.

Die `client.*`-Quellen feuern in der App, die `schedule()` aufruft;
dort registrierst du den Handler. `"scheduler.fire"` feuert beim
Scheduler-Leader, einem Worker: unter `swarmq worker` in einem
`--configure`-Hook registrieren
([Signals](signals.md#einen-handler-registrieren)).

## Locks und Dedup kombinieren

Dedup wirkt beim Producer, der Lock beim Worker. Client-Retries fallen
zu einem Task zusammen; Nachrichten mit demselben Lock-Namen laufen
nacheinander.

```python
class PollWebhook(Task):
    task_info = TaskInfo(
        name="poll_webhook",
        queues=["default"],
        lock="poll:{endpoint}",
        hard_timeout=120,
        unique_for=30,
        unique_until="start",
    )

    async def run(self, endpoint: str) -> None:
        ...
```

Ein Client, der `schedule("poll_webhook", endpoint="x")` fünfmal
innerhalb von 30 s aufruft, bekommt aus allen fünf Aufrufen dieselbe
Task-ID. Ein Poll für `x`, der nach dem Start des ersten eingeplant
wurde, wartet auf den Lock, bis der erste fertig ist.

## Verifizieren

Deklariere einen Lock, starte zwei Worker, plane zwei Tasks mit
demselben Lock-Namen und lies die Worker-Logs:

```bash
swarmq worker --module myapp.tasks --log-level INFO --log-lifecycle-level INFO
```

Worker 1 loggt `task_started` für die erste Nachricht (auf INFO sichtbar
dank `--log-lifecycle-level INFO`; der Standard-Level dieses Events ist
DEBUG). Worker 2 loggt `task_lock_conflict` mit `lock_name` und
`lock_conflict_count` für die zweite Nachricht und stellt sie zurück.
Nach `task_succeeded` von Worker 1 läuft die zurückgestellte Nachricht
und loggt ebenfalls `task_succeeded`.

Für Dedup zweimal einplanen:

```python
tid1 = await app.schedule("sync_user", user_id=42)
tid2 = await app.schedule("sync_user", user_id=42)
assert tid1 == tid2   # mit unique_for am Task
```

## Siehe auch

- [Zustellsemantik](../explanation/delivery-semantics.md): die
  Idempotenz-Erwartungen, die diese Kombination nötig machen
- [`TaskInfo.lock`-Referenz](../reference/configuration.md#locking-und-concurrency)
- [`TaskInfo.unique_for`-Referenz](../reference/configuration.md#deduplizierung)
- [Signals](signals.md): `TASK_LOCKED` und `TASK_DUPLICATE_DETECTED`
  abonnieren
