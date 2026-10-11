# Locking und Deduplizierung

Du willst verhindern, dass dieselbe logische Arbeit parallel läuft
(Locking), oder dass derselbe Aufruf zweimal enqueued wird
(Deduplizierung). SwarmQ bietet beides mit je eigenem
Entscheidungspunkt.

## Welches brauchst du?

- **Lock** — gattert **Ausführung**. Zwei Worker, die die Nachricht
  beide in Flight haben, werden `run()` nicht parallel aufrufen.
- **Unique Task** — gattert **Scheduling**. Zwei `schedule()`-
  Aufrufe mit denselben Args produzieren eine task_id. Der zweite
  Aufruf gibt still die bestehende Owner-task_id zurück.

Sie komponieren — ein Task kann beides deklarieren. Ein häufiges
Muster ist `unique_for`, um Client-Retries zu absorbieren, und
`lock`, um die eigentliche Ausführung zu serialisieren.

## Locking

### Einen Lock deklarieren

```python
class SyncUser(Task):
    task_info = TaskInfo(
        name="sync_user",
        queues=["users"],
        lock="user-{user_id}",
        hard_timeout=60,
    )

    async def run(self, user_id: int) -> None:
        ...
```

Der `lock`-Name kann ein Format-String sein, der Kwargs oder
Positional-Argument-Namen von `run()` referenziert. Bei Pickup
rendert der Worker das Template gegen die tatsächlichen Args —
`"user-{user_id}"` wird zu `"user-42"`, wenn
`schedule("sync_user", user_id=42)` dispatched wird.

`hard_timeout` ist **pflicht**, wenn `lock` gesetzt ist. Die TTL
des Locks wird davon abgeleitet (`hard_timeout + 30 s`): Sie ist die
Absturzfrist, sodass ein Task ohne Hard-Timeout den Lock nach einem
Worker-Crash sonst unbegrenzt halten könnte.

### Mutex vs Semaphor

Per Default ist `max_concurrency=1` — der Lock ist ein Mutex.
Setze ihn höher, um nebenläufige Ausführungen per Key zu cappen:

```python
task_info = TaskInfo(
    name="pdf_render",
    queues=["pdf"],
    lock="pdf",          # ein Bucket für alle Renders
    max_concurrency=4,   # bis zu 4 in Flight
    hard_timeout=300,
)
```

`max_concurrency=4` lässt vier Worker-Prozesse gleichzeitig PDFs
rendern, über die ganze Flotte hinweg, unabhängig von
`--concurrency` oder `--processes`. `max_concurrency` braucht
`lock`; es ohne zu setzen raist `ValueError` bei Klassendefinition.

### Lock-Acquisition-Verhalten

Wenn der Worker eine Nachricht aufnimmt, deren Lock gehalten wird:

- Die Nachricht wird **mit Delay requeued** (100 ms, je Konflikt
  verdoppelt, höchstens 60 s). Sie blockiert den Worker nicht und
  scheitert den Task nicht.
- Der Worker feuert bei jedem Konflikt das `TASK_LOCKED`-Signal
  (`lock_conflict_count` zählt die Konflikte dieser Nachricht) und
  loggt `task_lock_conflict` (INFO, mit `lock_name`,
  `lock_conflict_count` und `waited_ms`) beim ersten Konflikt und
  danach etwa einmal je Minute.
- Die Nachricht wartet, solange der Lock den Halter wechselt oder sein
  Halter ihn am Leben hält (ab 0.12 gibt es keinen Deckel von 10
  Konflikten mehr). In die DLQ geht sie mit `LockWaitExceeded`
  (Fehlerergebnis, `TASK_DLQ`, ein Workflow-Schritt markiert seinen
  Workflow als gescheitert) erst, wenn
  - derselbe Halter den Lock länger hält als die beim letzten
    Halterwechsel gesehene TTL des Keys plus 60 s (ein hängender
    Halter, dessen Lock am Leben gehalten wird), oder
  - seit dem ersten Konflikt `SWARMQ_LOCK_WAIT_MAX_S` (Standard 6 h)
    vergangen ist.
- `max_age` bleibt die harte Grenze: Ein Task mit `max_age` läuft in
  die DLQ, sobald er älter ist, egal wie lange er gewartet hat.

Gemessen wird mit den Wanduhren der Worker
(`x-swarmq-lock-wait-since`, `x-swarmq-lock-holder-since`); halte die
Uhren synchron, ein Versatz bis etwa 60 s wird toleriert. Ein
Lock-Store, der seine Halter nicht melden kann (fremder Store), loggt
einmal `lock_observe_unsupported` und wartet bis
`SWARMQ_LOCK_WAIT_MAX_S`; ebenso ein Store, dessen `observe` keine
`LockObservation` liefert. Die Gauges `swarmq_lock_waits` und
`swarmq_lock_wait_oldest_seconds` zeigen je Worker, wie viele
Nachrichten er zuletzt zurückgestellt hat und wie lange die älteste
schon wartet.

Eine neue Wartezeit beginnt, wenn der Broker die Nachricht neu
zustellt (der Worker starb, nachdem er den Lock erworben hatte, oder
gab die Nachricht beim Herunterfahren zurück), und auf der Kopie einer
späteren Ausführung (Retry, `dlq retry`; der Header
`x-swarmq-lock-wait-gen` hält Retry-Zähler und Replay-ID der wartenden
Ausführung fest). Eine alte Wartezeit beendet eine neu zugestellte
Nachricht also nie sofort. Klassische Queues führen keinen
Zustellzähler; dort setzt eine Neuzustellung die alte Wartezeit fort.

Im Mischbetrieb mit 0.11-Workern derselben App: 0.12 erhöht
`x-swarmq-lock-conflict-count` nicht mehr, ein 0.11-Worker sieht eine
0.12-Kopie also nie an seinem Deckel von 10. Stellt ein 0.11-Worker
eine Nachricht selbst zurück, erhöht er diesen Zähler weiter und
dead-lettert wie bisher beim 10. Konflikt; den Wartezustand von 0.12
lässt er dabei stehen.

Ist Valkey beim Acquire nicht erreichbar (oder antwortet nicht
innerhalb von `SWARMQ_BACKEND_OP_TIMEOUT`), ist das kein Konflikt: Die
Nachricht wird mit eigenem Backoff über die Delay-Queue
zurückgestellt, ohne `TASK_LOCKED` und ohne `lock_conflict_count` zu
erhöhen, und der Worker loggt `task_deferred_backend_unavailable`
(`gate="lock"`, `lock_name`). Jeder andere Fehler des Acquire loggt
`lock_acquire_failed` (ERROR) und reiht die Nachricht an derselben
Stelle neu ein. Ein Acquire, der nach Ablauf der Frist in Valkey doch
noch ausgeführt wurde, lässt den Lock bis zu seiner TTL gehalten.

Setze `max_age` bei Lock-nutzenden Tasks, deren Arbeit veraltet,
damit eine Nachricht nicht stundenlang hinter einem langen Halter
wartet.

### Locks während der Ausführung

Der Worker hält einen Lock, den er besitzt, so lange am Leben, wie
der Handler läuft (ab 0.12). Er beginnt ein Fenster vor der
ursprünglichen Frist `t0 + TTL` mit dem Verlängern (TTL =
`hard_timeout + 30 s`; das Fenster sind drei Takte, ein Takt ist
`min(max(TTL / 3, 50 ms), 20 s)`). Davor passiert nichts. Ab einem
`hard_timeout` von 30 s ist das Fenster 60 s lang, ein Task, der
innerhalb von `hard_timeout - 30 s` fertig wird, kostet also keinen
zusätzlichen Roundtrip. Bei kürzerem `hard_timeout` deckt das Fenster
die ganze TTL: Ein Handler, der länger als etwa ein Drittel seiner TTL
läuft, wird ab dem ersten Takt verlängert, ein Roundtrip je Takt. Jede
Verlängerung gibt ein Fenster dazu, Mutex wie Semaphor-Slot. Ein hart
beendeter Halter (SIGKILL, OOM) hinterlässt den Lock deshalb wie
bisher bis `t0 + TTL`, oder höchstens ein Fenster länger, wenn er
beim Überziehen beendet wurde.

Ein Handler, der seinen Abbruch ignoriert (Task-Code, der
`CancelledError` schluckt), überlebt `hard_timeout + 30 s` und behält
seinen Lock; der Worker loggt einmal `task_lock_held_past_hard_timeout`
(WARNING). Ein blockierter Event-Loop behält den Lock nicht: Er
blockiert auch die Verlängerung, der Lock kann ablaufen, während der
Handler läuft, und der Worker meldet das als `task_lock_lost`, sobald
der Loop wieder läuft. Blockierender Code gehört in den
[CPU-Modus](cpu-mode.md), dort tötet `hard_timeout` den Kindprozess.

Findet die Verlängerung den Lock weg oder übernommen (verdrängt durch
`maxmemory-policy`, Valkey-Datenverlust oder ein Valkey-Ausfall von
etwa zwei Takten, sobald die Verlängerungen begonnen haben, bei einem
Takt von 20 s also etwa 40 s), loggt der Worker `task_lock_lost`
(WARNING, `stage="running"`): Ein zweiter Halter kann jetzt parallel
laufen. Dieselbe Warnung mit `stage="release"` kommt, wenn der Lock
bei der Freigabe schon weg ist. Eine gescheiterte Verlängerung loggt
`lock_extend_failed` und wird im nächsten Takt wiederholt.

Grenzen: Ein Erwerber mit kurzer TTL räumt weiterhin Semaphor-Slots
ab, die älter als seine eigene TTL sind; gemischte
`hard_timeout`-Werte unter einem Semaphor-Namen werden daher nicht
unterstützt. Ein 0.11-Worker, der einen Slot erwirbt, verkürzt die
TTL des Keys weiter auf seine eigene.

### Lock-Render-Fehler

Referenziert das Template einen Kwarg, den die Nachricht nicht
trägt (Typo, alte Nachricht von einem umbenannten Task), scheitert
der Render und der Task wird mit `x-swarmq-lock-render-error`-Header in
die DLQ geroutet. Der Fehler wird dem Operator sichtbar gemacht,
nicht still geschluckt, weil ein gebrochenes Lock-Template ein Bug
ist — du willst ihn laut.

### Leere Lock-Namen

Ein `lock` aus `""` oder Whitespace würde auf den bloßen
Valkey-Namespace-Key `sq:lock:` kollabieren und mit jedem anderen
Empty-Locked-Task kollidieren. `TaskInfo` lehnt das bei
Klassendefinition ab.

## Deduplizierung (Unique Tasks)

### Dedup deklarieren

```python
class SendWelcomeEmail(Task):
    task_info = TaskInfo(
        name="send_welcome",
        queues=["default"],
        unique_for=3600,           # Dedup innerhalb der Stunde
        unique_until="completion", # Flag bei Terminalzustand freigeben
    )

    async def run(self, user_id: int) -> None:
        ...
```

`unique_for=3600` bedeutet: Innerhalb 3600 Sekunden nach dem ersten
Schedule gibt ein zweiter Aufruf mit denselben `(name, args,
kwargs)` die task_id des ersten Tasks zurück, ohne zu enqueuen.

### Wann das Flag freigeben

`unique_until` wählt den Freigabepunkt:

- `"completion"` (Default) — das Flag lebt, bis der Task einen
  Terminalzustand (Success, DLQ, Cancel oder Ablauf über `max_age`)
  erreicht. Das blockiert erneute Schedules während des gesamten
  Ausführungsfensters.
- `"start"` — das Flag wird in dem Moment freigegeben, in dem ein
  Worker die Nachricht aufnimmt. Ein Folge-`schedule()` während
  der Task läuft, enqueued einen neuen.

Wähle `"completion"` für idempotente Arbeit („Welcome-E-Mail an
User 42 senden") und `"start"` für Arbeit, bei der überlappende
Ausführungen ok sind, doppelte pending Messages aber nicht.

### Unique-Crons

Bei einem statischen Cron verhindert `unique_for`, dass ein langer Lauf
den nächsten Termin überlappt: Der Scheduler-Leader nimmt das Flag,
bevor er einen Termin veröffentlicht, und ein Termin, dessen Flag noch
gehalten wird, wird übersprungen (`TASK_DUPLICATE_DETECTED`,
`source="scheduler.fire"`) und gilt als erledigt. Ab 0.12 gilt das bei
jedem Leader, auch bei einem, der den Task nicht in seiner Registry
hat: Worker melden `unique_for` und `unique_until` mit ihren Crons
([Cron-Meldungen](scheduling-and-retries.de.md#cron-meldungen-und-leader)).

Scheitert der Publish eines Termins, nachdem der Leader das Flag
genommen hat, gibt er es wieder frei; der Versuch im nächsten Takt gilt
dann nicht als Duplikat. Das Flag bleibt, wenn der Publish beim Warten
auf das Confirm des Brokers seine Frist gerissen hat
(`PublishIndeterminate`), denn die Nachricht kann angekommen sein. Ein
Flag, das `schedule(delay=|eta=)` genommen hat, gehört dem Producer und
wird bei einem solchen Fehler nie freigegeben.

### Per-Call-Override

`unique_for` und `unique_until` können am `schedule()`-Aufruf
überschrieben werden:

```python
# Dedup nur für diesen Aufruf deaktivieren.
await app.schedule("send_welcome", user_id=42, unique_for=None)

# Engere TTL für diesen Aufruf.
await app.schedule("send_welcome", user_id=42, unique_for=60)
```

Übergib `None`, um Dedup explizit zu deaktivieren; übergib ein
`int`, um zu überschreiben. Lass den Kwarg aus, um den
`TaskInfo`-Default zu nutzen.

### Wie Dedup berechnet wird

Der Dedup-Key ist ein SHA-256-Hash einer kanonischen Kodierung von
`(task_name, args, kwargs)`. Das heißt:

- Argument-Reihenfolge zählt für Positional-Args (du hashst ein
  Tupel).
- Dict-Reihenfolge zählt **nicht** für Kwargs (die Kodierung
  sortiert nach Key).
- Argument-Typen zählen (`1` und `"1"` hashen unterschiedlich).

Zwei Clients, die `schedule("sync_user", user_id=42)` aus
verschiedenen Prozessen aufrufen, kollidieren auf demselben Hash
und dedupen korrekt.

### Dedup beobachten

Jeder Dedup-Hit feuert `TASK_DUPLICATE_DETECTED`:

```python
from swarmq import Signal
from swarmq.logging import get_logger

log = get_logger("myapp.dedup")


@app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
async def on_dup(
    existing_task_id, task_name, dedup_hash, source, **kwargs,
):
    batch_index = kwargs.get("batch_index")
    log.info(
        "myapp_dedup_hit",
        task=task_name,
        existing_task_id=existing_task_id,
        source=source,
        batch_index=batch_index,
    )
```

`source` unterscheidet den Ursprung:

- `"client.schedule"` — immediate Pfad.
- `"client.schedule_deferred"` — `delay=` / `eta=`-Pfad.
- `"client.schedule_many"` — Batch-Enqueue.
- `"scheduler.fire"` — der eingebettete Scheduler feuerte einen
  wiederkehrenden Task, der gegen einen vorherigen Run dedup'te.

## Locks und Dedup kombinieren

Dedup auf Client-Layer, Lock auf Worker-Layer. Client-Retries
kollabieren zu einem Task; nebenläufige Worker serialisieren die
Ausführung.

```python
class PollWebhook(Task):
    task_info = TaskInfo(
        name="poll_webhook",
        queues=["default"],
        lock="poll-{endpoint}",
        hard_timeout=120,
        unique_for=30,
        unique_until="start",
    )

    async def run(self, endpoint: str) -> None:
        ...
```

Ein Client, der `schedule("poll_webhook", endpoint="x")` 5-mal in
30 s retry't, bekommt aus allen 5 Aufrufen eine task_id. Zwei
Worker, die die Nachricht beide sehen, erwerben den Lock
sequentiell.

## Verifizieren

Deklariere einen Lock, plane den Task aus zwei Clients
nebenläufig und inspiziere die Worker-Logs:

```bash
swarmq worker --module myapp.tasks --log-level INFO --log-lifecycle-level INFO
```

Worker 1 loggt `task_started` für die erste Nachricht (auf INFO
sichtbar dank `--log-lifecycle-level INFO`; das Default-Level dieses
Events ist DEBUG). Worker 2 loggt `task_lock_conflict` mit `lock_name`
und `lock_conflict_count` für die zweite Nachricht und reiht sie
verzögert neu ein. Nach `task_succeeded` von Worker 1 läuft der
re-delivered Versuch von Worker 2 und loggt ebenfalls
`task_succeeded`.

Für Dedup zweimal planen:

```python
tid1 = await app.schedule("sync_user", user_id=42)
tid2 = await app.schedule("sync_user", user_id=42)
assert tid1 == tid2
```

## Siehe auch

- [Zustellungssemantik](../explanation/delivery-semantics.md) —
  Idempotenz-Erwartungen, die diese Komposition nötig machen
- [`TaskInfo.lock`-Referenz](../reference/configuration.md#locking-und-concurrency)
- [`TaskInfo.unique_for`-Referenz](../reference/configuration.md#deduplizierung)
- [Signals](signals.md) — Abonnement von `TASK_LOCKED` und
  `TASK_DUPLICATE_DETECTED`
