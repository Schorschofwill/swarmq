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
des Locks wird davon abgeleitet (`max(30 s, 0.25 × hard_timeout)`),
sodass ein Task ohne Hard-Timeout den Lock nach einem Worker-Crash
sonst unbegrenzt halten könnte.

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

- Die Nachricht wird **mit Delay requeued**. Sie blockiert den
  Worker nicht und scheitert den Task nicht.
- Der Worker loggt `task_lock_conflict` (INFO, mit `lock_name` und
  `lock_conflict_count`) und feuert das `TASK_LOCKED`-Signal, damit du
  es beobachten kannst.
- Ist `max_age` des Tasks gesetzt, läuft sie irgendwann in die DLQ,
  statt ewig zu retry'n.

Der Delay backed off — wiederholte Misses erweitern die Wartezeit.
Die meisten Lock-nutzenden Tasks wollen `max_age` als
Sicherheitsnetz, damit ein stuck Holder die Queue nicht dauerhaft
zurückstaut.

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
  Terminalzustand (Success, DLQ, Cancel) erreicht. Das blockiert
  erneute Schedules während des gesamten Ausführungsfensters.
- `"start"` — das Flag wird in dem Moment freigegeben, in dem ein
  Worker die Nachricht aufnimmt. Ein Folge-`schedule()` während
  der Task läuft, enqueued einen neuen.

Wähle `"completion"` für idempotente Arbeit („Welcome-E-Mail an
User 42 senden") und `"start"` für Arbeit, bei der überlappende
Ausführungen ok sind, doppelte pending Messages aber nicht.

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
