# Scheduling und Retries

Du willst steuern, **wann** ein Task läuft und **wie oft** er bei
Fehler retry't wird. Beides lebt auf `TaskInfo` (Defaults für jede
Ausführung) und auf `app.schedule()` (Per-Call-Overrides).

## Voraussetzungen

- Eine Task-Klasse mit `task_info = TaskInfo(name=..., queues=...)`.
- Ein laufender Broker und Backend — siehe
  [Schnellstart](../getting-started.md).

## Immediate vs Deferred

Default-`schedule()` ist immediate — die Nachricht landet in der
Queue, der erste bereite Worker holt sie.

```python
task_id = await app.schedule("send_email", to="a@example.com")
```

Zwei kwargs verzögern den Publish: `delay=` und `eta=`.

```python
# In 30 Sekunden laufen.
await app.schedule("send_email", to="a@example.com", delay=30)

# Zu einem spezifischen Zeitpunkt (muss in Zukunft; 1s Skew ok).
from datetime import datetime, timedelta, UTC
eta = datetime.now(UTC) + timedelta(minutes=15)
await app.schedule("send_email", to="a@example.com", eta=eta)
```

Deferred Tasks sitzen nicht in der RabbitMQ-Queue — sie sitzen im
Valkey-Scheduler-Store. Ein Worker (der gewählte Leader) pollt den
Store und publisht zum Fire-Zeitpunkt an RabbitMQ. Siehe
[Architektur → Message Lifecycle](../explanation/architecture.md#nachrichten-lifecycle).

`delay=0` ist ein einfacher Immediate-Publish; nur positives
`delay` oder beliebiges `eta` nimmt den Scheduler-Pfad.

## Cron-Scheduling

Für wiederkehrende Arbeit setze `cron` auf `TaskInfo`:

```python
class DailyReport(Task):
    task_info = TaskInfo(
        name="daily_report",
        queues=["reports"],
        cron="0 3 * * *",  # jeden Tag um 03:00 UTC (ohne Zeitzone)
    )

    async def run(self) -> None:
        ...
```

Nur Standard-5-Feld-Cron; Extended Syntax (`L`, `W`, `#`) wird zur
Klassendefinitionszeit abgelehnt. Der eingebettete Scheduler des
Workers nimmt die Deklaration automatisch auf — kein separater
Cron-Daemon.

!!! info "Verpasste Fires werden übersprungen, nicht nachgeholt"
    Nach Scheduler-Downtime feuert jeder Cron genau **einmal** als
    Catch-up; während des Ausfalls verpasste Termine laufen nie. Der
    Scheduler emittiert ein `SCHEDULER_FIRE_SKIPPED`-Signal (mit
    `schedule_name` und `skipped_count`), damit wichtige Crons
    alarmierbar sind.

### Zeitzonen

Ohne Zeitzone wird ein Cron-Ausdruck in UTC ausgewertet: `0 3 * * *`
feuert ganzjährig um 03:00 UTC. Mit `timezone` und einer IANA-Zone
feuert er stattdessen zur lokalen Uhrzeit:

```python
class DailyReport(Task):
    task_info = TaskInfo(
        name="daily_report",
        queues=["reports"],
        cron="0 3 * * *",
        timezone="Europe/Berlin",  # 03:00 in Berlin, Sommer wie Winter
    )
```

- Die Zone wird bei der Klassendefinition geprüft; ein unbekannter
  Name wirft `ValueError`. `timezone` ohne `cron` wird ebenfalls
  abgelehnt.
- `SWARMQ_SCHEDULER_TIMEZONE` (`SwarmQConfig(scheduler_timezone=...)`)
  setzt einen Standard für alle Cron-Tasks ohne eigene `timezone`. Eine
  unbekannte Zone ist dort ein `ConfigurationError`.
- Eine `timezone` am Task schlägt immer den Standard, auch ein
  ausdrückliches `timezone="UTC"`. Damit bleiben Crons in UTC, wenn der
  Standard gesetzt ist.
- `"UTC"`, `"Etc/UTC"` und keine Angabe sind gleichwertig: Die
  gespeicherte Definition trägt dann keine Zone, bestehende Schedules
  behalten beim Upgrade ihren Fingerabdruck.
- swarmq hängt von `tzdata` ab, damit Zonen auch in schlanken Images
  ohne `/usr/share/zoneinfo` aufgelöst werden.

Die Zone besser pro Task angeben. Ein globaler Standard, der in einem
anderen Deploy umgestellt wird als die betroffenen Ausdrücke,
verschiebt alle Crons dieser App um den UTC-Versatz.

Mit `swarmq schedule add --cron` angelegte Crons gelten immer in UTC;
die CLI hat noch keine Zonen-Option.

### Zeitumstellung

In einer Zone mit Sommerzeit gibt es eine Nacht im Jahr mit einer
doppelten Stunde (Herbst) und eine mit einer fehlenden Stunde
(Frühjahr). Die Regel hängt davon ab, ob das Stundenfeld des
Ausdrucks ein `*` enthält (auch `*/n`; `@hourly` und die anderen
`@`-Aliase zählen in ihrer Fünf-Feld-Form, `@hourly` ist also
`0 * * * *` und `@daily` ist `0 0 * * *`):

| Lage des Termins | Stundenfeld ohne `*` (z. B. `30 2 * * *`) | Stundenfeld mit `*` (z. B. `15 * * * *`, `*/30 * * * *`) |
|---|---|---|
| normale Zeit | feuert | feuert |
| doppelte Stunde, erster Durchlauf | feuert | feuert |
| doppelte Stunde, zweiter Durchlauf | feuert nicht; der nächste Termin wird ab dem Ende der doppelten Stunde gesucht | feuert (folgt der echten Zeit) |
| Lücke im Frühjahr | feuert einmal zur nächsten existierenden Uhrzeit | Verhalten von croniter, siehe unten |

Beispiele für `Europe/Berlin` im Jahr 2026:

- `30 2 * * *` feuert am 25. Oktober nur um 02:30 MESZ, nicht noch
  einmal um 02:30 MEZ; am 29. März, an dem es 02:30 nicht gibt, feuert
  er um 03:00 MESZ.
- `15 * * * *` feuert am 25. Oktober um 02:15 MESZ und noch einmal um
  02:15 MEZ (eine Stunde echter Zeit später). Am 29. März entfällt
  02:15: Nach 01:15 MEZ folgt 03:15 MESZ.
- `45 */2 * * *` feuert am 29. März um 00:45, dann um 03:00 statt zur
  fehlenden 02:45, dann um 04:45.

Mit `*` im Stundenfeld folgt die Lücke im Frühjahr croniter und kann
vom Ausgangspunkt der Rechnung abhängen. In `Australia/Lord_Howe`, wo
die Uhr um eine halbe Stunde springt, feuert `15 * * * *` am
4. Oktober 2026 um 01:15 (+10:30) und dann um 02:30 (+11:00). Beginnt
die Rechnung im zweiten Durchlauf einer doppelten Stunde, feuert ein
Cron ohne `*` im Stundenfeld erst am Folgetag: `30 2 * * *` ab 02:10
MEZ am 25. Oktober feuert als Nächstes am 26. Oktober um 02:30.

Ein Cron, dessen nächster Termin sich nicht berechnen lässt (etwa eine
Zone, die das `tzdata` des Leaders nicht kennt), blockiert keine
anderen Crons: Der Leader loggt `scheduler_next_fire_failed`, nimmt den
Eintrag aus dem Fire-Index und behält die Definition. Solange ein
lebender Worker den Cron meldet, plant ihn der nächste Übernahme-Takt
wieder ein, sobald sich der Termin berechnen lässt.

`swarmq.scheduler.next_fire_times` berechnet Termine mit derselben
Regel wie der Scheduler:

```python
from datetime import UTC, datetime
from swarmq.scheduler import next_fire_times

next_fire_times("0 3 * * *", datetime.now(UTC), timezone="Europe/Berlin", count=3)
# [datetime(…, 3, 0, tzinfo=ZoneInfo('Europe/Berlin')), …]
```

`start` muss zeitzonenbehaftet sein (ein naiver `start` wirft
`ValueError`); die Termine kommen
zeitzonenbehaftet in der angegebenen Zone zurück (ohne `timezone` in
UTC). Ein ungültiger Ausdruck wirft croniters `CroniterBadCronError`.

### Cron-Meldungen und Leader

Jeder Worker meldet die statischen Crons seiner eigenen Task-Registry
an Valkey: beim Start und danach alle
`SWARMQ_SCHEDULER_SEEN_INTERVAL_S` Sekunden (Standard `60`). Eine
Meldung enthält den Namen, die vollständige Definition und einen
Fingerabdruck über die Felder aus dem Code: Cron-Ausdruck, Zeitzone
(bereits aufgelöst aus Task oder Standard des Workers), Queue und
Task-Name. Meldungen tragen die Serverzeit von Valkey, Uhrabweichungen
zwischen Workern spielen keine Rolle.

Der Scheduler-Leader übernimmt gemeldete Crons im selben Takt: sofort
nach der Übernahme der Leader-Rolle und danach in jedem Takt. Pro Name
wählt er unter den lebenden Meldungen die Definition, die zuletzt neu
aufgetaucht ist. Lebendig ist eine Meldung, die in den letzten drei
Takten erneuert wurde. Der Leader prüft die Definition (Zone,
Ausdruck), bevor er sie schreibt; eine ungültige Meldung lässt die
gespeicherte Definition unverändert und loggt in jedem Takt
`scheduler_cron_report_invalid`.

Was das praktisch heißt:

- Ein im Code einer beliebigen App geänderter Cron, auch einer App,
  deren Worker nie Leader werden, gilt binnen zwei Takten nach dem
  Start des ersten Workers mit dem neuen Code.
- Ein Termin, der beim Ändern einer Definition schon fällig ist, geht
  nicht verloren: Er feuert einmal mit dem neuen Inhalt. Ein noch nicht
  fälliger Termin wird neu berechnet.
- Bei einem Rolling Deploy gewinnt der neuere Code-Stand. Ein alter
  Worker, der später neu startet, setzt ihn nicht zurück, weil seine
  Definition nicht neu ist. Neu ist eine Definition wieder, wenn sie
  drei Takte lang kein Worker gemeldet hat; so gewinnt auch ein Revert
  von B zurück auf A.
- Ein mit `swarmq schedule remove NAME` deaktivierter Schedule bleibt
  deaktiviert, wenn sich sein Code ändert.
- Ein statischer Cron, den ein lebender Worker meldet, ist nach
  `swarmq schedule remove --permanent` binnen eines Takts wieder da.
  Dauerhaft weg ist er erst, wenn ihn kein Code mehr meldet.
- Ein aus dem Code entfernter Cron hört auf zu feuern, sobald ihn
  `SWARMQ_SCHEDULER_PRUNE_AFTER_S` lang (Standard 24 h; ab 0.11.0, siehe
  [Crons entfernen](#crons-entfernen-die-keine-app-mehr-meldet)) kein
  Worker gemeldet hat. Sofort loswerden lässt er sich mit
  `swarmq schedule remove --permanent NAME`, sobald ihn kein Worker mehr
  meldet.
- Ein gleichnamiger CLI-Schedule schlägt den statischen Cron weiterhin,
  mit der Warnung `scheduler_static_cron_shadowed_by_cli`.
- Crons anderer Apps auf demselben Valkey feuern ohne die Warnung
  `scheduler_task_not_in_local_registry`, solange ein lebender Worker
  sie meldet. Ihre `TaskInfo` kennt der Leader weiterhin nicht;
  `unique_for` eines solchen Tasks wirkt also nicht auf seine
  Cron-Fires.

Meldungen, die 7 Tage lang nicht erneuert wurden, löscht der Leader,
oder nach dem Doppelten von `SWARMQ_SCHEDULER_PRUNE_AFTER_S`, falls das
länger ist.

Alle Apps auf einem Valkey sollten dasselbe
`SWARMQ_SCHEDULER_SEEN_INTERVAL_S` nutzen: Der Leader beurteilt die
Lebendigkeit mit seinem eigenen Takt.

### Crons entfernen, die keine App mehr meldet

Ab 0.11.0 entfernt der Leader einen statischen Cron, der mindestens
einmal gemeldet wurde und den seit `SWARMQ_SCHEDULER_PRUNE_AFTER_S`
Sekunden (Standard `86400`, 24 h) kein Worker mehr gemeldet hat. Er
prüft das in jedem Übernahme-Takt und loggt `scheduler_cron_pruned`.
Definition und Termin verschwinden in einem atomaren Schritt, und nur,
solange die Definition unverändert und jede Meldung des Namens noch zu
alt ist.

- **Nie gemeldete Crons bleiben.** Altbestand aus der Zeit vor 0.10.0
  und Crons von Apps, die noch ein swarmq vor 0.10.0 fahren, haben keine
  Meldung. Der Leader entfernt sie nie von selbst; er loggt
  `scheduler_cron_prune_candidate` einmal je Name und Leader-Lauf.
  `swarmq schedule prune --orphans` entfernt sie bewusst (siehe
  [CLI-Referenz](../reference/cli.md#swarmq-schedule-prune)).
  `swarmq schedule list` zeigt die jüngste Meldung je Cron in der
  Spalte `LAST_SEEN`: ein Datum, `never` für einen statischen Cron ohne
  jede Meldung, `-` für CLI-Schedules.
- **Pausierende Apps sind innerhalb der Frist sicher.** Ein Cron kommt
  mit seiner nächsten Meldung zurück; die Crons von App B bleiben,
  solange einer ihrer Worker innerhalb von
  `SWARMQ_SCHEDULER_PRUNE_AFTER_S` meldet. Die Frist anheben, wenn eine
  App länger stehen darf.
- **Es gelten die Einstellungen des Leaders.** `SWARMQ_SCHEDULER_PRUNE`
  und `SWARMQ_SCHEDULER_PRUNE_AFTER_S` liest der Worker, der gerade
  Scheduler-Leader ist, aus welcher App auf dem Valkey auch immer. In
  jeder App dieselben Werte setzen; eine längere Frist in nur einer App
  wirkt nicht, solange eine andere App den Leader stellt.
- **Deaktivierte Crons kommen deaktiviert zurück.** Der Leader merkt
  sich einen Cron, der beim Entfernen deaktiviert war
  (`sq:{sched}:disabled`); meldet ihn ein Worker wieder, wird er
  deaktiviert angelegt, und der Leader loggt
  `scheduler_cron_revived_disabled` (WARNING). Das gilt auch nach
  `schedule prune --orphans`. Gehört der Name inzwischen zu einem
  anderen Cron, löscht `swarmq schedule remove --permanent <name>` die
  Markierung; die nächste Meldung legt ihn aktiv an.
- **CLI-Schedules bleiben immer unberührt.**
- Wer einen Cron sofort loswerden will, nutzt weiterhin
  `swarmq schedule remove --permanent`.

`SWARMQ_SCHEDULER_PRUNE` schaltet es: `on` (Standard) entfernt, `log`
loggt nur `scheduler_cron_prune_skipped` einmal je Name, `off` tut
nichts. Die Frist muss mindestens das Dreifache von
`SWARMQ_SCHEDULER_SEEN_INTERVAL_S` betragen.

!!! warning "Jede App auf dem Valkey muss mindestens 0.10.0 fahren"
    Nur Worker ab 0.10.0 melden ihre Crons. Ein Cron, der einmal
    gemeldet wurde und jetzt nur noch von einem Worker vor 0.10.0
    bedient wird — etwa nachdem eine App zurückgerollt wurde —, gilt
    als verstummt und wird nach der Frist entfernt, obwohl die App noch
    läuft. Alle Apps auf demselben Valkey auf mindestens 0.10.0
    bringen, bevor Worker mit 0.11.0 Leader werden, oder in den
    0.11.0-Apps `SWARMQ_SCHEDULER_PRUNE=log` setzen, bis die letzte App
    umgestellt ist; dann `scheduler_cron_prune_skipped` auf Crons
    beobachten, die entfernt worden wären.

### Konflikte

Leben mehrere Definitionen eines Cron-Namens länger als 10 Minuten
nebeneinander, loggt der Leader in jedem Übernahme-Takt
`scheduler_cron_conflict` (mit allen lebenden Fingerabdrücken und dem
gewählten), sendet das Signal `SCHEDULER_CRON_CONFLICT` und zählt
`swarmq_scheduler_cron_conflicts_total{schedule_name}`. Es feuert
weiter die Definition, die zuletzt neu aufgetaucht ist. Eine kurze
Überlappung im Rolling Deploy ist kein Konflikt. Typische Ursache sind
gleichnamige Tasks in zwei Apps auf einem Valkey; sie brauchen
verschiedene Namen.

### Leader-Version

Der Leader schreibt seine swarmq-Version neben das Leader-Lock, und
jeder Worker meldet seine Version mit seinen Cron-Meldungen. Ein
swarmq vor 0.10.0 schreibt keine Version und kennt das Feld
`timezone` nicht: Als Leader verwirft er die Zone jedes Crons, den er
feuert, und feuert ihn danach wieder in UTC.

Ein Worker, der einen solchen Leader (ohne Version oder vor 0.10.0)
in zwei Wahl-Takten in Folge sieht (`SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL`),
loggt einmal pro Leader `scheduler_leader_outdated`, sendet das Signal
`SCHEDULER_LEADER_OUTDATED` und setzt die Gauge
`swarmq_scheduler_leader_outdated{worker_id}` auf `1`. Folgt ein
aktueller Leader, kommt das Signal erneut mit `outdated=False`, und die
Gauge fällt auf `0`. Ein Leader ab 0.10.0, der älter ist als der
Worker (rollendes Upgrade), kennt Zeitzonen und löst keine Warnung aus.

`swarmq schedule status` zeigt `LEADER_VERSION` und die meldenden
Worker mit ihren Versionen (siehe
[CLI-Referenz](../reference/cli.md#swarmq-schedule-status)).

### Von Hand umgerechnete UTC-Crons umstellen

Vor 0.10.0 mussten Crons, die zu einer lokalen Uhrzeit feuern sollen,
von Hand nach UTC umgerechnet werden. In der EU stimmt ein solcher
Ausdruck nur in der Sommerzeit: Nach der Rückstellung am 25. Oktober
2026 um 01:00 UTC feuert er eine Stunde zu spät. So werden solche
Crons auf eine Zone umgestellt:

1. **Zuerst alle Apps auf demselben Valkey auf 0.10.0 bringen, ohne
   einen Cron zu ändern.** Ein Leader vor 0.10.0 verwirft `timezone`
   bei jedem Fire. Mit `swarmq schedule status` prüfen, dass
   `LEADER_VERSION` und alle Einträge unter `WORKERS` 0.10.0 oder neuer
   zeigen, bevor es weitergeht.
2. **Pro App die Zone an jedem Task setzen und den Ausdruck im selben
   Commit auf Ortszeit zurückführen.** Eine Zone ohne zurückgeführten
   Ausdruck oder umgekehrt verschiebt den Cron um den UTC-Versatz.
   `timezone=` pro Task ist `SWARMQ_SCHEDULER_TIMEZONE` vorzuziehen;
   wer die Variable nutzt, stellt sie im selben Deploy um wie die
   Ausdrücke. Fallstricke:
    - **Wochentag und Tag im Monat.** Eine Umrechnung über Mitternacht
      hat auch den Tag verschoben: `30 22 * * 0` (Sonntag 22:30 UTC)
      ist in `Europe/Berlin` `30 0 * * 1` (Montag 00:30). Dasselbe gilt
      für den Tag im Monat und für Monatsgrenzen.
    - **`*/n` und Bereiche im Stundenfeld.** `0 */2 * * *` feuert in
      UTC und in Berlin zufällig zu denselben Zeitpunkten,
      `0 */5 * * *` nicht: Die umgerechneten Stunden waren 0, 5, 10,
      15, 20 UTC, in Berliner Sommerzeit also `0 2,7,12,17,22 * * *`.
      Solche Stunden ausschreiben und mit dem Rezept unten prüfen.
    - **`*` im Stundenfeld.** Ein Ausdruck wie `*/15 * * * *` ohne
      Einschränkung der Tage feuert in UTC und in einer Zone mit
      ganzstündigem Versatz zu denselben Zeitpunkten; er braucht keine
      Zone. Die abweichende Regel für die doppelte Stunde steht in der
      [Tabelle oben](#zeitumstellung).
3. **Vor dem 25. Oktober 2026, 01:00 UTC deployen** und vorher die
   Äquivalenz prüfen: Bis zur Umstellung muss der neue Ausdruck mit
   Zone dieselben Termine liefern wie der alte UTC-Ausdruck (Rezept
   unten).
4. **Kein Deploy zwischen 24. Oktober 22:00 und 25. Oktober 04:00
   UTC**, damit kein Rolling Deploy von Crons die Umstellung
   überlappt.
5. **Rollback:** Ausdrücke und Zone gemeinsam in einem Commit
   zurückrollen. swarmq vor 0.10.0 kennt `timezone=` nicht; wer swarmq
   selbst zurückrollt, muss also auch zu den umgerechneten
   UTC-Ausdrücken zurück.

Rezept für die Äquivalenzprüfung, etwa als Test im eigenen Projekt:

```python
from datetime import UTC, datetime

from swarmq.scheduler import next_fire_times

# Ende der Sommerzeit in der EU: 2026-10-25 01:00 UTC.
SWITCH = datetime(2026, 10, 25, 1, 0, tzinfo=UTC)


def fires_until(cron: str, start: datetime, end: datetime, timezone: str | None = None) -> list[datetime]:
    """Alle Termine von *cron* nach *start* und vor *end*, in UTC."""
    fires = []
    while True:
        (fire,) = next_fire_times(cron, start, timezone=timezone)
        fire = fire.astimezone(UTC)  # Zeitpunkte vergleichen, keine Wanduhrzeiten
        if fire >= end:
            return fires
        fires.append(fire)
        start = fire


def test_daily_report_keeps_its_fires() -> None:
    start = datetime.now(UTC)
    old = fires_until("0 1 * * *", start, SWITCH)  # von Hand aus 03:00 MESZ umgerechnet
    new = fires_until("0 3 * * *", start, SWITCH, timezone="Europe/Berlin")
    assert new == old
```

Vor dem Vergleich nach UTC umrechnen: Python hält zwei Datetimes in
verschiedenen Zonen nie für gleich, wenn einer davon in der doppelten
Stunde liegt, auch wenn beide denselben Zeitpunkt bezeichnen. Nach der
Umstellung unterscheiden sich die beiden Ausdrücke gewollt.

### CLI-definierte Schedules

Schedules hinzufügen ohne Code-Deploy:

```bash
swarmq schedule add weekly-billing \
  --cron '0 4 * * 1' \
  --task run_billing \
  --kwargs '{"tenant": "acme"}'

swarmq schedule list
swarmq schedule status --limit 5
```

Siehe [CLI → `swarmq schedule`](../reference/cli.md#swarmq-schedule)
für die volle Grammatik.

## Retry-Policy

Retries sind per Default aus. Aktiviere sie auf `TaskInfo`:

```python
class ChargeCard(Task):
    task_info = TaskInfo(
        name="charge",
        queues=["payments"],
        max_retries=5,
        retry_backoff="exponential",  # "linear", "fixed" auch verfügbar
        retry_base_delay=1.0,
    )

    async def run(self, card_id: str, amount: int) -> dict:
        return await stripe.charge(card_id, amount)
```

### Backoff-Formeln

| Strategie | Delay bei Versuch *n* (ab `n=1`) |
|---|---|
| `exponential` (Default) | `retry_base_delay × 2^(n-1)` |
| `linear` | `retry_base_delay × n` |
| `fixed` | `retry_base_delay` |

`exponential` und `linear` multiplizieren den Delay mit einem
Zufallsfaktor zwischen 1 und 2 (Jitter), damit sich die Retries vieler
gleichzeitig gescheiterter Nachrichten verteilen. `fixed` hat keinen
Jitter.

Globales Cap: `retry_max_delay` (Default 3600 s) begrenzt jeden
berechneten Delay. Ein Delay über dem Cap wird zu 0,6 bis
0,8 × `retry_max_delay`, aus demselben Jitter gezogen, damit auch
Retries am Cap verteilt bleiben. Setze `SWARMQ_RETRY_MAX_DELAY=...`
oder übergib `retry_max_delay=...` als `SwarmQ`-kwarg.

### Selektive Retries

Per Default verbrennt jede Exception aus `run()` einen
Retry-Versuch. Um nur spezifische Klassen zu retry'n, übergib
`retry_exceptions`:

```python
task_info = TaskInfo(
    name="charge",
    queues=["payments"],
    max_retries=5,
    retry_exceptions=[stripe.RateLimitError, stripe.APIConnectionError],
)
```

Jede andere Exception — `ValueError`, `KeyError`, der
Card-Declined-Fehler — umgeht das Retry-Budget und geht direkt in
die DLQ.

### Task-Body-Kontrolle mit `Retry` und `NoRetry`

Innerhalb `run()` kannst du die Policy überschreiben:

```python
from swarmq import Retry, NoRetry

class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email", queues=["default"], max_retries=3,
    )

    async def run(self, to: str) -> None:
        response = await smtp.send(to)
        if response.status == 429:
            raise Retry(delay=60.0)  # Retry in 60 s
        if response.status in (400, 403):
            raise NoRetry(f"unrecoverable: {response.status}")
```

- `Retry(delay=...)` — retry mit spezifischem Delay, ignoriert den
  konfigurierten Backoff. Nutze, wenn du die korrekte Wartezeit
  kennst (z. B. HTTP `Retry-After: 60`).
- `Retry()` ohne Delay — nutzt die Backoff-Formel.
- `NoRetry(reason)` — überspringt das Retry-Budget, routet sofort
  in die DLQ.

## Per-Call-Overrides

Alles auf `TaskInfo` ist ein Default. `schedule()` nimmt Overrides
für Scheduling-bezogene kwargs:

```python
# Queue zur Schedule-Zeit überschreiben.
await app.schedule("send_email", to="...", queue="priority")

# Dedup-Settings überschreiben.
await app.schedule("send_email", to="...", unique_for=300)
```

Retry-Policy kann nicht per Call überschrieben werden — sie ist
eine Eigenschaft des Tasks, nicht des Aufrufs.

## Timeouts

`TaskInfo.soft_timeout` und `TaskInfo.hard_timeout` (beide in
Sekunden) steuern, wie lange der Worker `run()` laufen lässt.

```python
task_info = TaskInfo(
    name="render_report",
    queues=["reports"],
    soft_timeout=240,
    hard_timeout=300,
)
```

- **Soft Timeout.** Der Worker raist `SoftTimeoutError` innerhalb
  `run()`, damit dein Task checkpointen und aufräumen kann.
- **Hard Timeout.** Der Worker bricht die Coroutine ab.

`hard_timeout` muss strikt größer als `soft_timeout` sein, wenn
beides gesetzt ist. `hard_timeout` ist auch pflicht, wenn ein Task
`lock=...` deklariert.

## Task-Expiry

Für zeitkritische Arbeit deklariere `max_age`, damit abgestandene
Tasks nie laufen:

```python
task_info = TaskInfo(
    name="send_otp",
    queues=["auth"],
    max_age=60,  # Abandon, wenn älter als 60 s bei Pickup
)
```

Bei Pickup, wenn `now - enqueued_at >= max_age`, loggt der Worker
`task_expired` (WARNING, mit `age_s` und `max_age_s`), feuert das
`TASK_EXPIRED`-Signal, routet die Nachricht in die DLQ und ruft
`run()` nicht auf.

## Cancellation

Um einen bereits in-flight (oder noch in der Queue liegenden) Task
abzubrechen:

```python
cancelled = await app.cancel(task_id)
```

Der Aufruf ist Fire-and-forget — er setzt ein Flag in Valkey und
kehrt zurück. Der Cancel-Monitor des Workers picks das Flag auf
und raist `TaskCancelledError` innerhalb der Task-Coroutine. Siehe
[`app.cancel()`](../reference/api.md#cancel) für den
Stale-Flag-Caveat.

## Verifizieren

Plane etwas mit Retries und beobachte die Logs:

```python
import asyncio
from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend
import myapp.tasks  # noqa — registriert den Task

async def main() -> None:
    async with SwarmQ(
        RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
        ValkeyBackend("valkey://localhost:6379/0"),
    ) as app:
        tid = await app.schedule("flaky", delay=2.0)
        print("scheduled", tid)

asyncio.run(main())
```

Der Client loggt beim Einreihen nichts; `scheduled <task_id>` ist
dein eigenes `print`. Nach der Verzögerung von zwei Sekunden zeigt der
Worker-Log für jeden fehlgeschlagenen Versuch eine `task_failed`-Zeile
(mit `error_type` und `error`), gefolgt von `task_retrying` (mit
`retry_count`, `max_retries` und `delay_ms`), mit den konfigurierten
Delays zwischen den Versuchen, und `task_succeeded`, sobald ein
Versuch gelingt. Starte den Worker zusätzlich mit
`--log-lifecycle-level INFO`, um auch `task_received` und
`task_started` für jeden Versuch zu sehen.

## Siehe auch

- [Zustellungssemantik](../explanation/delivery-semantics.md) — was
  at-least-once bei Retries bedeutet
- [Dead-Letter-Queues](dead-letter-queues.md) — was nach
  `max_retries`-Erschöpfung passiert
- [`TaskInfo.retry_*` Referenz](../reference/configuration.md#retries)
- [`Retry` / `NoRetry` API](../reference/api.md#exceptions)
