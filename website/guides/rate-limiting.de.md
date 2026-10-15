# Rate-Limiting

Du hast einen Task, der nicht öfter als N-mal pro Zeiteinheit
laufen darf — ein Webhook-Kontingent, ein Third-Party-API-Plan,
eine Crawler-Höflichkeitsregel. SwarmQs Rate-Limiter ist
deklarativ: Setze einen String auf `TaskInfo`, der Worker setzt
ihn global über jeden Worker durch, der dasselbe Valkey-Backend
teilt.

## Voraussetzungen

- Ein laufendes Valkey-Backend (geteilt über jeden Worker).
- Ein Task, den du limitieren willst.

## Das Limit deklarieren

```python
from swarmq import Task, TaskInfo


class SendSMS(Task):
    task_info = TaskInfo(
        name="send_sms",
        queues=["sms"],
        rate_limit="100/m",
    )

    async def run(self, to: str, body: str) -> None:
        await sms_provider.send(to, body)
```

Das Format ist `"N/unit"` — eine Integer-Anzahl gefolgt von einem
Schrägstrich und einer Zeiteinheit:

| Einheit | Aliase |
|---|---|
| Pro Sekunde | `s` |
| Pro Minute | `m`, `min` |
| Pro Stunde | `h`, `hour` |

Beispiele: `"5/s"`, `"100/m"`, `"1000/h"`. Ein Bad String (falsche
Einheit, fehlende Zahl, negative Anzahl) raist `ValueError` bei
der Klassendefinition.

## Wie das Limit durchgesetzt wird

Das Limit ist ein **2-Perioden-Sliding-Window**, implementiert als
einzelnes Lua-Skript auf Valkey. Jeder Worker, der eine Nachricht
aufnimmt, prüft den Counter atomar:

- **Unter dem Limit** — der Counter wird erhöht, und die Nachricht
  geht weiter zur Lock-Prüfung (falls der Task einen Lock hat) und zu
  `run()`. Der Slot zählt ab hier, auch wenn `run()` nicht folgt
  (siehe [Mit einem Lock](#kombination-mit-retries-und-locks)).
- **Über dem Limit** — der Worker nackt die Nachricht in eine
  Delay-Queue und feuert `TASK_RATE_LIMITED`. Nach der berechneten
  Wartezeit redelivert der Broker, und der Check läuft erneut.

Das Sliding-Window bedeutet, dass ein Burst von 100 bei `T` plus
weiteren 50 bei `T + 30s` unter einem `100/m`-Limit die ersten 100
durchlässt, die restlichen 50 für eine Teil-Fenster-Wartezeit
einreiht und sie freigibt, wenn genug Headroom wieder auftaucht.
Es gibt keine harte Kante bei der Minutenmarke.

## Was du als Konsument siehst

Eine rate-limited Nachricht scheitert nicht. Sie wartet. Der
`TaskInfo.max_age`-Guard ist das Einzige, was eine lange Wartezeit
in eine DLQ-Route verwandelt — ohne ihn werden rate-limited
Nachrichten unbegrenzt retry't.

```python
task_info = TaskInfo(
    name="send_sms",
    queues=["sms"],
    rate_limit="100/m",
    max_age=300,   # Nach 5 min Wartezeit aufgeben
)
```

Ist Valkey bei der Prüfung nicht erreichbar (oder antwortet nicht
innerhalb von `SWARMQ_BACKEND_OP_TIMEOUT`), wird die Nachricht nicht
rate-limitiert, sondern mit eigenem Backoff über die Delay-Queue
zurückgestellt: kein `TASK_RATE_LIMITED`, `rate_limit_count` bleibt
unverändert, und der Worker loggt `task_deferred_backend_unavailable`
(`gate="rate_limit"`). Jeder andere Fehler der Prüfung loggt
`rate_limit_check_failed` (ERROR) und reiht die Nachricht an derselben
Stelle neu ein. Siehe
[Zustellsemantik](../explanation/delivery-semantics.md#was-den-ack-abbricht).

## Global vs Per-Worker

Jeder Worker-Prozess trifft denselben Valkey-Counter, also ist das
Limit **global**, unabhängig von `--processes`, `--concurrency`
oder Anzahl deployter Worker-Pods. Es gibt keinen Per-Worker-Modus.

Brauchst du einen Per-Worker-Cap, nutze `--concurrency` — ein
einzelner Worker-Prozess überschreitet für einen einzelnen Task
nicht seine konfigurierte Concurrency.

## Observability

Hake `Signal.TASK_RATE_LIMITED`, um aufzuzeichnen, wann Limits
greifen. Der Worker löst es aus; unter `swarmq worker` registrierst du
den Handler deshalb in einem `--configure`-Hook (siehe
[Signals](signals.md#einen-handler-registrieren)):

```python title="myapp/hooks.py"
from swarmq import Signal, SwarmQ
from swarmq.logging import get_logger

log = get_logger("myapp.rate_limit")


def setup(app: SwarmQ) -> None:
    @app.on_signal(Signal.TASK_RATE_LIMITED)
    async def on_rate_limit(
        task_id, task_name, retry_after_ms, remaining, rate_limit_count,
        **kwargs,
    ):
        # A message that keeps bouncing means the limit is far below demand.
        if rate_limit_count >= 10:
            log.warning(
                "myapp_rate_limit_bouncing",
                task_id=task_id, task_name=task_name,
                retry_after_ms=retry_after_ms,
                bounce_count=rate_limit_count,
            )
```

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup
```

Payload-Felder:

- `retry_after_ms` — wie lange die Nachricht in der Delay-Queue
  wartet.
- `remaining` — geschätzte verbleibende Budget im aktuellen Fenster.
- `rate_limit_count` — wie oft genau diese Nachricht vom
  Rate-Limiter requeued wurde (ein Bounce-Counter für Operator-
  Alerts).

Um Aufschübe nur zu sehen, brauchst du keinen Handler: Der Worker
loggt jeden selbst als `task_rate_limited`-Zeile (INFO, Logger
`swarmq.worker.gates.rate_limit`) mit `retry_after_ms`, `remaining`
und `rate_limit_count`. Schließe die Parameterliste des Handlers mit
`**kwargs` ab, damit er weiterläuft, wenn ein Release einen Schlüssel
ergänzt.

Wenn `swarmq[metrics]` installiert ist und `--metrics-port`
gesetzt, werden Rate-Limit-Events zusätzlich als Prometheus-Counter
exportiert.

## Das Limit wählen

Starte leicht unter der Upstream-Quota, nicht darauf. Beispiel:
Erlaubt SendGrid 100 req/s, setze `rate_limit="90/s"`. 10 %
Headroom toleriert Clock-Drift und Burst-Smoothing.

Für Limits, die sich mit deinem Plan-Tier ändern, lies das Limit
aus deiner Config und setze es bei `TaskInfo`-Konstruktion — keine
Laufzeit-Mutation, aber ein Restart nimmt den neuen Wert auf.

## Kombination mit Retries und Locks

Rate-Limiting komponiert natürlich mit Retries und Locks:

- **Mit Retries.** Retry-Versuche zählen auch gegen das Limit —
  ein Retry ist ein zweiter `run()` derselben Arbeit.
- **Mit einem Lock.** Zuerst läuft die Rate-Limit-Prüfung; den Lock
  (`lock`, `max_concurrency`) versucht der Worker erst danach. Eine
  rate-limited Nachricht versucht den Lock nie. Umgekehrt hat eine
  Nachricht, die das Rate-Limit passiert und dann den Lock besetzt
  findet, einen Slot verbraucht, obwohl `run()` nicht lief: Sie geht
  mit dem Lock-Backoff zurück (100 ms, verdoppelt bis 60 s) und
  passiert das Rate-Limit bei jeder Neuzustellung erneut. Warten viele
  Kopien auf einen Lock, geht ein großer Teil des Budgets an diese
  Versuche, und es laufen weniger Tasks, als das Limit erlaubt. Lass
  Luft im `rate_limit` oder halte die Zahl der Kopien klein, die um
  einen Lock konkurrieren (etwa mit `unique_for`).

## Verifizieren

```bash
swarmq inspect --module myapp.tasks
```

Die `RATE_LIMIT`-Spalte listet den deklarierten String für jeden
Task. Dann plane einen Burst größer als das Limit und beobachte
die Worker-Logs: einige Tasks beenden sofort (`task_succeeded`),
andere zeigen `task_rate_limited`-Zeilen mit `retry_after_ms` und
erscheinen nach der Verzögerung wieder.

## Siehe auch

- [Signals](signals.md) — Registrierung des
  `TASK_RATE_LIMITED`-Handlers
- [`TaskInfo.rate_limit`-Referenz](../reference/configuration.md#rate-limiting)
- [Locking und Deduplizierung](locking-and-dedup.md) — das
  Kompagnon-Primitiv zum Serialisieren von Ausführung
