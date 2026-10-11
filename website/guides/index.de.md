# How-to-Guides

Zielorientierte Rezepte für häufige Aufgaben. Jeder Guide
beantwortet eine Frage — *„Wie mache ich X?"* — mit den Schritten,
einem Code-Snippet und einem Verifikationshinweis. Lies den, den du
brauchst; überspringe den Rest.

Nutze die Guides, wenn du bereits weißt, was du erreichen willst.
Für einen strukturierten Walkthrough von Grund auf siehe die
[Tutorials](../tutorials/index.md). Für erschöpfende Parameter-
Listen siehe die [Referenz](../reference/index.md).

## Task-Definition und Scheduling

- [Tasks definieren](defining-tasks.md) — die `Task`-Subklassen-Form,
  erlaubte Argument-Typen, Lifecycle-Hooks und die Anti-Pattern, die
  in Produktion beißen.
- [Scheduling und Retries](scheduling-and-retries.md) — immediate vs
  deferred (`delay`, `eta`), Cron-Schedules, Retry-Backoff,
  `Retry` / `NoRetry`, Timeouts, `max_age`.

## Querschnittsthemen

- [Middleware](middleware.md) — sechs Hooks um jedes
  Task-Lifecycle-Event.
- [Signals](signals.md) — registriere async-Handler auf spezifischen
  Lifecycle-Events (`TASK_FAILED`, `TASK_RATE_LIMITED` etc.).
- [Rate-Limiting](rate-limiting.md) — globale Sliding-Window-Limits,
  auf `TaskInfo` als `"100/m"` deklariert.
- [Locking und Deduplizierung](locking-and-dedup.md) — Ausführungen
  serialisieren (Lock) oder redundante Enqueues zusammenklappen
  (Unique Task).
- [Workflows](workflows.md) — Tasks zu `chain`, `group` und `chord`
  zusammensetzen, mit `app.apply()` starten und über ein
  `WorkflowHandle` abwarten, inspizieren oder abbrechen.

## Operations

- [Ausrollen und Upgraden](deployment.md) — Rollout-Reihenfolge,
  Grace-Periods, vhost und Namensraum, Rechte und Policies,
  Rollback-Regeln; der Upgrade-Pfad von 0.11 auf 0.12.
- [Dead-Letter-Queues](dead-letter-queues.md) — DLQ'te Nachrichten
  mit `swarmq dlq` inspizieren, retryen und purgen.
- [CPU-Modus](cpu-mode.md) — `--pool cpu` führt Task-Körper in
  Rechenkindern mit echter Killbarkeit aus; Dimensionierung, Recycling
  und der Supervisor, der jetzt für jeden Worker Standard ist.
- [Hot Reload (nur Dev)](hot-reload.md) — `--reload` für den
  lokalen Developer-Loop; niemals für Produktion.
- [Tasks testen](testing-tasks.md) — `Task.run()` unit-testen ohne
  Broker; Integration-Tests mit vollem Stack.
