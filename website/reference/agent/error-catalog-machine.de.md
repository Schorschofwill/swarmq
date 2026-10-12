# Fehlerkatalog (maschinenlesbar)

Strikt-tabellarische Form des
[menschlichen Fehlerkatalogs](../errors.md). Jede Exception, die
von `SwarmQError` erbt, erscheint einmal mit stabilen Keys,
geeignet für programmatische Klassifizierung.

## Schema

Jede Zeile hat die folgenden Spalten:

| Spalte | Typ | Bedeutung |
|---|---|---|
| `exception_class` | `string` | Voll qualifizierte Python-Klasse. |
| `parent` | `string` | Direkte Parent-Klasse in der SwarmQ-Hierarchie. |
| `origin` | `"library" \| "task_code"` | Wird die Exception von SwarmQ selbst oder von User-Task-Code (Control Flow) geraist? |
| `when` | `"startup" \| "schedule" \| "run" \| "result" \| "cli"` | Lifecycle-Stage, der diese Exception auslösen kann. |
| `retriable` | `"yes" \| "no" \| "controlled"` | `controlled` bedeutet „retriable nur, wenn `retry_exceptions` ihn listet". `Retry` ist `yes`: Er wiederholt unabhängig von `retry_exceptions`, innerhalb von `max_retries`. |
| `dlq_on_uncaught` | `"yes" \| "no"` | Routet der Worker in die DLQ, wenn das ungecheckt aus `run()` entkommt? |
| `exit_code_on_cli` | `int` | CLI-Exit-Code, wenn die Exception aus `swarmq` propagiert (falls anwendbar). |
| `transport_field` | `string \| null` | Wo die Exception im Task-Result-Envelope oder CLI-JSON-Output erscheint. |

## Katalog

| exception_class | parent | origin | when | retriable | dlq_on_uncaught | exit_code_on_cli | transport_field |
|---|---|---|---|---|---|---|---|
| `swarmq.exceptions.SwarmQError` | `Exception` | library | any | controlled | yes | `1` | `error_class` |
| `swarmq.exceptions.ConfigurationError` | `SwarmQError` | library | startup, cli | no | n/a | `78` (`swarmq worker`), `1` (andere Subcommands) | Logsatz `worker_config_error` (Worker), `stderr line` (andere Subcommands) |
| `swarmq.exceptions.LoggingConfigError` | `ConfigurationError` | library | startup, cli | no | n/a | `78` | Logsatz `worker_config_error` mit `path` |
| `swarmq.exceptions.BrokerConnectionError` | `SwarmQError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.PublishIndeterminate` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.SettleFailedAfterPublish` | `BrokerConnectionError` | library | run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.MessageHeadersTooLarge` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.UnroutableMessage` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BackendConnectionError` | `SwarmQError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BackendResponseError` | `BackendConnectionError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.TaskNotFoundError` | `SwarmQError` | library | schedule | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.LockWaitExceeded` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.RedeliveryLimitError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.TaskError` | `SwarmQError` | library | result | n/a (post-mortem) | n/a | n/a | `error_message`, `error_class`, `is_error=true` |
| `swarmq.exceptions.TaskCancelledError` | `TaskError` | library | run, result | no | no (Cancel umgeht DLQ) | n/a | `is_cancelled=true` |
| `swarmq.exceptions.ResultNotReadyError` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.ResultExpiredError` | `SwarmQError` | library | result | no | n/a | n/a | not raised (reserved) |
| `swarmq.exceptions.ResultSerializationError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.Retry` | `SwarmQError` | task_code | run | yes (verbraucht Budget) | no | n/a | Control Flow |
| `swarmq.exceptions.NoRetry` | `SwarmQError` | task_code | run | no | yes (sofort) | n/a | Control Flow |
| `swarmq.exceptions.SoftTimeoutError` | `SwarmQError` | library | run | controlled | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.WorkflowValidationError` | `SwarmQError` | library | schedule, run | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowNotFound` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowResultExpired` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowStateConflict` | `SwarmQError` | library | run | yes | no | n/a | Neuzustellung der Schritt-Nachricht |

`ResultExpiredError` wird derzeit nie geworfen; nach Ablauf der
Ergebnis-TTL wirft `get_result` `ResultNotReadyError`.

## CLI-Exit-Codes

Separat vom Python-Exception-Katalog — diese decken den Vertrag
des `swarmq`-Binaries ab:

| Code | Bedeutung | Raising Exception (wenn zutreffend) |
|---|---|---|
| `0` | Erfolg oder Graceful Shutdown | — |
| `1` | Laufzeitfehler | `BrokerConnectionError`, `BackendConnectionError`; `ConfigurationError` außerhalb von `swarmq worker` |
| `2` | Schlechte Argumente (argparse) | — (von argparse gedruckt) |
| `3` | `dlq inspect` / `dlq retry <task_id>` nicht gefunden | `_DLQNotFound` (interner Sentinel) |
| `78` | Konfigurationsfehler von `swarmq worker` (`EX_CONFIG`) — im Elternprozess oder in einem beliebigen Worker-Kind; der Supervisor reicht ihn durch | `ConfigurationError`, `ModuleNotFoundError`, `ImportError` des Task-Moduls, ein werfender `--configure`-Hook |
| `130` | Durch Strg-C unterbrochen, außerhalb der Behandlung durch den Befehl selbst | `KeyboardInterrupt` |

`75` (`EX_TEMPFAIL`) wird nur zwischen Worker-Kind und Supervisor
verwendet (Recycling ohne Bestätigung) und erreicht den Aufrufer nie.
Auch `71` (`EX_OSERR`) ist intern: Ein CPU-Worker, dessen still neu
gestarteter Forkserver verworfen wurde (`ForkSourceRejected`), endet
damit, und der Supervisor startet ihn mit frischem Forkserver neu.
`78` heißt „nicht ohne Änderung neu starten", `1` heißt „mit Backoff
erneut versuchen".

## Klassifizierungshinweise

Beim Bau eines Konsumenten für SwarmQ-Events:

1. **Fang immer die Basisklasse.** `except SwarmQError as exc:`
   ist safe — es enthält nie unbeteiligte Python-Exceptions.
2. **Verzweige auf konkreten Klassen für Recovery.**
   `TaskNotFoundError` ist ein Bug; `BrokerConnectionError` ist
   ein Ausfall; `TaskCancelledError` ist eine bewusste
   User-Aktion. Behandle sie unterschiedlich.
3. **Fang nie `Retry` oder `NoRetry` in Library-Code.** Sie sind
   Control-Flow-Signale aus Task-Code — der Worker verlässt sich
   darauf, dass sie entkommen.
4. **`TaskError` verliert die Original-Klasse.** Wenn sie
   `get_result()` erreicht, ist die raisende Klasse nur als Name
   verfügbar: beratend in `TaskError.error_class` (aus
   `error_class` des Ergebnisses, z. B. `builtins.ValueError`),
   neben dem Text in `error_message`. Bei Ergebnissen älterer
   Worker und bei Fehlern, die der Worker selbst erzeugt (harter
   Timeout, Drain, verlorenes Rechenkind, Ablauf), ist es `None`.
5. **`ResultSerializationError` ist ein Programmiererfehler.** Er
   bedeutet, der Task-Body gab etwas zurück, das msgpack nicht
   kodieren kann. Fixe den Task, retry ihn nicht.

## Minimale Entscheidungsmatrix

Für Agenten, die eine schnelle Aktionstabelle brauchen:

| Beobachtet | Aktion |
|---|---|
| `BrokerConnectionError` / `BackendConnectionError` | Back-off, Retry — die Library reconnected automatisch. |
| `PublishIndeterminate` | Nicht blind neu veröffentlichen: Der Broker kann die Nachricht schon haben. Erst das Ergebnis prüfen oder den Task idempotent machen. |
| `MessageHeadersTooLarge` | Nicht mit denselben Headern wiederholen: Es wurde nichts gesendet, und sie scheitern wieder. Die Header verkleinern. |
| `UnroutableMessage` | Nicht wiederholen, bevor die Queue existiert: den Worker starten (oder korrigieren), der sie deklariert, dann erneut veröffentlichen. Nichts wurde zugestellt. |
| `LockWaitExceeded` (`error_type` in der DLQ) | Die Wartezeit auf einen Task-Lock endete (hängender Halter oder `SWARMQ_LOCK_WAIT_MAX_S`). Den in `x-swarmq-lock-name` genannten Halter prüfen, einen festgefahrenen Lock per `DEL` entfernen, dann `swarmq dlq retry`. |
| `RedeliveryLimitError` (`error_type` in der DLQ) | Der Worker hat die Zustellung bis zum Zustelllimit zurückgegeben. Die Ursache im Log vor dem DLQ-Eintrag beheben, dann `swarmq dlq retry`. |
| `BackendResponseError` | Valkey hat mit einem Fehler geantwortet (`WRONGTYPE`, Skriptfehler); Warten hilft nicht. Schlüssel oder Skript prüfen, dann erneut versuchen. |
| `ConfigurationError` | Nicht retry'n. Config fixen und neustarten. |
| `TaskNotFoundError` bei `schedule()` | Bug melden; der Task ist nicht registriert. |
| `TaskError` bei `get_result()` | Nach `TaskError.error_class` (beratend) klassifizieren, sonst nach `error_message`. Retry hilft je nach Upstream-Ursache. |
| `TaskCancelledError` bei `get_result()` | User/Operator hat abgebrochen. Nicht retry'n, außer du weißt, dass der Cancel versehentlich war. |
| `ResultNotReadyError` bei `get_result()` | Timeout erhöhen oder der Task hängt wirklich. Worker-Logs / DLQ prüfen. |
| CLI-Exit `3` | Task nicht in DLQ. Nachricht wurde bereits retry't, gepurged oder ist nie gescheitert. |

## Siehe auch

- [Nachrichtenformate](message-formats.md) — die Envelope-Felder,
  die dieser Katalog referenziert
- [Entscheidungsbäume → Wann geht eine Nachricht in die DLQ?](decision-trees.md#wann-geht-eine-nachricht-in-die-dlq)
- [Fehlerkatalog (menschlich)](../errors.md) — prosa-erste Version
