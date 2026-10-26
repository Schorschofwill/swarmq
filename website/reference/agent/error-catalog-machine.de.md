# Fehlerkatalog (maschinenlesbar)

Strikt tabellarische Form des
[menschlichen Fehlerkatalogs](../errors.md). Jede Exception, die von
`SwarmQError` erbt, erscheint genau einmal, mit stabilen Schlüsseln
zur programmatischen Klassifizierung. Dieselben Daten, aus dem Code
erzeugt, stehen in [`spec/errors.json`](spec/errors.json); ein Test
hält die Spalten `parent` bis `dlq_on_uncaught` dieser Tabelle gleich.
`exit_code_on_cli` und `transport_field` sind hier genauer formuliert
als im JSON. Wo der Exit-Code vom Befehl abhängt (`ConfigurationError`),
behält das JSON in `exit_code_on_cli` den Code des Workers und ergänzt
`exit_code_by_command`.

## Schema

Jede Zeile hat die folgenden Spalten:

| Spalte | Typ | Bedeutung |
|---|---|---|
| `exception_class` | `string` | Voll qualifizierte Python-Klasse. |
| `parent` | `string` | Direkte Elternklasse in der SwarmQ-Hierarchie. |
| `origin` | `"library" \| "task_code"` | Wirft SwarmQ selbst die Exception, oder Task-Code zur Ablaufsteuerung? |
| `when` | `"startup" \| "schedule" \| "run" \| "result" \| "cli"` | Phase, in der die Exception auftreten kann. |
| `retriable` | `"yes" \| "no" \| "controlled" \| "n/a"` | `controlled` heißt „wiederholt nur, wenn `retry_exceptions` es erlaubt“. `Retry` ist `yes`: Er wiederholt unabhängig von `retry_exceptions`, innerhalb von `max_retries`. |
| `dlq_on_uncaught` | `"yes" \| "no" \| "n/a"` | Dead-lettert der Worker die Nachricht, wenn die Exception aus `run()` entkommt? |
| `exit_code_on_cli` | `int \| n/a` | Exit-Code der CLI, wenn die Exception aus `swarmq` herausläuft. |
| `transport_field` | `string` | Wo die Exception sichtbar wird: im Task-Ergebnis, in einem Logsatz, auf stderr oder beim Aufrufer. |

## Katalog

| exception_class | parent | origin | when | retriable | dlq_on_uncaught | exit_code_on_cli | transport_field |
|---|---|---|---|---|---|---|---|
| `swarmq.exceptions.SwarmQError` | `Exception` | library | any | controlled | yes | `1` | `error_class` |
| `swarmq.exceptions.ConfigurationError` | `SwarmQError` | library | startup, cli | no | n/a | `78` (`swarmq worker`, `swarmq check`, ungültiger `SWARMQ_NAMESPACE` in jedem Befehl), `1` (andere Fehler von `schedule`, `dlq`, `inspect`) | Logsatz `worker_config_error` (Worker), Zeile auf stderr (andere Unterbefehle) |
| `swarmq.exceptions.LoggingConfigError` | `ConfigurationError` | library | startup, cli | no | n/a | `78` | Logsatz `worker_config_error` mit `path` |
| `swarmq.exceptions.QueueArgumentsMismatch` | `ConfigurationError` | library | startup | no | n/a | `78` | Logsatz `worker_config_error` |
| `swarmq.exceptions.BrokerConnectionError` | `SwarmQError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.PublishIndeterminate` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.SettleFailedAfterPublish` | `BrokerConnectionError` | library | run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.MessageHeadersTooLarge` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.UnroutableMessage` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BrokerPublishBlocked` | `BrokerConnectionError` | library | schedule, run | yes | n/a | `1` | `stderr line` |
| `swarmq.exceptions.PublishRejected` | `BrokerConnectionError` | library | schedule, run | yes | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BackendConnectionError` | `SwarmQError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BackendResponseError` | `BackendConnectionError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.TaskNotFoundError` | `SwarmQError` | library | schedule | no | n/a | n/a | an den Aufrufer geworfen; im Worker `error_class` des DLQ-Ergebnisses nach `SWARMQ_UNKNOWN_TASK_GRACE_S` |
| `swarmq.exceptions.LockWaitExceeded` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `error_class`, `is_error=true`; `error_type` in der DLQ (nie geworfen) |
| `swarmq.exceptions.RedeliveryLimitError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `error_class`, `is_error=true`; `error_type` in der DLQ |
| `swarmq.exceptions.TaskError` | `SwarmQError` | library | result | n/a (post-mortem) | n/a | n/a | `error_message`, `error_class`, `is_error=true` |
| `swarmq.exceptions.TaskCancelledError` | `TaskError` | library | result | no | no (Cancel umgeht DLQ) | n/a | `is_cancelled=true` |
| `swarmq.exceptions.ResultNotReadyError` | `SwarmQError` | library | result | no | n/a | n/a | an den Aufrufer geworfen |
| `swarmq.exceptions.ResultExpiredError` | `SwarmQError` | library | result | no | n/a | n/a | nicht geworfen (reserviert) |
| `swarmq.exceptions.ResultSerializationError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `error_class`, `is_error=true` |
| `swarmq.exceptions.Retry` | `SwarmQError` | task_code | run | yes (verbraucht Budget) | no | n/a | Ablaufsteuerung |
| `swarmq.exceptions.NoRetry` | `SwarmQError` | task_code | run | no | yes (sofort) | n/a | Ablaufsteuerung |
| `swarmq.exceptions.SoftTimeoutError` | `SwarmQError` | library | run | controlled | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.WorkflowValidationError` | `SwarmQError` | library | schedule, run | no | n/a | n/a | an den Aufrufer geworfen |
| `swarmq.exceptions.WorkflowNotFound` | `SwarmQError` | library | result | no | n/a | n/a | an den Aufrufer geworfen |
| `swarmq.exceptions.WorkflowResultExpired` | `SwarmQError` | library | result | no | n/a | n/a | an den Aufrufer geworfen |
| `swarmq.exceptions.WorkflowStateConflict` | `SwarmQError` | library | run | yes | no | n/a | Neuzustellung der Schritt-Nachricht |

`ResultExpiredError` wird derzeit nie geworfen; nach Ablauf der
Ergebnis-TTL wirft `get_result` `ResultNotReadyError`.
`UnroutableMessage` steht außerdem als `error_class` im Fehlerergebnis
eines verzögerten Tasks, den der Scheduler verworfen hat, weil seine
Queue nie erschien. `PublishCancelledUnsent` (eine Unterklasse von
`asyncio.CancelledError`, nicht von `SwarmQError`) ist intern und wird
nicht exportiert.

## CLI-Exit-Codes

Der Vertrag des Programms `swarmq`, getrennt von den Python-Exceptions
(alle Einzelheiten in der [CLI-Referenz](../cli.md#exit-codes)):

| Code | Bedeutung | Auslösende Exception (wenn zutreffend) |
|---|---|---|
| `0` | Erfolg oder geordnetes Herunterfahren | keine |
| `1` | Laufzeitfehler; später erneut versuchen | `BrokerConnectionError`, `BackendConnectionError`; `ConfigurationError` von `schedule`, `dlq` und `inspect`; jeder Fehlschlag von `swarmq worker --check`, auch argparse-Fehler; andere Exceptions beim Import des Task-Moduls (`TypeError`, `ValueError`, E-057) |
| `2` | Falsche Argumente (argparse), auch `dlq retry` mit beidem oder keinem von `task_id` und `--all` | keine (argparse gibt die Meldung aus) |
| `3` | `dlq inspect` / `dlq retry <task_id>` fand keinen DLQ-Eintrag | `_DLQNotFound` (interner Sentinel) |
| `78` | Konfigurationsfehler (`EX_CONFIG`) von `swarmq worker`, im Elternprozess oder in einem Worker-Kind (der Supervisor reicht ihn durch); `swarmq check`, wenn ein Worker den Start verweigern würde; ein ungültiger `SWARMQ_NAMESPACE` in jedem Befehl | `ConfigurationError` (auch `QueueArgumentsMismatch`, `LoggingConfigError`), `ImportError` (auch `ModuleNotFoundError`) oder `SyntaxError` des Task-Moduls, ein scheiternder `--configure`-Hook |
| `130` | Durch Strg-C unterbrochen, außerhalb der Behandlung durch den Befehl, etwa beim Import von Modulen; ein laufender Befehl behandelt Strg-C als geordnetes Ende mit `0` | `KeyboardInterrupt` |

`75` (`EX_TEMPFAIL`) wird nur zwischen Worker-Kind und Supervisor
verwendet (Recycling ohne Bestätigung) und erreicht den Aufrufer nie.
Auch `71` (`EX_OSERR`) ist intern: Ein CPU-Worker, dessen still neu
gestarteter Forkserver verworfen wurde (`ForkSourceRejected`), endet
damit, und der Supervisor startet ihn mit frischem Forkserver neu.
`78` heißt „nicht ohne Änderung neu starten", `1` heißt „mit Backoff
erneut versuchen".

## Klassifizierungshinweise

Für einen Konsumenten von SwarmQ-Ereignissen:

1. **Fang die Basisklasse.** `except SwarmQError as exc:` ist sicher;
   es erfasst nie fremde Python-Exceptions.
2. **Verzweige für die Behebung nach konkreten Klassen.**
   `TaskNotFoundError` ist ein Bug, `BrokerConnectionError` ein
   Ausfall, `TaskCancelledError` eine bewusste Aktion eines Nutzers.
   Behandle sie unterschiedlich.
3. **Fang `Retry` oder `NoRetry` nie in Bibliothekscode.** Sie steuern
   den Ablauf aus dem Task-Code, und der Worker verlässt sich darauf,
   dass sie herauslaufen.
4. **`TaskError` verliert die ursprüngliche Klasse.** Bei
   `get_result()` ist die werfende Klasse nur noch als Name da:
   beratend in `TaskError.error_class` (aus `error_class` des
   Ergebnisses, etwa `builtins.ValueError`), neben dem Text in
   `error_message`. Bei Ergebnissen älterer Worker und bei Fehlern,
   die der Worker selbst erzeugt (harter Timeout, Drain, verlorenes
   Rechenkind, Ablauf, `WorkerHungError`), ist es `None`.
5. **`ResultSerializationError` ist ein Programmierfehler.** Der
   Task-Body hat etwas zurückgegeben, das msgpack nicht kodieren kann.
   Korrigiere den Task, statt ihn zu wiederholen.

## Minimale Entscheidungsmatrix

Für Agenten, die eine schnelle Aktionstabelle brauchen:

| Beobachtet | Aktion |
|---|---|
| `BrokerConnectionError` / `BackendConnectionError` | Mit Backoff erneut versuchen; die Bibliothek verbindet sich selbst neu. |
| `PublishIndeterminate` | Nicht blind neu veröffentlichen: Der Broker kann die Nachricht schon haben. Erst das Ergebnis prüfen oder den Task idempotent machen. |
| `MessageHeadersTooLarge` | Nicht mit denselben Headern wiederholen: Es wurde nichts gesendet, und sie scheitern wieder. Die Header verkleinern. |
| `UnroutableMessage` | Nicht wiederholen, bevor die Queue existiert: den Worker starten (oder korrigieren), der sie deklariert, dann erneut veröffentlichen. Nichts wurde zugestellt. |
| `BrokerPublishBlocked` | Der Broker hat das Publizieren blockiert (Speicher- oder Plattenalarm); nichts wurde gesendet. Später erneut versuchen; Speicher oder Platte am Broker freigeben. |
| `PublishRejected` | Die Queue hat die Nachricht abgelehnt (voll mit `reject-publish` oder Quorum-Queue ohne Quorum); nichts wurde gespeichert. Queue reparieren, dann erneut versuchen. |
| `LockWaitExceeded` (`error_type` in der DLQ) | Die Wartezeit auf einen Task-Lock endete (hängender Halter oder `SWARMQ_LOCK_WAIT_MAX_S`). Den in `x-swarmq-lock-name` genannten Halter prüfen, einen festgefahrenen Lock per `DEL` entfernen, dann `swarmq dlq retry`. |
| `RedeliveryLimitError` (`error_type` in der DLQ) | Der Worker hat die Zustellung bis zum Zustelllimit zurückgegeben. Die Ursache im Log vor dem DLQ-Eintrag beheben, dann `swarmq dlq retry`. |
| `BackendResponseError` | Valkey hat mit einem Fehler geantwortet (`WRONGTYPE`, Skriptfehler); Warten hilft nicht. Schlüssel oder Skript prüfen, dann erneut versuchen. |
| `ConfigurationError` | Nicht wiederholen. Konfiguration korrigieren und neu starten. |
| `QueueArgumentsMismatch` | Nicht unverändert neu starten (Exit `78`). Vorherige Einstellung wiederherstellen oder die Queue migrieren. |
| `TaskNotFoundError` bei `schedule()` | Bug melden; der Task ist nicht registriert. |
| `TaskError` bei `get_result()` | Nach `TaskError.error_class` (beratend) klassifizieren, sonst nach `error_message`. Retry hilft je nach Upstream-Ursache. |
| `TaskCancelledError` bei `get_result()` | Ein Nutzer oder Operator hat abgebrochen. Nicht wiederholen, außer der Abbruch war erkennbar ein Versehen. |
| `ResultNotReadyError` bei `get_result()` | Timeout erhöhen, oder der Task hängt wirklich. Worker-Logs und DLQ prüfen. |
| CLI-Exit `3` | Task nicht in der DLQ. Die Nachricht wurde schon wiederholt, gelöscht oder ist nie gescheitert. |

## Siehe auch

- [Nachrichtenformate](message-formats.md) — die Envelope-Felder,
  auf die dieser Katalog verweist
- [Entscheidungsbäume → Wann geht eine Nachricht in die DLQ?](decision-trees.md#wann-geht-eine-nachricht-in-die-dlq)
- [Fehlerkatalog (menschlich)](../errors.md) — die Prosa-Fassung
