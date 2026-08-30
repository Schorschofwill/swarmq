# Error catalog (machine-readable)

Strict tabular form of the [human error catalog](../errors.md).
Every exception that descends from `SwarmQError` appears once with
stable keys suitable for programmatic classification.

## Schema

Every row has the following columns:

| Column | Type | Meaning |
|---|---|---|
| `exception_class` | `string` | Fully-qualified Python class. |
| `parent` | `string` | Direct parent class in the SwarmQ hierarchy. |
| `origin` | `"library" \| "task_code"` | Is the exception raised by SwarmQ itself or by user task code (control flow)? |
| `when` | `"startup" \| "schedule" \| "run" \| "result" \| "cli"` | Lifecycle stage that can surface this exception. |
| `retriable` | `"yes" \| "no" \| "controlled"` | `controlled` means "retriable only when `retry_exceptions` lists it". |
| `dlq_on_uncaught` | `"yes" \| "no"` | Does the worker route to DLQ when this escapes `run()` unchecked? |
| `exit_code_on_cli` | `int` | CLI exit code when the exception propagates out of `swarmq` (if applicable). |
| `transport_field` | `string \| null` | Where the exception appears in the task-result envelope or CLI JSON output. |

## Catalog

| exception_class | parent | origin | when | retriable | dlq_on_uncaught | exit_code_on_cli | transport_field |
|---|---|---|---|---|---|---|---|
| `swarmq.exceptions.SwarmQError` | `Exception` | library | any | controlled | yes | `1` | `error_class` |
| `swarmq.exceptions.ConfigurationError` | `SwarmQError` | library | startup, cli | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BrokerConnectionError` | `SwarmQError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BackendConnectionError` | `SwarmQError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.TaskNotFoundError` | `SwarmQError` | library | schedule | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.TaskError` | `SwarmQError` | library | result | n/a (post-mortem) | n/a | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.TaskCancelledError` | `TaskError` | library | run, result | no | no (cancel bypasses DLQ) | n/a | `is_cancelled=true` |
| `swarmq.exceptions.ResultNotReadyError` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.ResultExpiredError` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.ResultSerializationError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.Retry` | `SwarmQError` | task_code | run | yes (consumes budget) | no | n/a | control flow |
| `swarmq.exceptions.NoRetry` | `SwarmQError` | task_code | run | no | yes (immediate) | n/a | control flow |
| `swarmq.exceptions.SoftTimeoutError` | `SwarmQError` | library | run | controlled | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.WorkflowValidationError` | `SwarmQError` | library | schedule, run | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowNotFound` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowResultExpired` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |

## CLI exit codes

Separate from the Python exception catalog — these cover the `swarmq`
binary's contract:

| Code | Meaning | Raising exception (when applicable) |
|---|---|---|
| `0` | Success or graceful shutdown | — |
| `1` | Runtime error | `ConfigurationError`, `BrokerConnectionError`, `BackendConnectionError`, `ModuleNotFoundError` |
| `2` | Bad arguments (argparse) | — (printed by argparse) |
| `3` | `dlq inspect` / `dlq retry <task_id>` not found | `_DLQNotFound` (internal sentinel) |

## Classification guidance

When building a consumer for SwarmQ events:

1. **Always catch the base class.** `except SwarmQError as exc:`
   is safe — it never includes unrelated Python exceptions.
2. **Branch on concrete classes for recovery.** `TaskNotFoundError`
   is a bug; `BrokerConnectionError` is an outage; `TaskCancelledError`
   is a deliberate user action. Handle them differently.
3. **Never catch `Retry` or `NoRetry` in library code.** They are
   control-flow signals from task code — the worker relies on
   them escaping.
4. **`TaskError` loses the original class.** By the time it
   reaches `get_result()`, the raising class is only available as
   a string in `error_message` (and advisory `error_class`).
5. **`ResultSerializationError` is a programmer error.** It
   means the task body returned something msgpack cannot encode.
   Fix the task, do not retry.

## Minimal decision matrix

For agents that need a quick action table:

| Observed | Action |
|---|---|
| `BrokerConnectionError` / `BackendConnectionError` | Back off, retry — the library reconnects automatically. |
| `ConfigurationError` | Do not retry. Fix the config and restart. |
| `TaskNotFoundError` at `schedule()` | Report a bug; the task is not registered. |
| `TaskError` at `get_result()` | Classify by `error_message`. Retry may or may not help depending on upstream cause. |
| `TaskCancelledError` at `get_result()` | User/operator cancelled. Do not retry unless you know the cancel was spurious. |
| `ResultNotReadyError` at `get_result()` | Increase timeout, or the task is genuinely stuck. Check worker logs / DLQ. |
| CLI exit `3` | Task not in DLQ. Message was already retried, purged, or never failed. |

## See also

- [Message formats](message-formats.md) — the envelope fields this
  catalog references
- [Decision trees → When does a message go to the DLQ?](decision-trees.md#when-does-a-message-go-to-the-dlq)
- [Error catalog (human)](../errors.md) — prose-first version
