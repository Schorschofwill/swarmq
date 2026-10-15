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
| `retriable` | `"yes" \| "no" \| "controlled"` | `controlled` means "retriable only when `retry_exceptions` lists it". `Retry` is `yes`: it retries regardless of `retry_exceptions`, within `max_retries`. |
| `dlq_on_uncaught` | `"yes" \| "no"` | Does the worker route to DLQ when this escapes `run()` unchecked? |
| `exit_code_on_cli` | `int` | CLI exit code when the exception propagates out of `swarmq` (if applicable). |
| `transport_field` | `string \| null` | Where the exception appears in the task-result envelope or CLI JSON output. |

## Catalog

| exception_class | parent | origin | when | retriable | dlq_on_uncaught | exit_code_on_cli | transport_field |
|---|---|---|---|---|---|---|---|
| `swarmq.exceptions.SwarmQError` | `Exception` | library | any | controlled | yes | `1` | `error_class` |
| `swarmq.exceptions.ConfigurationError` | `SwarmQError` | library | startup, cli | no | n/a | `78` (`swarmq worker`), `1` (other subcommands) | `worker_config_error` log line (worker), `stderr line` (other subcommands) |
| `swarmq.exceptions.LoggingConfigError` | `ConfigurationError` | library | startup, cli | no | n/a | `78` | `worker_config_error` log line with `path` |
| `swarmq.exceptions.QueueArgumentsMismatch` | `ConfigurationError` | library | startup | no | n/a | `78` | `worker_config_error` log line |
| `swarmq.exceptions.BrokerConnectionError` | `SwarmQError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.PublishIndeterminate` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.SettleFailedAfterPublish` | `BrokerConnectionError` | library | run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.MessageHeadersTooLarge` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.UnroutableMessage` | `BrokerConnectionError` | library | schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BrokerPublishBlocked` | `BrokerConnectionError` | library | schedule, run | yes | n/a | `1` | `stderr line` |
| `swarmq.exceptions.PublishRejected` | `BrokerConnectionError` | library | schedule, run | yes | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BackendConnectionError` | `SwarmQError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.BackendResponseError` | `BackendConnectionError` | library | startup, schedule, run | no | n/a | `1` | `stderr line` |
| `swarmq.exceptions.TaskNotFoundError` | `SwarmQError` | library | schedule | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.LockWaitExceeded` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.RedeliveryLimitError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.TaskError` | `SwarmQError` | library | result | n/a (post-mortem) | n/a | n/a | `error_message`, `error_class`, `is_error=true` |
| `swarmq.exceptions.TaskCancelledError` | `TaskError` | library | result | no | no (cancel bypasses DLQ) | n/a | `is_cancelled=true` |
| `swarmq.exceptions.ResultNotReadyError` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.ResultExpiredError` | `SwarmQError` | library | result | no | n/a | n/a | not raised (reserved) |
| `swarmq.exceptions.ResultSerializationError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.Retry` | `SwarmQError` | task_code | run | yes (consumes budget) | no | n/a | control flow |
| `swarmq.exceptions.NoRetry` | `SwarmQError` | task_code | run | no | yes (immediate) | n/a | control flow |
| `swarmq.exceptions.SoftTimeoutError` | `SwarmQError` | library | run | controlled | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.WorkflowValidationError` | `SwarmQError` | library | schedule, run | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowNotFound` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowResultExpired` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowStateConflict` | `SwarmQError` | library | run | yes | no | n/a | redelivery of the step message |

`ResultExpiredError` is currently never raised; after the result TTL
expires, `get_result` raises `ResultNotReadyError`.

## CLI exit codes

Separate from the Python exception catalog — these cover the `swarmq`
binary's contract:

| Code | Meaning | Raising exception (when applicable) |
|---|---|---|
| `0` | Success or graceful shutdown | — |
| `1` | Runtime error | `BrokerConnectionError`, `BackendConnectionError`; `ConfigurationError` outside `swarmq worker` |
| `2` | Bad arguments (argparse) | — (printed by argparse) |
| `3` | `dlq inspect` / `dlq retry <task_id>` not found | `_DLQNotFound` (internal sentinel) |
| `78` | Configuration error of `swarmq worker` (`EX_CONFIG`) — in the parent process or in any worker child; the supervisor propagates it | `ConfigurationError`, `ModuleNotFoundError`, `ImportError` or `SyntaxError` of the task module, a raising `--configure` hook |
| `130` | Interrupted by Ctrl-C outside the command's own handling | `KeyboardInterrupt` |

`75` (`EX_TEMPFAIL`) is used between a worker child and the supervisor
only (recycle without acknowledgement) and never reaches the caller.
`71` (`EX_OSERR`) is internal as well: a CPU worker whose silently
restarted forkserver was rejected (`ForkSourceRejected`) exits with it,
and the supervisor restarts it with a fresh forkserver.
Treat `78` as "do not restart without a change" and `1` as "retry with
backoff".

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
   a name: the advisory `TaskError.error_class` (from the result's
   `error_class`, e.g. `builtins.ValueError`), next to the text in
   `error_message`. It is `None` for results of older workers and
   for failures the worker generates itself (hard timeout, drain,
   lost compute child, expiry).
5. **`ResultSerializationError` is a programmer error.** It
   means the task body returned something msgpack cannot encode.
   Fix the task, do not retry.

## Minimal decision matrix

For agents that need a quick action table:

| Observed | Action |
|---|---|
| `BrokerConnectionError` / `BackendConnectionError` | Back off, retry — the library reconnects automatically. |
| `PublishIndeterminate` | Do not blindly re-publish: the broker may already have the message. Check the result or make the task idempotent first. |
| `MessageHeadersTooLarge` | Do not retry with the same headers: nothing was sent, and they fail again. Reduce the headers. |
| `UnroutableMessage` | Do not retry until the queue exists: start (or fix) the worker that declares it, then publish again. Nothing was delivered. |
| `BrokerPublishBlocked` | The broker blocked publishing (memory or disk alarm); nothing was sent. Retry later; free memory or disk on the broker. |
| `PublishRejected` | The queue refused the message (full with `reject-publish`, or a quorum queue without quorum); nothing was stored. Fix the queue, then retry. |
| `LockWaitExceeded` (DLQ `error_type`) | The wait for a task lock ended (stalled holder or `SWARMQ_LOCK_WAIT_MAX_S`). Check the holder named by `x-swarmq-lock-name`, `DEL` a wedged lock, then `swarmq dlq retry`. |
| `RedeliveryLimitError` (DLQ `error_type`) | The worker handed the delivery back until the delivery limit. Fix the cause in the log before the DLQ entry, then `swarmq dlq retry`. |
| `BackendResponseError` | Valkey answered with an error (`WRONGTYPE`, script error); waiting does not help. Inspect the key or script, then retry. |
| `ConfigurationError` | Do not retry. Fix the config and restart. |
| `QueueArgumentsMismatch` | Do not restart unchanged (exit `78`). Restore the previous setting or migrate the queue. |
| `TaskNotFoundError` at `schedule()` | Report a bug; the task is not registered. |
| `TaskError` at `get_result()` | Classify by `TaskError.error_class` (advisory), else by `error_message`. Retry may or may not help depending on upstream cause. |
| `TaskCancelledError` at `get_result()` | User/operator cancelled. Do not retry unless you know the cancel was spurious. |
| `ResultNotReadyError` at `get_result()` | Increase timeout, or the task is genuinely stuck. Check worker logs / DLQ. |
| CLI exit `3` | Task not in DLQ. Message was already retried, purged, or never failed. |

## See also

- [Message formats](message-formats.md) — the envelope fields this
  catalog references
- [Decision trees → When does a message go to the DLQ?](decision-trees.md#when-does-a-message-go-to-the-dlq)
- [Error catalog (human)](../errors.md) — prose-first version
