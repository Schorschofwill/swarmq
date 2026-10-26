# Error catalog (machine-readable)

Strict tabular form of the [human error catalog](../errors.md).
Every exception that descends from `SwarmQError` appears exactly once,
with stable keys for programmatic classification. The same data, as
generated from the code, is in [`spec/errors.json`](spec/errors.json);
a test keeps the columns `parent` to `dlq_on_uncaught` of this table
equal to it. `exit_code_on_cli` and `transport_field` are worded more
precisely here than in the JSON. Where the exit code depends on the
command (`ConfigurationError`), the JSON keeps the worker's code in
`exit_code_on_cli` and adds `exit_code_by_command`.

## Schema

Every row has the following columns:

| Column | Type | Meaning |
|---|---|---|
| `exception_class` | `string` | Fully-qualified Python class. |
| `parent` | `string` | Direct parent class in the SwarmQ hierarchy. |
| `origin` | `"library" \| "task_code"` | Raised by SwarmQ itself, or by task code as control flow? |
| `when` | `"startup" \| "schedule" \| "run" \| "result" \| "cli"` | Lifecycle stage that can surface this exception. |
| `retriable` | `"yes" \| "no" \| "controlled" \| "n/a"` | `controlled` means "retried only when `retry_exceptions` allows it". `Retry` is `yes`: it retries regardless of `retry_exceptions`, within `max_retries`. |
| `dlq_on_uncaught` | `"yes" \| "no" \| "n/a"` | Does the worker dead-letter the message when this escapes `run()`? |
| `exit_code_on_cli` | `int \| n/a` | CLI exit code when the exception propagates out of `swarmq`. |
| `transport_field` | `string` | Where the exception shows up: the task result, a log line, stderr or the caller. |

## Catalog

| exception_class | parent | origin | when | retriable | dlq_on_uncaught | exit_code_on_cli | transport_field |
|---|---|---|---|---|---|---|---|
| `swarmq.exceptions.SwarmQError` | `Exception` | library | any | controlled | yes | `1` | `error_class` |
| `swarmq.exceptions.ConfigurationError` | `SwarmQError` | library | startup, cli | no | n/a | `78` (`swarmq worker`, `swarmq check`, invalid `SWARMQ_NAMESPACE` in any command), `1` (other errors of `schedule`, `dlq`, `inspect`) | `worker_config_error` log line (worker), `stderr line` (other subcommands) |
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
| `swarmq.exceptions.TaskNotFoundError` | `SwarmQError` | library | schedule | no | n/a | n/a | raised to caller; on the worker `error_class` of the DLQ result after `SWARMQ_UNKNOWN_TASK_GRACE_S` |
| `swarmq.exceptions.LockWaitExceeded` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `error_class`, `is_error=true`; DLQ `error_type` (never raised) |
| `swarmq.exceptions.RedeliveryLimitError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `error_class`, `is_error=true`; DLQ `error_type` |
| `swarmq.exceptions.TaskError` | `SwarmQError` | library | result | n/a (post-mortem) | n/a | n/a | `error_message`, `error_class`, `is_error=true` |
| `swarmq.exceptions.TaskCancelledError` | `TaskError` | library | result | no | no (cancel bypasses DLQ) | n/a | `is_cancelled=true` |
| `swarmq.exceptions.ResultNotReadyError` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.ResultExpiredError` | `SwarmQError` | library | result | no | n/a | n/a | not raised (reserved) |
| `swarmq.exceptions.ResultSerializationError` | `SwarmQError` | library | run | no | yes | n/a | `error_message`, `error_class`, `is_error=true` |
| `swarmq.exceptions.Retry` | `SwarmQError` | task_code | run | yes (consumes budget) | no | n/a | control flow |
| `swarmq.exceptions.NoRetry` | `SwarmQError` | task_code | run | no | yes (immediate) | n/a | control flow |
| `swarmq.exceptions.SoftTimeoutError` | `SwarmQError` | library | run | controlled | yes | n/a | `error_message`, `is_error=true` |
| `swarmq.exceptions.WorkflowValidationError` | `SwarmQError` | library | schedule, run | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowNotFound` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowResultExpired` | `SwarmQError` | library | result | no | n/a | n/a | raised to caller |
| `swarmq.exceptions.WorkflowStateConflict` | `SwarmQError` | library | run | yes | no | n/a | redelivery of the step message |

`ResultExpiredError` is currently never raised; after the result TTL
expires, `get_result` raises `ResultNotReadyError`. `UnroutableMessage`
also appears as `error_class` of the error result of a delayed task
that the scheduler dropped because its queue never appeared.
`PublishCancelledUnsent` (a subclass of `asyncio.CancelledError`, not
of `SwarmQError`) is internal and not exported.

## CLI exit codes

The contract of the `swarmq` binary, separate from the Python
exceptions (full detail in the [CLI reference](../cli.md#exit-codes)):

| Code | Meaning | Raising exception (when applicable) |
|---|---|---|
| `0` | Success or graceful shutdown | none |
| `1` | Runtime error; retry later | `BrokerConnectionError`, `BackendConnectionError`; `ConfigurationError` of `schedule`, `dlq` and `inspect`; every failure of `swarmq worker --check`, argparse errors included; other exceptions while importing the task module (`TypeError`, `ValueError`, E-057) |
| `2` | Bad arguments (argparse), also `dlq retry` with both or neither of `task_id` and `--all` | none (printed by argparse) |
| `3` | `dlq inspect` / `dlq retry <task_id>` found no DLQ entry | `_DLQNotFound` (internal sentinel) |
| `78` | Configuration error (`EX_CONFIG`) of `swarmq worker`, in the parent process or in any worker child (the supervisor propagates it); `swarmq check` when a worker would refuse to start; an invalid `SWARMQ_NAMESPACE` in any command | `ConfigurationError` (also `QueueArgumentsMismatch`, `LoggingConfigError`), `ImportError` (including `ModuleNotFoundError`) or `SyntaxError` of the task module, a failing `--configure` hook |
| `130` | Interrupted by Ctrl-C outside the command's own handling, for example while modules are imported; a running command treats Ctrl-C as a graceful exit with `0` | `KeyboardInterrupt` |

`75` (`EX_TEMPFAIL`) is used between a worker child and the supervisor
only (recycle without acknowledgement) and never reaches the caller.
`71` (`EX_OSERR`) is internal as well: a CPU worker whose silently
restarted forkserver was rejected (`ForkSourceRejected`) exits with it,
and the supervisor restarts it with a fresh forkserver.
Treat `78` as "do not restart without a change" and `1` as "retry with
backoff".

## Classification guidance

When building a consumer for SwarmQ events:

1. **Catch the base class.** `except SwarmQError as exc:` is safe; it
   never includes unrelated Python exceptions.
2. **Branch on concrete classes for recovery.** `TaskNotFoundError`
   is a bug; `BrokerConnectionError` is an outage; `TaskCancelledError`
   is a deliberate user action. Handle them differently.
3. **Never catch `Retry` or `NoRetry` in library code.** They are
   control-flow signals from task code, and the worker relies on
   them escaping.
4. **`TaskError` loses the original class.** By the time it
   reaches `get_result()`, the raising class is only available as
   a name: the advisory `TaskError.error_class` (from the result's
   `error_class`, e.g. `builtins.ValueError`), next to the text in
   `error_message`. It is `None` for results of older workers and
   for failures the worker generates itself (hard timeout, drain,
   lost compute child, expiry, `WorkerHungError`).
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
