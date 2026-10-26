# Hot reload (development only)

`--reload` watches your task modules and restarts the workers whenever
you save a change. It reuses the supervisor's `SIGHUP` reload: the
supervisor drains the running tasks and re-executes itself with
`os.execvpe`, so the new code is loaded without restarting the
container.

!!! warning "Not a production feature"
    Hot reload is meant for the local development loop. Every worker
    started with `--reload` logs a `hot_reload_enabled` WARNING. If that
    line shows up in your production logs, the flag was left on by
    mistake. SwarmQ does not enforce this; the operator decides.

## Quick start

```bash
pip install 'swarmq[reload]'
swarmq worker --module myapp.tasks --reload
```

Edit `myapp/tasks.py`, save, and watch the log:

```text
reload_triggered changed_count=1 changed_sample=['.../myapp/tasks.py']
process_manager_reload_requested
process_manager_reload_execvp
```

About two seconds later a fresh supervisor with the new code is running.

## What gets watched

By default the watched directory is the **parent directory of the
`--module` source file**:

- `--module myapp.tasks` (file `myapp/tasks.py`) → watches `myapp/`.
- `--module myapp` (package with `__init__.py`) → watches `myapp/`.
- A namespace package (no `__init__.py`) → **error**, pass
  `--watch-dir`.

Override it with one or more `--watch-dir PATH` flags:

```bash
swarmq worker --module myapp.tasks --reload \
  --watch-dir ./tasks --watch-dir ./plugins
```

Watching is recursive. If the detected directory lies inside the
virtualenv or the standard library, the supervisor logs the WARNING
`watch_dir_inside_venv_or_stdlib`: every `pip install` would then
trigger reloads, so pass `--watch-dir` instead. `--watch-dir` without
`--reload` is ignored with a WARNING.

## What triggers a reload

Only changes to Python source files (`.py`, `.pyx`, `.pyd`) trigger a
reload; changes to configuration, templates or data files do not. Of
those, the filter (`watchfiles.PythonFilter` plus a SwarmQ extension)
also ignores:

- `__pycache__/`, `.git/`, `.hg/`, `.svn/`, `.tox/`, `.venv/`,
  `.idea/`, `node_modules/`, `.mypy_cache/`, `.pytest_cache/`,
  `.hypothesis/`
- editor files: `*.swp`, `*.swx`, `*~`, `.#*`, and `4913` (Vim's probe
  file)
- `*.tmp` and `*.bak`

To restart after a non-Python change, save a task module or send
`SIGHUP` to the supervisor.

## How it works

| Phase | Mechanism |
|---|---|
| Detect | A daemon thread runs `watchfiles.watch(...)`. Changes are collected over a 1.6 s debounce window, so format-on-save bursts trigger one reload. |
| Trigger | After the window the thread sends `SIGHUP` to its own process (the supervisor). |
| Reload | The `SIGHUP` handler only records the request; the supervisor's monitor loop (every 0.5 s) carries it out. It stops the workers within the shutdown budget, flushes its log queue within `log_drain_timeout` and re-executes `sys.executable` with the argv and environment captured at startup. Changes to `sys.argv` or `os.environ` at runtime cannot leak into the new process. The argv comes from `sys.orig_argv`, so interpreter flags and `python -m swarmq.cli worker …` survive the reload. |

A change that arrives while a reload is pending, being checked or in
progress is discarded; the running reload picks up the newer code
anyway. A crashed watcher thread logs `watch_thread_crashed` and stops;
the supervisor keeps running without reload until its next restart.

### Import errors keep the supervisor alive

Without `--reload`, an `ImportError` or `SyntaxError` in the task module
ends `swarmq worker` with exit `78` after one attempt. With `--reload`
the supervisor stays up, starts no workers and waits: fix the file,
save, and the next reload starts the workers again. Other exceptions at
module level still crash the worker and count against the restart limit.

### Reloading with `--log-config`

With `--log-config`, the supervisor validates the logging config file
again before it stops any worker. The check runs `dictConfig` in a
separate interpreter in its own process group while supervision,
autoscaling and the control channel carry on; several `SIGHUP`s during
a check lead to one check. The outcome:

- **valid:** the reload proceeds;
- **invalid since the start:** the reload is aborted with a
  `log_config_reload_failed` ERROR (fields `path`, `error_type`,
  `error`, `hint`);
- **not finished after 30 seconds**, for example because a handler
  blocks on its target (syslog over TCP): the reload is aborted with a
  `log_config_reload_probe_timeout` ERROR (fields `path`, `timeout_s`,
  `hint`). This does not mean the file is invalid.

When aborted, workers and logging keep running unchanged. Fix the file
or its target and save a task module again (or send `SIGHUP`) to retry.

### `SIGTERM` always wins

`SIGTERM` and `SIGINT` win over a reload: during a check, while the
workers stop for a reload, and when a `SIGHUP` arrives right after the
`SIGTERM`. The supervisor then shuts down without re-executing itself
and kills a running check. A `SIGTERM` that arrives in the last moment
before `os.execvpe` stays pending and ends the new supervisor as soon as
it has set up its signal handlers.

## Interaction with other worker flags

- **`--processes`:** every `swarmq worker` runs under the supervisor,
  so `--reload` works with the default of one worker process and with
  `--processes N`.
- **`--autoscale MIN,MAX`:** compatible. The watcher lives in the
  supervisor, not in a worker, so scaling does not disturb it.
- **`--pool cpu`:** compatible; the reload starts the workers, and with
  them fresh forkservers, with the new code.
- **`--burst`:** do not combine. Burst mode exits once the queues have
  been idle for `--burst-idle-timeout` seconds; a reloaded supervisor
  would drain again and exit. Use one flag per invocation.
- **`--metrics-port` / `--health-port`:** unaffected.

## Dry-run validation

Check that the module and the watch directories resolve, without
starting the supervisor or touching broker and backend:

```bash
swarmq worker --module myapp.tasks --reload --check
# → {"ok": true, "module": "myapp.tasks", "module_origin": "/abs/path/myapp/tasks.py",
#    "reload": true, "watch_dirs": ["/abs/path/myapp"], ...}
```

`--check` imports the module, checks that `watchfiles` is installed and
resolves the watch directories. It exits `0` on success and `1` with
`"ok": false` otherwise (missing module or `watchfiles`, namespace
package, …). The full payload is described in the
[CLI reference](../reference/cli.md#swarmq-worker).

## Troubleshooting

### `--reload: watchfiles is not installed`

```bash
pip install 'swarmq[reload]'
```

Without `--check`, the worker exits with `78` in this case. An invalid
`--watch-dir` is a usage error (exit `2`).

### `inotify_init1() failed: (24) Too many open files` on Linux

You hit the per-user inotify limit. Narrow the watch with `--watch-dir`
or raise the limit:

```bash
echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### `cannot auto-detect watch directory for module ... (namespace package)`

`--module` points at a [PEP 420 namespace
package](https://peps.python.org/pep-0420/); no single directory covers
it. Pass `--watch-dir`:

```bash
swarmq worker --module myns.tasks --reload --watch-dir ./src/myns
```

### A reload fires but the code does not change

`os.execvpe` replaces the process image, so every module is imported
again. If the old code still runs, check that the changed file is the
one `--module` imports (`module_origin` in `--check`). If the log shows
`reload_aborted_alive_children`, a worker hung in an uninterruptible
kernel wait (D state) during the stop and the supervisor refused to
orphan it; it exits with `0` instead of reloading. Restart it once the
process is gone.

## See also

- [CLI reference](../reference/cli.md#swarmq-worker): `--reload`,
  `--watch-dir`, `--check`
- [Logging](../reference/logging.md#event-catalog): the reload events
