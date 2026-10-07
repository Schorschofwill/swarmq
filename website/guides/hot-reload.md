# Hot reload (development only)

`--reload` watches your task modules for changes and triggers the
existing SIGHUP reload pipeline — the supervisor drains in-flight
tasks, then re-execs itself via `os.execvpe` so new code is picked up
without a full container restart.

!!! warning "Not a production feature"
    Hot reload is designed for the local dev loop. On every worker
    start with `--reload`, SwarmQ logs a `hot_reload_enabled` WARNING.
    If that log shows up in production aggregators, the flag was left
    on by mistake. No enforcement — the operator is authoritative.

## TL;DR

```bash
pip install 'swarmq[reload]'
swarmq worker --module myapp.tasks --processes 2 --reload
```

Edit `myapp/tasks.py`, save, watch the log emit:

```
reload_triggered changed_count=1 changed_sample=['.../tasks.py']
process_manager_reload_execvp
```

Within ~2s a fresh supervisor is up with your new code loaded.

## What gets watched

By default, the watched directory is the **parent of `--module`'s
resolved source file**:

- Top-level module `myapp.tasks` → watches `myapp/`.
- Package `myapp` (with `__init__.py`) → watches `myapp/`.
- Namespace package (no `__init__.py`) → **error**, pass `--watch-dir`.

Override with one or more `--watch-dir PATH` flags (repeatable):

```bash
swarmq worker --module myapp.tasks --reload \
  --watch-dir ./tasks --watch-dir ./plugins
```

## What gets ignored

The filter is `PythonFilter` from `watchfiles` plus a SwarmQ-specific
extension. The following **do not** trigger a reload:

- `__pycache__/`, `*.pyc`
- `.git/`, `.hg/`, `.svn/`
- `*.swp`, `*.swx`, `*~` (Vim backup), `.#*` (Emacs lock)
- `4913` (Vim probe-file written before each save)
- `*.tmp`, `*.bak`

Framework source under `src/swarmq/` is **not** watched — otherwise
`pip install -e .` would loop endlessly while you develop SwarmQ
itself.

## How it works

| Phase | Mechanism |
|---|---|
| Detect | Daemon thread runs `watchfiles.watch(...)` (Rust `notify` crate under the hood). Events hit a 1.6s debounce window to absorb format-on-save bursts. |
| Trigger | On the first debounced event, the thread calls `os.kill(os.getpid(), SIGHUP)`. |
| Reload | The supervisor's SIGHUP handler only records the request; its monitor loop (every 0.5 s) carries it out: it drains children, flushes its log queue within `log_drain_timeout` and `os.execvpe`s `sys.executable` with the startup-argv + startup-env snapshot — in-process mutation of `sys.argv` / `os.environ` cannot influence the re-exec vector (R4.6 hardening). The argv snapshot is taken from `sys.orig_argv`, so interpreter flags and `python -m` survive: a supervisor started as `python -m swarmq.cli worker …` reloads into the same command. |

With `--log-config`, the supervisor validates the logging config file
again before it stops any worker. The check runs `dictConfig` in a
separate interpreter, in its own process group, and the monitor loop
polls it; restarting dead or hung workers, autoscaling and the control
channel carry on meanwhile. Several `SIGHUP`s during a check lead to
exactly one check. The outcome:

- valid: the reload proceeds;
- the file has become invalid since the start: the reload is aborted
  with a `log_config_reload_failed` ERROR (fields `path`, `error_type`,
  `error`, `hint`);
- the check has not finished after 30 seconds, for example because a
  handler blocks on its target (syslog over TCP): the reload is aborted
  with a `log_config_reload_probe_timeout` ERROR (fields `path`,
  `timeout_s`, `hint`) — this does not mean the file is invalid.

When aborted, workers and logging keep running unchanged. Fix the file
or its target and save a task module again (or send `SIGHUP`) to retry.

`SIGTERM` (and `SIGINT`) always win over a reload: during a check, after
the reload was decided while the workers are stopping, and when a
`SIGHUP` arrives right after the `SIGTERM`. The supervisor then shuts
down without re-executing itself; a check still running is killed. A
`SIGTERM` that arrives in the last moment before `os.execvpe` stays
pending and ends the new supervisor as soon as it has set up its signal
handlers — also when another thread of the supervisor receives it.

The watcher suppresses re-entrancy: if a reload is already pending, being
checked or in flight when a second change lands, the second event is
discarded (the
already-running reload will pick up the newer code anyway). A crashed
watcher thread logs `watch_thread_crashed` and exits silently — the
supervisor keeps running without reload until the next restart.

## Interaction with other worker flags

- **`--processes`:** `--reload` activates the multi-process
  supervisor automatically. You do not need to pass `--processes`
  alongside it; the default of one child worker is enough.
- **`--autoscale MIN,MAX`:** compatible with `--reload`. The
  watcher lives on the supervisor, not on any individual child, so
  scale events do not disturb the reload pipeline.
- **`--burst`:** not designed to be combined with `--reload`.
  Burst mode exits cleanly once the queue has been idle for
  `--burst-idle-timeout` seconds — a reload trigger that fires
  during the drain window would still be honoured (SIGHUP reaches
  the supervisor), but the re-executed process would then drain
  again immediately and exit. If you want "reload for dev,
  drain-and-exit for batch runs," pick one flag per invocation.
- **`--metrics-port` / `--health-port`:** unaffected.

## Dry-run validation

Before committing a reload invocation to systemd or a dev-container,
verify that the module and watch-dirs resolve without starting the
supervisor:

```bash
swarmq worker --module myapp.tasks --reload --check
# → {"ok": true, "module": "myapp.tasks", "reload": true,
#    "watch_dirs": ["/abs/path/to/myapp"], "module_origin": "…"}
```

`--check` imports the module once (same `find_spec` path the
supervisor uses), verifies the `watchfiles` dependency is installed,
and resolves the watch directory. It never touches broker or backend.
Exit code is `0` on success, `1` on any `ConfigurationError`
(e.g. missing module, missing `watchfiles`, namespace package).

## Troubleshooting

### `--reload: watchfiles is not installed`

```bash
pip install 'swarmq[reload]'
```

### `inotify_init1() failed: (24) Too many open files` on Linux

You hit the per-user inotify watch limit (default 8192). The watcher
scope is already narrow, but a very large task tree can still exceed
it. Raise the limit:

```bash
echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### `cannot auto-detect watch directory for module ... (namespace package)`

`--module` points at a [PEP 420 namespace
package](https://peps.python.org/pep-0420/) — no single directory
covers it. Pass `--watch-dir` explicitly:

```bash
swarmq worker --module myns.tasks --reload --watch-dir ./src/myns
```

### Reload fires but code doesn't update

Check that the `--module` path imports freshly on reload — `os.execvpe`
replaces the process image, so module state is rebuilt from scratch.
If you see `reload_aborted_alive_children`, a worker was stuck in
D-state during shutdown and the supervisor refused to orphan it;
re-send SIGHUP once it exits.
