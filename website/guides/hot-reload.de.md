# Hot Reload (nur Dev)

`--reload` beobachtet deine Task-Module auf Änderungen und triggert
die bestehende SIGHUP-Reload-Pipeline — der Supervisor drained
in-flight-Tasks, dann re-exec'd er sich selbst via `os.execvpe`,
sodass neuer Code ohne vollständigen Container-Restart geladen wird.

!!! warning "Kein Produktions-Feature"
    Hot Reload ist für den lokalen Dev-Loop gedacht. Bei jedem
    Worker-Start mit `--reload` loggt SwarmQ ein
    `hot_reload_enabled`-WARNING. Taucht dieser Log in Produktions-
    Aggregatoren auf, wurde das Flag versehentlich aktiv gelassen.
    Keine Durchsetzung — der Operator ist autoritativ.

## TL;DR

```bash
pip install 'swarmq[reload]'
swarmq worker --module myapp.tasks --processes 2 --reload
```

Editiere `myapp/tasks.py`, speichere, beobachte den Log:

```
reload_triggered changed_count=1 changed_sample=['.../tasks.py']
process_manager_reload_execvp
```

Innerhalb ~2 s ist ein frischer Supervisor mit deinem neuen Code
hochgefahren.

## Was beobachtet wird

Per Default ist das beobachtete Verzeichnis das **Elternverzeichnis
der aufgelösten Quelldatei von `--module`**:

- Top-Level-Modul `myapp.tasks` → beobachtet `myapp/`.
- Package `myapp` (mit `__init__.py`) → beobachtet `myapp/`.
- Namespace-Package (ohne `__init__.py`) → **Fehler**, nutze
  `--watch-dir`.

Override mit einem oder mehreren `--watch-dir PATH`-Flags
(wiederholbar):

```bash
swarmq worker --module myapp.tasks --reload \
  --watch-dir ./tasks --watch-dir ./plugins
```

## Was ignoriert wird

Der Filter ist `PythonFilter` aus `watchfiles` plus eine
SwarmQ-spezifische Erweiterung. Folgendes triggert **keinen**
Reload:

- `__pycache__/`, `*.pyc`
- `.git/`, `.hg/`, `.svn/`
- `*.swp`, `*.swx`, `*~` (Vim-Backup), `.#*` (Emacs-Lock)
- `4913` (Vim-Probe-File, vor jedem Save geschrieben)
- `*.tmp`, `*.bak`

Framework-Source unter `src/swarmq/` wird **nicht** beobachtet —
sonst würde `pip install -e .` während der SwarmQ-Entwicklung selbst
endlos loopen.

## Wie es funktioniert

| Phase | Mechanismus |
|---|---|
| Detect | Daemon-Thread läuft `watchfiles.watch(...)` (Rust-`notify`-Crate darunter). Events treffen ein 1,6s-Debounce-Fenster, um Format-on-Save-Bursts zu absorbieren. |
| Trigger | Beim ersten debounceden Event ruft der Thread `os.kill(os.getpid(), SIGHUP)` auf. |
| Reload | Der bestehende SIGHUP-Handler des Supervisors drained Children und `os.execvpe`t `sys.executable` mit dem Startup-argv + Startup-env-Snapshot — In-Process-Mutation von `sys.argv` / `os.environ` kann den Re-Exec-Vektor nicht beeinflussen (R4.6-Hardening). Der argv-Snapshot stammt aus `sys.orig_argv`, Interpreter-Flags und `python -m` bleiben also erhalten: Ein als `python -m swarmq.cli worker …` gestarteter Supervisor lädt mit demselben Kommando neu. |

Mit `--log-config` prüft der Supervisor die Logging-Konfigurationsdatei
erneut, bevor er einen Worker stoppt. Ist die Datei seit dem Start
ungültig geworden, bricht der Reload mit einem ERROR
`log_config_reload_failed` ab (Felder `path`, `error_type`, `error`,
`hint`); Worker und Logging laufen unverändert weiter. Korrigiere die
Datei und speichere erneut ein Task-Modul (oder sende `SIGHUP`), um es
noch einmal zu versuchen.

Der Watcher unterdrückt Re-Entrancy: Läuft bereits ein Reload,
wenn eine zweite Änderung landet, wird das zweite Event verworfen
(der laufende Reload nimmt den neueren Code ohnehin auf). Ein
gecrashter Watcher-Thread loggt `watch_thread_crashed` und exitet
still — der Supervisor läuft ohne Reload bis zum nächsten Restart
weiter.

## Interaktion mit anderen Worker-Flags

- **`--processes`:** `--reload` aktiviert den Multi-Process-
  Supervisor automatisch. Du musst `--processes` nicht dazu
  übergeben; der Default von einem Child-Worker reicht.
- **`--autoscale MIN,MAX`:** kompatibel mit `--reload`. Der
  Watcher lebt auf dem Supervisor, nicht auf einem einzelnen Child,
  also stören Scale-Events die Reload-Pipeline nicht.
- **`--burst`:** nicht dafür gedacht, mit `--reload` kombiniert zu
  werden. Burst-Mode exitet sauber, sobald die Queue
  `--burst-idle-timeout` Sekunden Idle war — ein Reload-Trigger
  während des Drain-Fensters würde zwar geehrt (SIGHUP erreicht
  den Supervisor), aber der re-executete Prozess würde sofort
  wieder drainen und exiten. Willst du „Reload für Dev,
  Drain-and-Exit für Batch-Runs", wähle ein Flag pro Invocation.
- **`--metrics-port` / `--health-port`:** unbeeinflusst.

## Dry-Run-Validierung

Bevor du einen Reload-Aufruf an systemd oder einen Dev-Container
committest, verifiziere, dass Modul und Watch-Dirs auflösen, ohne
den Supervisor zu starten:

```bash
swarmq worker --module myapp.tasks --reload --check
# → {"ok": true, "module": "myapp.tasks", "reload": true,
#    "watch_dirs": ["/abs/path/to/myapp"], "module_origin": "…"}
```

`--check` importiert das Modul einmal (derselbe `find_spec`-Pfad,
den der Supervisor nutzt), verifiziert die `watchfiles`-Dependency
und löst das Watch-Verzeichnis auf. Es berührt weder Broker noch
Backend. Exit-Code `0` bei Erfolg, `1` bei jedem
`ConfigurationError` (z. B. fehlendes Modul, fehlendes
`watchfiles`, Namespace-Package).

## Troubleshooting

### `--reload: watchfiles is not installed`

```bash
pip install 'swarmq[reload]'
```

### `inotify_init1() failed: (24) Too many open files` auf Linux

Du triffst das Per-User-Inotify-Watch-Limit (Default 8192). Der
Watcher-Scope ist bereits eng, aber ein sehr großer Task-Baum
kann ihn trotzdem überschreiten. Limit erhöhen:

```bash
echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### `cannot auto-detect watch directory for module ... (namespace package)`

`--module` zeigt auf ein [PEP-420-Namespace-
Package](https://peps.python.org/pep-0420/) — kein einzelnes
Verzeichnis deckt es ab. Übergib `--watch-dir` explizit:

```bash
swarmq worker --module myns.tasks --reload --watch-dir ./src/myns
```

### Reload feuert, aber Code aktualisiert nicht

Prüfe, dass der `--module`-Pfad beim Reload frisch importiert —
`os.execvpe` ersetzt das Prozess-Image, also wird Modul-State von
Grund auf neu gebaut. Siehst du `reload_aborted_alive_children`,
war ein Worker während des Shutdowns in D-State festgefahren und
der Supervisor weigerte sich, ihn zu verwaisen; sende SIGHUP
erneut, sobald er exitet.
