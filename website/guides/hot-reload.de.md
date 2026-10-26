# Hot Reload (nur Dev)

`--reload` beobachtet deine Task-Module und startet die Worker bei jeder
gespeicherten Änderung neu. Es nutzt den `SIGHUP`-Reload des
Supervisors: Der Supervisor lässt die laufenden Tasks auslaufen und
führt sich per `os.execvpe` neu aus, sodass der neue Code ohne
Container-Neustart geladen wird.

!!! warning "Kein Produktions-Feature"
    Hot Reload ist für die lokale Entwicklungsschleife gedacht. Jeder
    Worker, der mit `--reload` startet, loggt eine WARNING
    `hot_reload_enabled`. Taucht diese Zeile in deinen Produktions-Logs
    auf, wurde das Flag versehentlich angelassen. SwarmQ erzwingt nichts;
    der Operator entscheidet.

## Schnellstart

```bash
pip install 'swarmq[reload]'
swarmq worker --module myapp.tasks --reload
```

`myapp/tasks.py` bearbeiten, speichern und das Log beobachten:

```text
reload_triggered changed_count=1 changed_sample=['.../myapp/tasks.py']
process_manager_reload_requested
process_manager_reload_execvp
```

Etwa zwei Sekunden später läuft ein frischer Supervisor mit dem neuen
Code.

## Was beobachtet wird

Standardmäßig wird das **übergeordnete Verzeichnis der Quelldatei von
`--module`** beobachtet:

- `--module myapp.tasks` (Datei `myapp/tasks.py`) → beobachtet `myapp/`.
- `--module myapp` (Package mit `__init__.py`) → beobachtet `myapp/`.
- Ein Namespace-Package (ohne `__init__.py`) → **Fehler**, gib
  `--watch-dir` an.

Mit einem oder mehreren `--watch-dir PATH` überschreibst du das:

```bash
swarmq worker --module myapp.tasks --reload \
  --watch-dir ./tasks --watch-dir ./plugins
```

Beobachtet wird rekursiv. Liegt das ermittelte Verzeichnis im virtualenv
oder in der Standardbibliothek, loggt der Supervisor die WARNING
`watch_dir_inside_venv_or_stdlib`: Jedes `pip install` löste dann
Reloads aus, gib stattdessen `--watch-dir` an. `--watch-dir` ohne
`--reload` wird mit einer WARNING ignoriert.

## Was einen Reload auslöst

Nur Änderungen an Python-Quelldateien (`.py`, `.pyx`, `.pyd`) lösen
einen Reload aus, Änderungen an Konfiguration, Templates oder Daten
nicht. Davon ignoriert der Filter (`watchfiles.PythonFilter` plus eine
SwarmQ-Erweiterung) außerdem:

- `__pycache__/`, `.git/`, `.hg/`, `.svn/`, `.tox/`, `.venv/`,
  `.idea/`, `node_modules/`, `.mypy_cache/`, `.pytest_cache/`,
  `.hypothesis/`
- Editor-Dateien: `*.swp`, `*.swx`, `*~`, `.#*` und `4913` (die
  Probedatei von Vim)
- `*.tmp` und `*.bak`

Für einen Neustart nach einer Änderung an anderen Dateien speichere ein
Task-Modul oder schicke dem Supervisor `SIGHUP`.

## Wie es funktioniert

| Phase | Mechanismus |
|---|---|
| Erkennen | Ein Daemon-Thread führt `watchfiles.watch(...)` aus. Änderungen werden über ein Debounce-Fenster von 1,6 s gesammelt, sodass Format-on-Save-Schübe einen einzigen Reload auslösen. |
| Auslösen | Nach dem Fenster schickt der Thread `SIGHUP` an den eigenen Prozess (den Supervisor). |
| Reload | Der `SIGHUP`-Handler merkt sich nur die Anforderung; die Überwachungsschleife des Supervisors (alle 0,5 s) führt sie aus. Sie stoppt die Worker im Shutdown-Budget, leert ihre Log-Queue innerhalb von `log_drain_timeout` und führt `sys.executable` mit dem argv und der Umgebung vom Start neu aus. Änderungen an `sys.argv` oder `os.environ` zur Laufzeit gelangen nicht in den neuen Prozess. Das argv stammt aus `sys.orig_argv`, Interpreter-Flags und `python -m swarmq.cli worker …` überstehen den Reload also. |

Eine Änderung, die eintrifft, während ein Reload ansteht, geprüft wird
oder läuft, wird verworfen; der laufende Reload übernimmt den neueren
Code ohnehin. Ein abgestürzter Watcher-Thread loggt
`watch_thread_crashed` und hört auf; der Supervisor läuft bis zu seinem
nächsten Neustart ohne Reload weiter.

### Importfehler halten den Supervisor am Leben

Ohne `--reload` beendet ein `ImportError` oder `SyntaxError` im
Task-Modul `swarmq worker` nach einem Versuch mit Exit `78`. Mit
`--reload` bleibt der Supervisor stehen, startet keine Worker und
wartet: Datei reparieren, speichern, und der nächste Reload startet die
Worker wieder. Andere Exceptions auf Modulebene lassen den Worker
weiterhin abstürzen und zählen gegen das Neustartlimit.

### Reload mit `--log-config`

Mit `--log-config` prüft der Supervisor die Logging-Konfigurationsdatei
erneut, bevor er einen Worker stoppt. Die Prüfung führt `dictConfig` in
einem eigenen Interpreter in einer eigenen Prozessgruppe aus, während
Überwachung, Autoscaling und Kontrollkanal weiterlaufen; mehrere
`SIGHUP`s während einer Prüfung führen zu einer Prüfung. Das Ergebnis:

- **gültig:** Der Reload läuft weiter;
- **seit dem Start ungültig geworden:** Der Reload wird mit einem ERROR
  `log_config_reload_failed` abgebrochen (Felder `path`, `error_type`,
  `error`, `hint`);
- **nach 30 Sekunden nicht fertig**, etwa weil ein Handler an seinem
  Ziel hängt (Syslog über TCP): Der Reload wird mit einem ERROR
  `log_config_reload_probe_timeout` abgebrochen (Felder `path`,
  `timeout_s`, `hint`). Das heißt nicht, dass die Datei ungültig ist.

Nach einem Abbruch laufen Worker und Logging unverändert weiter.
Repariere die Datei oder ihr Ziel und speichere erneut ein Task-Modul
(oder schicke `SIGHUP`), um es noch einmal zu versuchen.

### `SIGTERM` gewinnt immer

`SIGTERM` und `SIGINT` gewinnen gegen einen Reload: während einer
Prüfung, während die Worker für einen Reload stoppen, und wenn ein
`SIGHUP` direkt nach dem `SIGTERM` eintrifft. Der Supervisor fährt dann
herunter, ohne sich neu auszuführen, und beendet eine laufende Prüfung.
Ein `SIGTERM`, das im letzten Moment vor `os.execvpe` eintrifft, bleibt
anhängig und beendet den neuen Supervisor, sobald dieser seine
Signal-Handler eingerichtet hat.

## Zusammenspiel mit anderen Worker-Flags

- **`--processes`:** Jedes `swarmq worker` läuft unter dem Supervisor,
  `--reload` funktioniert also mit dem Default von einem
  Worker-Prozess und mit `--processes N`.
- **`--autoscale MIN,MAX`:** verträglich. Der Watcher lebt im
  Supervisor, nicht in einem Worker, Skalieren stört ihn also nicht.
- **`--pool cpu`:** verträglich; der Reload startet die Worker und mit
  ihnen frische Forkserver mit dem neuen Code.
- **`--burst`:** nicht kombinieren. Der Burst-Modus endet, sobald die
  Queues `--burst-idle-timeout` Sekunden leer waren; ein neu geladener
  Supervisor liefe erneut leer und endete. Nimm pro Aufruf eines der
  beiden Flags.
- **`--metrics-port` / `--health-port`:** unbeeinflusst.

## Trockenlauf-Prüfung

Prüfe, ob Modul und beobachtete Verzeichnisse aufgelöst werden, ohne den
Supervisor zu starten oder Broker und Backend zu berühren:

```bash
swarmq worker --module myapp.tasks --reload --check
# → {"ok": true, "module": "myapp.tasks", "module_origin": "/abs/path/myapp/tasks.py",
#    "reload": true, "watch_dirs": ["/abs/path/myapp"], ...}
```

`--check` importiert das Modul, prüft, ob `watchfiles` installiert ist,
und löst die beobachteten Verzeichnisse auf. Es endet mit `0` bei
Erfolg und sonst mit `1` und `"ok": false` (fehlendes Modul oder
`watchfiles`, Namespace-Package, …). Die volle Ausgabe beschreibt die
[CLI-Referenz](../reference/cli.md#swarmq-worker).

## Fehlerbehebung

### `--reload: watchfiles is not installed`

```bash
pip install 'swarmq[reload]'
```

Ohne `--check` endet der Worker in diesem Fall mit `78`. Ein ungültiges
`--watch-dir` ist ein Bedienfehler (Exit `2`).

### `inotify_init1() failed: (24) Too many open files` unter Linux

Du hast das inotify-Limit pro Benutzer erreicht. Verenge die
Beobachtung mit `--watch-dir` oder erhöhe das Limit:

```bash
echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### `cannot auto-detect watch directory for module ... (namespace package)`

`--module` zeigt auf ein [PEP-420-Namespace-Package](https://peps.python.org/pep-0420/);
kein einzelnes Verzeichnis deckt es ab. Gib `--watch-dir` an:

```bash
swarmq worker --module myns.tasks --reload --watch-dir ./src/myns
```

### Ein Reload feuert, aber der Code ändert sich nicht

`os.execvpe` ersetzt das Prozessabbild, jedes Modul wird also neu
importiert. Läuft trotzdem der alte Code, prüfe, ob die geänderte Datei
die ist, die `--module` importiert (`module_origin` in `--check`). Zeigt
das Log `reload_aborted_alive_children`, hing ein Worker beim Stoppen in
einem nicht unterbrechbaren Kernel-Wartezustand (D-State), und der
Supervisor hat sich geweigert, ihn verwaisen zu lassen; er endet dann
mit `0`, statt neu zu laden. Starte ihn neu, sobald der Prozess weg ist.

## Siehe auch

- [CLI-Referenz](../reference/cli.md#swarmq-worker): `--reload`,
  `--watch-dir`, `--check`
- [Logging](../reference/logging.md#event-katalog): die Reload-Events
