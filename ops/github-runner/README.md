# GitHub-Runner auf celsius

Ephemere Runner für die CI von swarmq und swarmv (Plan
`docs/plans/2026-10-28-1308-chore-umzug-github-drei-repos-plan.md`, KTD4–KTD6).

- Ein **Slot** ist eine systemd-Instanz `github-runner@<repo>-<nummer>`. Er holt eine
  JIT-Konfiguration, startet damit genau einen Runner für genau einen Job,
  löscht danach die Kopie samt Arbeitsverzeichnis und beginnt von vorn.
- Der **Supervisor** (`github_runner.py`) läuft als root und hält das PAT. Den
  Runner startet er als Host-Prozess unter `ghrunner`, aus einer frischen Kopie
  der gepinnten Runner-Distribution, mit `DOCKER_HOST` auf den rootless Docker
  von `ghrunner`. Job-Container und Services laufen dort, nie auf dem Docker
  des Hosts.
- Der Runner bekommt nur die JIT-Konfiguration, und zwar über die Umgebung
  (`ACTIONS_RUNNER_INPUT_JITCONFIG`), nicht über argv: Die Kommandozeile kann
  jeder Benutzer auf celsius lesen, die Umgebung nur `ghrunner` und root. Das
  PAT erscheint nicht in seiner Umgebung, nicht in seinen Argumenten und nicht
  in den Logs.

Warum kein Runner-Container: Bei `container:`-Jobs bindet der Runner seine
externals und das Arbeitsverzeichnis über den Docker-Daemon in den
Job-Container ein. Der Daemon sucht diese Pfade auf dem Host. Als Host-Prozess
sieht der Runner dieselben Pfade wie der Daemon.

Nur Standardbibliothek, Python ab 3.11. Auf celsius sind weder pip noch jq nötig.

## Einrichtung (einmalig, als root)

### Nutzer `ghrunner` mit rootless Docker

```sh
apt install uidmap dbus-user-session docker-ce-rootless-extras   # Paketnamen je nach Quelle
useradd --create-home --shell /bin/bash ghrunner                 # nicht in der Gruppe docker
loginctl enable-linger ghrunner                                  # Docker läuft ohne Login
machinectl shell ghrunner@ /usr/bin/dockerd-rootless-setuptool.sh install
machinectl shell ghrunner@ /usr/bin/systemctl --user enable --now docker
id -u ghrunner                                                   # <uid> für DOCKER_HOST
```

Prüfen: `DOCKER_HOST=unix:///run/user/<uid>/docker.sock docker info` (als
ghrunner) zeigt `rootless`. Der Docker des Hosts bleibt unberührt.
`ghrunner` darf nicht in der Gruppe `docker` sein und den Host-Socket nicht lesen.

### Runner-Distribution pinnen

```sh
VERSION=<version>   # aktuelles Release von https://github.com/actions/runner/releases
mkdir -p /opt/github-runner/dist/$VERSION
cd /opt/github-runner/dist/$VERSION
curl -fLO https://github.com/actions/runner/releases/download/v$VERSION/actions-runner-linux-x64-$VERSION.tar.gz
sha256sum actions-runner-linux-x64-$VERSION.tar.gz   # gegen die Prüfsumme in den Release-Notes
tar xzf actions-runner-linux-x64-$VERSION.tar.gz && rm actions-runner-linux-x64-$VERSION.tar.gz
./bin/installdependencies.sh                         # einmal je neuer Version
chown -R root:root . && chmod -R go-w .
```

Der Verzeichnisname ist die Version. `status` vergleicht ihn mit dem neuesten
Release. Die Distribution gehört root. Jeder Slot kopiert sie vor dem Start
nach `<STATE>/slots/<slot>/runner` und übergibt die Kopie an `ghrunner`.

### PAT

Fine-grained Personal Access Token unter `Schorschofwill`:

- Repository access: nur `swarmq` und `swarmv`
- Permissions: Administration **read and write**. Weniger reicht für die
  Registrierung von Runnern nicht. Metadata read ist automatisch dabei.
- Ablaufdatum setzen, höchstens ein Jahr. `status` warnt 14 Tage vorher.

```sh
install -d -m 0700 /etc/github-runner
install -m 0600 /dev/null /etc/github-runner/token
$EDITOR /etc/github-runner/token        # nur das Token, eine Zeile
```

Der Supervisor liest das PAT beim Start. Nach einem Tausch die Slots neu starten.

### Supervisor und Units

```sh
install -d /opt/github-runner
install -m 0644 ops/github-runner/github_runner.py /opt/github-runner/
install -m 0644 ops/github-runner/github-runner@.service /etc/systemd/system/
install -m 0600 ops/github-runner/github-runner.env.example /etc/github-runner/github-runner.env
$EDITOR /etc/github-runner/github-runner.env   # GH_RUNNER_DIST und GH_RUNNER_DOCKER_HOST setzen
systemctl daemon-reload
```

Damit kein Slot vor dem rootless Docker startet, ein Drop-in
`systemctl edit github-runner@.service` mit `After=user@<uid>.service` anlegen.

## Pools starten und vergrößern

Die Labels sind `self-hosted` plus `celsius-swarmq` bzw. `celsius-swarmv`.
Workflows wählen den Runner mit `runs-on: [self-hosted, celsius-swarmq]`.

- **Start:** zwei Slots je Pool.

  ```sh
  systemctl enable --now github-runner@swarmq-{1,2}.service
  systemctl enable --now github-runner@swarmv-{1,2}.service
  ```

- **Endgröße:** swarmq 8, swarmv 5. Während des Umzugs teilten sich GitHub-
  und GitLab-Runner den Host, deshalb lief bis dahin nur der Start (KTD10).

  ```sh
  systemctl enable --now github-runner@swarmq-{3..8}.service
  systemctl enable --now github-runner@swarmv-{3..5}.service
  ```

- **Verkleinern:** Einen Slot mit `systemctl disable --now github-runner@<slot>`
  stoppen und `<STATE>/slots/<slot>.state` löschen, sonst listet `status` ihn weiter.

Logs: `journalctl -u 'github-runner@*' -f`.

## Pausieren, Fortsetzen, Status

Zustandsverzeichnis: `GH_RUNNER_STATE_DIR`, Vorgabe `/var/lib/github-runner`.

| Datei | Bedeutung |
|---|---|
| `pause` | vorhanden: kein Slot holt eine neue Konfiguration |
| `lock` | flock-Sperre für Schalter und Slot-Start |
| `slots/<slot>.state` | ein Wort: `holt`, `laeuft`, `pausiert` oder `fehler` |

```sh
python3 /opt/github-runner/github_runner.py pause [--timeout SEKUNDEN]
python3 /opt/github-runner/github_runner.py resume
python3 /opt/github-runner/github_runner.py status
```

- `pause` setzt den Schalter unter der Sperre und kehrt zurück, wenn jeder Slot
  `pausiert` meldet. Ein wartender Runner wird vorher per API abgemeldet und erst
  danach beendet. Lehnt die API das ab, weil er einen Job hat, läuft der Job zu
  Ende. Ist der Schalter schon gesetzt, meldet `pause` „schon pausiert“ und
  wartet trotzdem. Nach `--timeout` endet es mit 1, der Schalter bleibt gesetzt.
- `resume` entfernt den Schalter. Ohne Schalter tut es nichts.
- `status` zeigt Schalter und Slots. Es warnt, wenn es ein neueres
  Runner-Release gibt und wenn das PAT in weniger als 14 Tagen abläuft. Exit 1,
  wenn GitHub nicht erreichbar ist (bei 401 meist ein abgelaufenes PAT).

`status` braucht `GH_RUNNER_DIST` und das PAT, also root und die Umgebungsdatei:
`set -a; . /etc/github-runner/github-runner.env; set +a` vor dem Aufruf.

## Monatlich: Runner-Version anheben

GitHub nimmt veraltete Runner nach einiger Zeit nicht mehr an. Ein ephemerer
Runner aktualisiert sich sonst bei jedem Job selbst in seine Kopie, das kostet
jedes Mal einen Download. Deshalb einmal im Monat, oder wenn `status` warnt:

1. Neue Version wie oben unter `/opt/github-runner/dist/<neu>` entpacken und prüfen.
2. `GH_RUNNER_DIST` in `/etc/github-runner/github-runner.env` umstellen.
3. Slots neu starten: `systemctl restart 'github-runner@*'`. Laufende Jobs
   brechen dabei ab, also vorher `pause` und danach `resume`.
4. Die alte Version nach ein paar Tagen löschen.

## Fork-PRs (KTD5)

swarmq und swarmv sind öffentlich. Code aus Fork-PRs läuft nur nach Freigabe.

- Repo-Einstellung: Actions → „Require approval for all external contributors“.
- Vor jeder Freigabe den vollständigen Diff lesen. Als ausführbarer Code auf
  celsius gilt jede Änderung an Tests, `conftest.py`, `justfile`,
  `pyproject.toml`, `uv.lock`, Dockerfiles, Skripten und `.github/`.
- Jeder neue Push auf einen freigegebenen PR wird erneut geprüft.
- Kein Workflow nutzt `pull_request_target`.

Ein freigegebener bösartiger Job erreicht `ghrunner` und das Netz von celsius,
aber nicht root, nicht das PAT und nicht die Messcontainer auf dem Docker des
Hosts. Bei Verdacht: PAT widerrufen, Repo-Einstellungen und Umgebung `pypi`
prüfen, den Docker von `ghrunner` neu aufsetzen.

Was ephemer ist und was nicht: Je Job neu sind die Runner-Kopie, das
Arbeitsverzeichnis und die Job-Container. Über Jobs hinweg bleiben das
Home-Verzeichnis von `ghrunner`, der rootless Docker mit seinen Images und Volumes
(darunter die Fork-Caches `swarmq-uv-cache-fork` und `swarmv-uv-cache-fork`)
und ein Prozess, der sich aus der Prozessgruppe des Runners gelöst hat. Weil
alle Jobs in `container:` laufen und der Job-Container den Docker-Socket nicht
sieht, erreicht Fork-Code davon nur die Fork-Volumes des uv-Caches; deshalb
ist der Cache nach Vertrauensstufe getrennt. Nach einem verdächtigen Fork-Job
daher zusätzlich beide Fork-Volumes löschen (`docker volume rm` als
`ghrunner`).
