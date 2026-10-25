# Mutmut im Container

Mutation-Testing (mutmut 3.8) läuft in einem Abbild aus diesem Repo, gebaut und
gestartet über einen Docker-Kontext. Auf dem Zielrechner liegt kein Checkout und
kein uv; der Laptop bleibt frei (Plan `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md`,
U2, KTD4).

## Dateien

| Datei | Zweck |
|---|---|
| `Dockerfile` | Basis `ghcr.io/astral-sh/uv:0.11.6-python3.14-trixie` (wie CI-Job `.uv-git`), `uv sync --frozen --extra test --extra reload`, Repo nach `/app` |
| `entrypoint.sh` | schreibt die Testauswahl in die `pyproject.toml` des Containers, setzt für Lauf b die Dienst-Variablen, ruft `mutmut run` |
| `compose.yml` | Projekt `swarmq-mutation`: Dienst `mutmut` (3 CPUs, 8 GB), Profil `b` mit `rabbitmq` 4.3.6 und `valkey` 8, keine Host-Ports, alles mit Label `swarmq-mutation` |
| `../../.dockerignore` | hält `.venv`, `.git`, Caches, `site`, `dist`, `mutants*`, `runs`, `prototypes` aus dem Build-Kontext (rund 15 MB) |

## Läufe

| Lauf | Testauswahl | Muster | Kinder |
|---|---|---|---|
| `a` | `tests/unit` | alle Mutanten aus `[tool.mutmut].source_paths` | 3 |
| `b` | `tests/unit`, `tests/integration/backend` | `swarmq.backend.valkey.*` | 1 |

mutmut 3.8 liest die Testauswahl nur aus `pytest_add_cli_args_test_selection` in
`pyproject.toml`; einen CLI-Schalter dafür gibt es nicht. Der Entrypoint schreibt
den Schlüssel deshalb in die Kopie im Container; die `pyproject.toml` im Repo
nennt weiter nur `tests/unit`.

Lauf b: Die Integrationstests finden die Dienste über
`SWARMQ_TEST_RABBITMQ_HOST=rabbitmq`, `SWARMQ_TEST_VALKEY_HOST=valkey`, die
Standardports und `SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1` (nur im Container, die
Dienste sind Wegwerf-Container dieses Projekts). Ohne pytest-xdist teilen alle
Kinder vhost `/` und Valkey-DB 0, und die autouse-Fixture `flush_valkey` leert
sie vor und nach jedem Test; darum genau ein Kind. Vor `mutmut run` wartet der
Entrypoint auf die Dienste und lässt `pytest tests/integration/backend -x -q`
laufen, weil `check_docker` bei fehlenden Diensten überspringt statt scheitert;
meldet der Vorlauf einen übersprungenen Test, startet mutmut nicht. Eine andere
Kindzahl als 1 weist der Entrypoint für Lauf b ab.

Umgebungsvariablen beim Start (an Compose durchgereicht):

- `MUTMUT_MUSTER`: Muster für `mutmut run`, fnmatch über Schlüssel der Form
  `swarmq.<modulpfad>.<funktion>__mutmut_N`. Ein Muster ohne `*` oder ohne
  Präfix `swarmq.` weist der Entrypoint ab (mutmut bräche sonst mit AssertionError oder KeyError ab).
- `MUTMUT_MAX_CHILDREN`: überschreibt die Kindzahl von Lauf a. cgroup-Limits ändern
  `os.cpu_count()` nicht, deshalb wird sie immer ausdrücklich gesetzt.

## Rezepte

Kontext aus `SWARMQ_MUTATION_CONTEXT`, Standard `lastserver`. Jeder Aufruf nennt
ihn mit `docker --context`; `docker context use` wird nie benutzt.

```bash
uv run just mutation-starten a     # bauen, frisch starten (abgekoppelt); bricht ab, wenn schon
                                   # ein swarmq-mutation-Container oder loadsuite-runner-* läuft
docker --context lastserver compose -f docker/mutation/compose.yml ps -a
docker --context lastserver compose -f docker/mutation/compose.yml logs --tail 20 mutmut
uv run just mutation-holen a       # mutants/ nach mutants-a/mutants, mutmut results -> mutants-a/ergebnisse.txt
uv run just mutation-abbauen       # down -v --rmi local, prüft danach: nichts mit Label swarmq-mutation
```

`mutation-holen` geht auch während des Laufs (Zwischenstand) und auf den beendeten
Container. Die neue Kopie wird zuerst ausgewertet; erst wenn `mutmut results` sie
lesen kann, ersetzt sie `mutants-LAUF/`. mutmut schreibt die `.meta`-Dateien nicht
atomar: Scheitert das Holen während des Laufs, einfach wiederholen.

`mutation-starten` bricht ab, wenn der Kontext nicht erreichbar ist oder noch ein
Container (auch ein beendeter mit ungeholten Ergebnissen) oder ein Volume mit Label
`swarmq-mutation` existiert: erst `mutation-holen`, dann `mutation-abbauen`.

Lokaler Rauchtest auf dem Laptop (ein kleines Modul, ein Kind):

```bash
SWARMQ_MUTATION_CONTEXT=default MUTMUT_MUSTER='swarmq.backend.valkey.chords.*' \
  MUTMUT_MAX_CHILDREN=1 uv run just mutation-starten a
```

## Grenzen

- `mutation-abbauen` entfernt das gebaute Abbild, nicht die Basisabbilder
  (uv, rabbitmq, valkey) und nicht den Build-Cache; sie tragen kein Label und
  werden auf dem Lastserver auch von der Loadsuite genutzt.
- Ein neuer Lauf beginnt immer in einem neuen Container mit leerem `mutants/`
  (`--force-recreate`); Ergebnisse vorher holen.
