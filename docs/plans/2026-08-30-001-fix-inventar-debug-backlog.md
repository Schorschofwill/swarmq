# Fix-Inventar: Debug-Feature-Backlog der Lasttest-Suite

**Stand:** 2026-08-30 (3. Abgleich) · **Zweck:** Abgleichsgrundlage für neu
gemeldete Probleme.

Quelle der Einträge: `docs/debug-feature-backlog.md` im Repo `swarmq-loadsuite`.
Umsetzung: MR !39 (`fix/publish-deadline-port-guard-logfix`), Release 0.3.0.
Plan: `docs/plans/2026-08-28-001-fix-publish-deadline-port-guard-logfix-plan.md`.

---

## Behoben in 0.3.0

### B8 — `schedule` hatte kein Publish-Timeout

**War:** Ein Publish wartete auf einem still toten Transport unbegrenzt auf sein
Publisher-Confirm. Keine Ausnahme, Kanal formal offen, der bestehende
Wiederholpfad griff nie. Im Referenzlauf vom 2026-08-28 wurden aus einem
fünfsekündigen Toxiproxy-Ausfall 62 Minuten Stillstand, per SIGTERM nicht
beendbar.

**Jetzt:** `SWARMQ_PUBLISH_TIMEOUT_S` / `SwarmQConfig.publish_timeout_s`,
Default 120 s, ab Werk aktiv. Der Deckel umspannt den **gesamten** Vorgang
inklusive aller Wiederholversuche, nicht den Einzelversuch, und bindet an die
Broker-Naht — gilt also für Client-Publish, Retry-Wiedereinstellung,
Dead-Lettering, Scheduler-Tick, Workflow-Folgeschritt und den
Dead-Letter-Admin-Pfad.

**Mitgeliefert:**
- Nach einem Fristbruch wird der Publish-Kanal verworfen (Identitätsprüfung
  unter der Sperre, Schließen nebenläufig mit eigener kurzer Frist) — sonst
  verbrennt jeder Folge-Publish erneut die volle Frist.
- Im Herunterfahren gilt zusätzlich das verbleibende Gnadenfrist-Budget
  abzüglich einer anteiligen Reserve; laufende Deckel werden nachgezogen.
- Neuer Fehlertyp `PublishIndeterminate` (erbt von `BrokerConnectionError`).
  Bei ihm bleibt eine `unique_for`-Dedup-Sperre **stehen** statt freigegeben zu
  werden — ein Wiederholversuch wird abgewiesen statt zu duplizieren.

**Dateien:** `src/swarmq/config.py`, `app.py`, `exceptions.py`, `agent_spec.py`,
`broker/rabbitmq/{connection,delivery}.py`, `contracts/broker.py`,
`worker/lifecycle.py`, `client/schedule.py`

### B3 — Metrik-Port je Kindprozess (**nur die erste Hälfte**)

**War:** `--processes N` vererbte allen Kindern denselben Port. Das erste band
ihn, die übrigen scheiterten mit Traceback, der Supervisor startete sie bis zum
Restart-Budget nach — danach lief der Worker dauerhaft unterbesetzt weiter,
ohne dass eine Meldung die Portkollision nannte.

**Jetzt:** Der Start wird abgelehnt, bevor ein Kindprozess entsteht. Geprüft
wird der **aufgelöste** Wert — `SWARMQ_METRICS_PORT` / `SWARMQ_HEALTH_PORT`
also genauso wie die Kommandozeilenoptionen; die Meldung nennt die wirksame
Quelle. Der Autoskalierungsbereich zählt mit seiner **Obergrenze**.
`swarmq worker --check` meldet denselben Konflikt.

**WICHTIG — nicht behoben:** Ein **Port je Kindprozess** existiert weiterhin
nicht. Die Mehrprozess-Topologie mit Beobachtungs-Port bleibt unbenutzbar; die
Lasttest-Suite braucht ihren Workaround (Ports per Env im eigenen Worker-Entry)
weiter. Ein neu gemeldetes Problem in dieser Richtung ist **nicht** durch 0.3.0
abgedeckt.

**Dateien:** `src/swarmq/process_manager/{check,run}.py`, `cli/check_cmd.py`,
`cli/parser.py`

### B6 — JSON-Logs verdoppelten jede Zeile

**War:** Die Prozessorkette des `ProcessorFormatter` entfernte ihre eigenen
Arbeitsschlüssel nicht vor dem Renderer. `_record` trägt den LogRecord, dessen
`msg` das komplette Ereignis-Dict ist — die Nutzlast stand zweimal in jeder
Zeile.

**Jetzt:** `remove_processors_meta` vor dem Renderer. Beide Renderer teilen den
Formatter, die **Textausgabe trug die Schlüssel ebenfalls** und verliert sie
mit.

**Dateien:** `src/swarmq/logging.py`

---

## Behoben nach 0.3.0 (noch nicht veröffentlicht)

### B10 — `group` verliert zusammengesetzte Glieder

Kam mit `576279f` (Loadsuite, 2026-08-29) zusammen mit B9 in den Backlog; beide
fielen beim Bau der Konformitätsstufe auf, wo zum ersten Mal Workflows durch den
Stack laufen. Der Commit hat ausschließlich hinzugefügt — B1–B8 sind
textlich unverändert.

**Klasse:** Anders als B1–B7 und B9 ist das keine Fähigkeitslücke, sondern ein
**stiller Korrektheitsfehler**. Der Aufrufer bekommt eine Ergebnisliste, die wie
ein vollständiges Ergebnis aussieht, und kann sie von einer vollständigen nicht
unterscheiden. Dieselbe Klasse wie B8, nur ohne den Stillstand, der einen darauf
stößt.

**Am Code bestätigt** (`src/swarmq/workflow_orchestration.py`, Stand 0.3.0):

- `_frontier` legt für ein zusammengesetztes Glied **keinen** Schlüssel
  `<gruppe>/g<i>` an, sondern rekursiert direkt auf dessen Blätter — aus
  `group(sig(a), chain(sig(b), sig(c)))` werden die Schlüssel `/g0` und `/g1/0`,
  ein `/g1` entsteht nie (`:287`).
- `_materialise` erzeugt für das fertige Glied zwar eine synthetische Kennung,
  schreibt sie aber **nie** nach `step_tasks` (`:642`).
- `_ordered_member_ids` sucht `<gruppe>/g<i>` und überspringt den fehlenden
  Schlüssel wortlos (`:605`).

**Zwei Verschärfungen, die der Backlog-Eintrag nicht nennt:**

1. **Der Chord-Header ist genauso betroffen — und schlimmer.** Dieselbe Methode
   baut die Liste, die als `inject` in den Chord-Body wandert. Ein `chord`, dessen
   Header eine `group` mit zusammengesetztem Glied ist, füttert den Body mit einer
   stillschweigend gekürzten Eingabe. Der Fehler steckt dann nicht im
   Rückgabewert, sondern in der nachgelagerten Rechnung selbst.
2. **Es gibt dafür keinerlei Testabdeckung.** Kein Test im Repo konstruiert eine
   Gruppe mit zusammengesetztem Glied — weder Unit noch Integration, Property
   oder Ende-zu-Ende. Deshalb ist der Defekt durch alle Härtungsrunden gekommen.

Die Einreihung selbst ist korrekt: Die synthetische Kennung landet im
Chord-Set, die Gruppe feuert also zum richtigen Zeitpunkt und mit der richtigen
Zahl. Kaputt ist ausschließlich die Ergebnis-Zusammenstellung — das erklärt die
Beobachtung, dass `children()` gleichzeitig Vollständigkeit meldet.

**Jetzt behoben.** Die Gliedkennung wird beim Fan-in aus der Topologie
abgeleitet, statt an einer Stelle gesucht, an der sie für die halbe
Knotenwelt nie stand: `Signature` → `step_tasks`, `Chain` → rekursiv der
letzte Schritt, `Chord` → rekursiv der Body, `Group` →
`aggregate_result_id`. Der Eingriff liegt vollständig auf der Leseseite; das
Zustandsformat ist unverändert, die Golden-Fixture musste nicht neu erzeugt
werden. Dazu ein Guard, der eine unvollständige Gliederliste als Fehler
meldet statt auszuliefern — für **jede** Gruppe, auch die flache.

**Achtung beim Abgleich:** Der Fix ist **noch nicht veröffentlicht**. Für
jeden PyPI-Nutzer ist B10 in 0.2.0 und 0.3.0 weiterhin offen.

**Neu offen geblieben (KTD5):** `advance()` ist ein Read-Modify-Write ohne
CAS, `store_workflow_state` überschreibt das ganze Dokument blind. Zwei
nebenläufige Advances können einander Schritt-Marker löschen — vorbestehend,
bisher nur in `children()` sichtbar, durch den neuen Guard jetzt
betriebsrelevant. Ein strikter `xfail` in `tests/unit/test_workflow_advancer.py`
hält die Lücke fest. Die Behebung braucht einen eigenen Plan.

**Dateien:** `src/swarmq/workflow_orchestration.py`, `workflow.py`,
`tests/unit/test_workflow_advancer.py`,
`tests/integration/test_workflow_backend.py`, `tests/e2e/test_workflows.py`,
`tests/property/test_workflow_advancer_properties.py`

**Plan:** `docs/plans/2026-08-30-002-fix-workflow-group-composite-members-plan.md`

---

## Nicht behoben — weiterhin offen

| # | Thema | Warum offen |
|---|---|---|
| B1 | Stabile Slot-Identität in Signalen | `worker_id` ist weiterhin `id(self)`. Der Supervisor vergibt monoton wachsende IDs, auch beim Respawn — es gibt **überhaupt keine** slot-stabile Identität, auch nicht intern. Braucht ein Slot-Konzept, das freie Plätze recycelt. |
| B2 | Signal-Hook im CLI-Worker | `swarmq worker` baut die App weiterhin selbst und exponiert sie nicht. |
| B3 (2. Hälfte) | Port je Kindprozess | Teilt den fehlenden Unterbau mit B1. |
| B4 | Natives CPU-Offloading | Kein `run_in_executor`/`to_thread` im gesamten `src/`. Eigenständige Produktentscheidung. |
| B5 | Konfigurierbares `x-delivery-limit` | Weiterhin hart auf 20 in `broker/rabbitmq/delivery.py`. **Rückt durch 0.3.0 näher:** Jede von einer gerissenen Frist erzwungene erneute Zustellung zehrt an diesem Budget. |
| B7 | Signal für broker-seitiges Dead-Lettering | `TASK_DLQ` feuert weiterhin nur bei swarmq-eigener Übergabe. |
| B9 | Workflow-Folgeschritte umgehen Middleware und `TASK_ENQUEUED` | Während der 0.3.0-Arbeit neu in den Backlog gekommen, nie bearbeitet. |

---

## Bekannte Risiken, die 0.3.0 bewusst offenlässt

- **Alle vier Dead-Letter-Kommandos entnehmen Nachrichten zerstörend** — auch
  die diagnostisch gemeinten `list_dead_letters` und `inspect_dead_letter`.
  Reißt dort die Frist, ist der Verlust nicht auf einzelne Nachrichten
  begrenzt. Während einer Broker-Störung kein Dead-Letter-Kommando ausführen.
  Kein Arbeitspaket ändert diesen Pfad.
- **Der Startguard ist ein Breaking Change** für eine real laufende
  Konfiguration: Wer heute Mehrprozess mit festem Port fährt, hat einen
  degradierten, aber laufenden Worker und bekommt nach dem Upgrade einen
  Startabbruch.
- **`Publisher.set_publish_deadline`** ist neu im Vertrag. Der Worker ruft es
  über eine Schutzabfrage, ein älterer Fremdbroker fährt also weiterhin sauber
  herunter — bekommt aber die gekoppelte Frist nicht.

---

## Was beim Abgleich zu beachten ist

Ein neu gemeldetes Problem ist **nur dann** durch 0.3.0 abgedeckt, wenn es
exakt einen der drei behobenen Fälle trifft. Häufige Verwechslungen:

- *„Metriken fehlen im Mehrprozess-Betrieb"* → **nicht** behoben. 0.3.0 lehnt
  die Kombination nur noch sichtbar ab, statt sie still scheitern zu lassen.
- *„Publish hängt"* → behoben, **aber**: Ein Publish, der nach 120 s abbricht,
  ist das neue korrekte Verhalten. Wenn etwas jetzt schneller scheitert als
  vorher, ist das der Deckel.
- *„Task doppelt ausgeführt"* → hängt davon ab: Bei `unique_for` schützt 0.3.0
  jetzt. Ohne Unique-Sperre bleibt die Zustellung nach einem Fristbruch
  unbestimmt — das ist der dokumentierte Preis, kein Fehler.
- *„Logs zu groß"* → behoben, sowohl JSON als auch Text.
- *„Workflow-Ergebnis unvollständig / Glied fehlt"* → das ist B10. Im Code
  behoben, **aber in 0.2.0 und 0.3.0 weiterhin ausgeliefert** — bis der Fix
  veröffentlicht ist, ist eine solche Meldung berechtigt.
- *„Workflow scheitert mit `WorkflowValidationError` beim Fan-in"* → **neu**
  und gewollt: der Guard aus dem B10-Fix. Er trifft auch flache Gruppen und
  macht einen bisher stillen Verlust sichtbar. Wahrscheinlichster Auslöser
  ist KTD5 oder ein Upgrade mitten im Lauf.
