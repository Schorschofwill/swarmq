# Fix-Inventar: Debug-Feature-Backlog der Lasttest-Suite

**Stand:** 2026-08-30 · **Zweck:** Abgleichsgrundlage für neu gemeldete Probleme.

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
