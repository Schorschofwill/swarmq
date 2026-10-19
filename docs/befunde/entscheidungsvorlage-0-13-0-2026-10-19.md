# Entscheidungsvorlage vor 0.13.0 final (2026-10-19)

Grundlage: main @ `c4be147` (0.13.0rc1 plus Nachträge !101), Register
`docs/befunde/fehlersuche-runde-4-2026-10.md` (Abschnitte „Entscheidung
überdenken?“, „Entscheidungen des Orchestrators“, „Spikes (U21)“, „Deferred
und Verdachte“). Jeder Punkt wurde gegen den Code geprüft, nur gelesen;
belegt mit Datei:Zeile in den Prüfberichten der drei Agenten (Zusammenfassung
hier). Die Loadsuite-Abnahme von 0.13.0rc1 ist bestanden (7/7, kein Verlust).

Antwortformat: je Nummer die Option, z. B. „1A, 2 ja, 3 ja, 4B …“.

## Teil 1: Vor 0.13.0 final

### 1. Dauerhafte Fehlerantwort im Chord-Skript am Zustelllimit (KD7/KD9, Punkt 5)

**Befund (bestätigt):** Eine Fehlerantwort wie `WRONGTYPE` auf dem Chord- oder
Ergebnis-Key lässt jeden Advance als vorübergehend scheitern
(`chords.py:69-77`, `BackendResponseError` ⊂ `BackendConnectionError`). Der
Schritt läuft rund 19-mal vollständig neu (`results.py:327-331`). Am Limit geht
die Nachricht ohne Fehlerergebnis in die DLQ, der Workflow bleibt `running` bis
zur TTL (`terminal.py:105-111`). Der Erfolg wird seit F4-01 nicht mehr
überschrieben.

**Reichweite:** `OOM`, `READONLY`, `LOADING`, `TRYAGAIN`, `CLUSTERDOWN`,
`MASTERDOWN` und `MISCONF` stuft swarmq schon als Ausfall ein
(`connection.py:52-79`). Sie sind nicht betroffen. `NOSCRIPT` lädt valkey-py
selbst nach. `CROSSSLOT` ist ausgeschlossen. Übrig bleibt nur ein falscher
Key-Typ auf dem Chord- oder Ergebnis-Key bei intaktem `sq:wf:{wf}`, also eine
manuelle Änderung, ein fremder Schreiber oder ein künftiger Bug.
Sichtbar ist der Fall schon heute: `workflow_advance_backend_error` mit
`workflow_id` und Fehlertext (WARNING, am Limit ERROR).

| Option | Verhalten | Aufwand |
|---|---|---|
| **A** bewusste Folge | wie heute | Register: Nachtrag E-040, KD7 als eigene Regel mit Marker. 0.12.4-Changelog: Überschrift „Release gap until 0.13.0“ und Zusammenfassung berichtigen (en/de). Kein Code. |
| **B** am Limit fail-markieren, nur bei `BackendResponseError` | Workflow wird `failed`, `get()` wirft `TaskError` mit dem `WRONGTYPE`-Text. Der Erfolg bleibt. Ist `sq:wf:{wf}` selbst kaputt, verhält sich swarmq wie heute. Die rund 19 Neuläufe bleiben. | Etwa 10 Zeilen in `terminal.py`, 2–3 Tests, kein Test mit `entscheidung`-Marker berührt. Register und Doku. |
| **C** nur sichtbarer | Ursache als Header in der DLQ-Kopie | klein, geringer Mehrwert |

**Empfehlung:** **A jetzt, B in 0.13.1.** Das neue Argument für B: Lock- und
Rate-Limit-Gate enden bei derselben Fehlerantwort schon mit `failed`; nur der
Advance-Pfad endet in `running`. Teilfrage zu B: Nach gelungener Markierung
**ACK ohne DLQ-Kopie** (wie `UnroutableMessage`, Empfehlung) oder zusätzlich
eine DLQ-Kopie? Nebenbefund für B: Bei FIRE_AND_FORGET-Schritten bleibt der
Workflow sofort `running`, ohne Limit. Das sollte B ausdrücklich mitnehmen.

**Stand 2026-10-19:** A in 0.13.0 umgesetzt; B umgesetzt in 0.13.1, Commit
`9fe9299` (Branch `fix/0-13-1`, unveröffentlicht): am Limit fail-markieren bei
`BackendResponseError`, ACK ohne DLQ-Kopie, FIRE_AND_FORGET sofort `failed`
(E-079, Nachtrag E-040).

### 2. Obergrenze für `TaskInfo`-Fristen (Folge-Lücke E-063)

**Befund:** `MAX_DEADLINE_S` (zehn Jahre) prüft nur `SwarmQConfig`
(`config_validation.py:29-32`). `TaskInfo.result_ttl`, `max_age`, `unique_for`
und `hard_timeout` haben keine Obergrenze (`task.py:171-200`,
`seconds.py:28-49`). Realistischer Fall ist `result_ttl=sys.maxsize` als
„für immer“: Mal 1000 ist das größer als int64. Valkey lehnt das `PX` ab
(`BackendResponseError`). Der Worker wiederholt das Speichern, stellt neu zu,
und **der Task läuft bei jeder Zustellung erneut** bis zum Zustelllimit, danach
geht er in die DLQ. Der Upgrade-Hinweis 3 von 0.13.0 verspricht die Grenze für
„jede Frist und TTL“.

**Option ja:** Bei Werten über `MAX_DEADLINE_S` wirft `TaskInfo` einen
`ValueError` beim Import, passend zu E-065 und E-057 (Exit 1). Klein: ein Test
je Feld, eine Changelog-Zeile. **In einem 0.13.x-Patch wäre das ein Breaking
Change**, deshalb gehört es in 0.13.0, also einen **rc2**.
**Option nein:** Den Upgrade-Hinweis 3 auf `SwarmQConfig` eingrenzen.

**Empfehlung:** **ja, mit rc2.** Die Abnahme von rc2 braucht nur die Smokes,
weil sich nur die Import-Validierung ändert (Staffelung).

### 3. Doku-Korrekturen vor final (kein Code)

**Empfehlung: alle umsetzen.**
- **a)** `SWARMQ_BACKEND_CONNECT_TIMEOUT_S` (0.12.4-Hinweis,
  `configuration.md:83`): Die Frist ist eine Obergrenze und keine Wartezeit.
  Ein verworfener Verbindungsaufbau endet nach höchstens rund 33 s; ein Wert
  darüber verlängert nichts. Die Doku verspricht heute mehr, als der Code tut.
- **b)** 0.13.0-Upgrade-Hinweis: `ExecutionGuardStore.peek` ist eine neue
  Pflichtmethode des öffentlichen Vertrags. Eine eigene Implementierung ohne
  `peek` endet am Zustelllimit mit `AttributeError`.
- **c)** `cli.md`: Nach `schedule remove` gilt für ein Neuanlegen mit gleichem
  Namen und gleichen Argumenten bis zum Ablauf des Claims
  `inserted:false, already_exists:false, duplicate_of:<eigener Name>`
  (steht in E-076, fehlt in der Doku).
- **d)** Register-Text zu Exit-Codes präzisieren: Exit 78 gibt es auch bei
  `schedule`, `dlq` und `inspect` für einen ungültigen `SWARMQ_NAMESPACE`
  (`cli.md` stimmt bereits).

### 4. Vier Entscheidungen des Orchestrators bestätigen

Alle vier sind wie beschrieben umgesetzt und getestet. **Empfehlung: alle
bestätigen.**
- **a)** `schedule add --cron` mit abgelehntem Ausdruck endet mit Exit `1`, nicht
  `78`. **Achtung:** Dabei wurde der Wortlaut deines Nachtrags E-065 vom
  2026-10-17 umgeschrieben (78 → 1). Das solltest du ausdrücklich bestätigen.
- **b)** `MAX_DEADLINE_S` gilt mit zehn Jahren für alle Fristen und TTLs, auch
  für reine Valkey-TTLs. Dazu Punkt 2.
- **c)** `delivery_limit` ist höchstens 2^63−1, darüber gibt es eine
  `ConfigurationError` (78).
- **d)** `already_exists` wird durch Lesen bestimmt; ein gehaltener Claim bleibt
  stehen, `duplicate_of` nennt den Halter. Dazu Punkt 3c.

## Teil 2: Neuer Fehler-Kandidat (Gegenprüfung nötig)

### 5. Rückstellungs-Gates bei gescheitertem Settle (bisher „E-043-Kandidat“, Punkt 4)

**Befund:** Das ist keine Entscheidung, sondern ein FEHLER, und er reicht
weiter als im Register beschrieben.
- **F4-17 (Log und Ausgang):**
  - Das Rate-Limit-, Lock-, Ausführungsschutz- und Backend-Defer-Gate fangen
    bei `retry_later` jede Ausnahme gleich.
  - Bei `SettleFailedAfterPublish` (Kopie liegt, nur das Ack des Originals
    scheiterte) loggen sie ERROR bzw. WARNING „Publish gescheitert“ und rufen
    `redeliver_in_place` auf dem toten Kanal.
  - `retry.py:172-185` behandelt denselben Fall richtig (F4-15).
  - `rate_limit_requeue_failed` und `lock_conflict_requeue_failed` fehlen im
    Logkatalog.
- **F4-18 (Unique-Freigabe):**
  - Das `nack` auf dem toten Kanal wirft. In vier Gates liegt der Aufruf
    ungeschützt im `try` der Pipeline, der Terminal-Default gibt das
    Unique-Flag frei (`pipeline.py:501-502`, `:598`).
  - Die Nachricht ist aber unbestätigt und kommt wieder. Das widerspricht
    E-001 und dem Kommentar `pipeline.py:634-641`.
  - Bei `unique_until=completion` öffnet sich das Dedup-Fenster, bevor der Task
    gelaufen ist.
  - Das tritt auch ohne `SettleFailedAfterPublish` auf, sobald Publish und
    `nack` beide scheitern, etwa bei einem Verbindungsabriss.
  - Per Probe belegt ist es für das Rate-Limit-Gate, für die drei anderen nur
    am Code.
- **Folgen:** kein Verlust; Fehlalarm und eine Dedup-Lücke. Mit abgeschaltetem
  Ausführungsschutz oder bei FIRE_AND_FORGET ist ein Doppellauf möglich. Das
  ist keine Regression, das Verhalten besteht seit Einführung der Gates.

| Option | Inhalt | Aufwand |
|---|---|---|
| A | nur Rate-Limit-Gate wie F4-15, Nachtrag E-043 | ~2 h |
| **B** | alle vier Gates plus `backend_defer` wie `retry.py`, `redeliver_in_place` abgesichert, Katalog ergänzt, E-043 auf alle Rückstellungs-Kopien | ½–1 Tag |
| C | B zentral in der Gate-Schleife der Pipeline (Gate-Ausnahme ergibt `Redelivered`) | wie B, berührt KTD5 |

**Empfehlung:** Gegenprüfung sofort nach `docs/befunde/README.md` und Aufnahme
als F4-17/F4-18. Fix nach **B**. Zeitpunkt: in **rc2**, wenn rc2 nach Punkt 2
ohnehin kommt (gleiche Abnahme, das Muster ist aus `retry.py` erprobt); sonst
0.13.1.

**Stand 2026-10-19:** Georg hat **B** entschieden. Gegenprüfung bestanden,
im Register als FEHLER F4-17 und F4-18 (P3) aufgenommen und in 0.13.0rc2
behoben (F4-18 f5bad41, F4-17 8c3b48f; Nachtrag E-043 vom 2026-10-19). Die
Probe hat die Unique-Freigabe für alle vier Gates bestätigt, nicht nur für
das Rate-Limit-Gate. Siehe `fehlersuche-runde-4-2026-10.md`, F4-17/F4-18 und
„Entscheidung überdenken?“ Punkt 4.

## Teil 3: 0.13.1 und später

### 6. E-059: Abbruch beim Speichern gibt den Claim frei (Punkt 1)

**Befund (bestätigt):**
- valkey-py sendet `EVALSHA` und liest dann; bei einem Abbruch schließt es nur
  die Verbindung.
- Das Skript kann also gelaufen sein und der Eintrag feuern.
- Trotzdem gibt der Abbruch den Claim frei (`client/schedule.py:697-705`,
  `cli/schedule_cmd.py:220-230`).
- **Client:** Eine Wiederholung legt ein echtes Duplikat an, weil jeder Aufruf
  eine frische ID bekommt.
- **CLI:** Gleicher Name scheitert an `HSETNX`, das Risiko ist kleiner.
- **Sofortiger Pfad:** Die gleiche Lücke gibt es dort (`client/schedule.py:320-333`).
- **Claim-Setzen:** Ein Abbruch hält das Flag schon heute (E-052).

**Optionen:**
- A: so lassen.
- B: nur verzögerter Pfad und CLI.
- **C: einheitlich, ein Abbruch gilt als unbestimmt, der Claim bleibt bis zur
  TTL**, auch im sofortigen Pfad und bei `schedule_many`, mit Nachtrag zu E-059
  und E-041. Aufwand etwa ½ Tag.
- D: abgeschirmt nachlesen; nicht empfohlen.

**Preis von C:** Die Sperre hängt bis `unique_for + Verzögerung`, ein
`get_result` auf eine ID, die nie lief, wartet bis zum Timeout. Das ist
derselbe Preis wie beim Ausfall nach E-041.

**Empfehlung:** **C in 0.13.1.** Das ist keine Regression, und die Änderung
blockiert eher, als dass sie etwas aufbricht.

**Stand 2026-10-19:** C umgesetzt in 0.13.1, Commit `f82ab70` (Branch
`fix/0-13-1`, unveröffentlicht): ein Abbruch beim Senden oder Speichern gilt
als unbestimmt, der Claim bleibt, auch im sofortigen Pfad und bei
`schedule_many` (Nachträge E-059, E-041, Verweis E-052).

### 7. E-075: PF-16 (b), Zeitabläufe beim TCP-Aufbau nicht wiederholen (Punkt 2)

**Befund:**
- Mit der Variante endet `connect()` gegen einen Host, der Pakete verwirft,
  nach 5 s statt nach 30 s.
- Damit wäre die Gesamtfrist im einzigen Fall wirkungslos, in dem sie beim
  Start heute entscheidet.
- Ein Worker-Start bricht dann nach 5 s ab. Das Budget des Supervisors (6
  Starts) ist nach rund 30 s statt 3 min verbraucht.
- Ein Port, der abweist (typisch bei Pod ohne Endpoints), endet schon heute
  nach 3,1 s.
- Der Laufzeitgewinn (178,8 s → 38,1 s) betrifft nur Befehle ohne
  `op_timeout`; der Hot Path hat schon 5 s.

**Optionen:**
- (a) nichts tun, nur die Doku nach 3a berichtigen.
- (b) die Variante wie im Spike.
- (c) die Variante plus eine Schleife bis zur Frist in `connect()`: Der Start
  wartet dann wirklich bis zur Frist, zur Laufzeit gilt die kürzere Zahl. Das
  sind rund 10–20 Zeilen mehr.

**Empfehlung:** Jetzt **(a)**; die Variante später als **(c)**, in 0.14 mit
Nachtrag E-075 und Toxiproxy-Test.

**Stand 2026-10-19 (0.14):** (a) in 0.13.0rc2 umgesetzt (Doku). (c) umgesetzt
auf Branch `feat/0-14` (U3, Commit `54c6bc6`, unveröffentlicht): Ein
Zeitablauf beim TCP-Aufbau wird im Verbindungsaufbau nicht wiederholt,
`connect()` wiederholt den `PING` danach bis zur Gesamtfrist und endet mit
`Valkey connect timed out after …s`; ein abweisender Port endet weiter nach
rund 3,1 s. Zur Laufzeit gemessen (skaliert, echtes SYN-Blackhole) 30 → 5
TCP-Versuche, auf Produktionswerte 178,8 s → rund 38 s. Mit der Vorgabe 30 s
ändert sich beim Start nichts; erst Fristen über rund 33 s warten länger.
Festschreibend ist ein `timing`-Test mit Blackhole, weil Toxiproxy keinen
SYN-Verwurf erzeugen kann; der Toxiproxy-Test ist Regressionswächter für den
Fall „stumm“ (`d1b3cbf`). Nachtrag 0.14 zu E-075, Changelog 0.14.0
Upgrade-Hinweise 1 und 2.

### 8. E-025: PF-19 (a), Kanal-Close bis `unblocked` zurückstellen (Punkt 3)

**Befund:**
- Je Kanalverwurf im Alarm fallen drei Logzeilen weg, darunter die aiormq-ERROR
  „Got frame for closed channel“, auf die Alarmregeln anspringen.
- Neu kommt eine aiormq-WARNING „Delivery tag … was ignored“.
- Korrektheit und Zustellung sind heute schon richtig.
- **Neu gegenüber dem Spike:** Die Variante berührt E-078. Reißt die Frist im
  Alarm, enden andere offene Confirms auf dem verworfenen Kanal eher bestätigt
  als unbestimmt. Das ist besser für die Zustellung, hält aber Slots länger.
  Nötig wäre ein Nachtrag zu E-078 neben E-025 (3).
- Der Integrationstest hängt an PF-1 (Chaos-Stack 4.3.6).

**Empfehlung:** umsetzen, aber in 0.14 zusammen mit PF-1, nicht vor final.

**Stand 2026-10-19 (0.14):** umgesetzt auf Branch `feat/0-14` nach PF-1
(Chaos-Stack 4.3.6, U5, Commit `b3b7d89`): U6, Commit `9253099`,
unveröffentlicht. Das Close wartet bei blockierter Verbindung auf `unblocked`
oder das Verbindungsende, höchstens die Publish-Frist. Nachtrag E-025 (3) und
Präzisierung E-078 (übrige Confirms enden bestätigt, wenn die Freigabe vor
ihrer eigenen Frist kommt, sonst mit ihr als `PublishIndeterminate`).
Integrationstest unter `exclusive` mit `memory_alarm`, Chaos-Variante mit
Toxiproxy-Abriss. Der Gewinn zeigt sich nur bei Alarmen, die kürzer sind als
die Publish-Frist; in `stoerung-1h` (60 s Alarm, 20 s Frist) bleibt die
aiormq-ERROR. Changelog 0.14.0 Upgrade-Hinweis 4, Katalogzeile für die
aiormq-WARNING.

### 9. Szenario mit langem Valkey-Ausfall in der Loadsuite

Seit der Backend-Smoke mit 15 s Ausfall läuft, fährt kein Lastszenario mehr den
Pfad „E-072-Wiederholung erschöpft, Neuzustellung“. Abgedeckt ist er nur noch
durch einen Combo-Test in swarmq.

**Empfehlung:** ein eigener Smoke mit rund 45 s Ausfall, ohne Eile. Er gehört
zu den Smokes für 0.13.1.

## Teil 4: kann warten (Einordnung „Deferred und Verdachte“)

Ohne Risiko für Nutzer vor final:
- Refactors (`fail_workflow`-Enum, doppelter Registry-Lookup)
- F4-15 ohne Ausführungsschutz
- F4-01 über das Cancel-Gate
- abgeleitete TTLs +300 s
- Anzeige bei `|` in der Worker-ID
- `ResultTail._store` und `CancelledError`
- `force_release` und `current_owner` (kein Aufrufer bzw. von E4 gedeckt)
- `ValkeySetpointStore`-Matrix
- gleichzeitige `connect()`-Aufrufe (Härtung 0.14)
- Handshake-Reset bei Konfigurationsfehlern
- Exit 1 statt 78 bei Env-Fehlern außerhalb von `worker`/`check`
- rund 50 Testlücken (eigene Runde)
- große Variante F4-02
- PF-1/PF-2

`DeadLetterScan` außerhalb von `swarmq.contracts` ist faktisch entschieden und
im Changelog dokumentiert; formal bestätigen.

## Vorgeschlagener Ablauf

1. Deine Antworten zu 1–9.
2. Gegenprüfung F4-17/F4-18, Aufnahme ins Register.
3. Ein Branch für 0.13.0rc2 mit diesen Teilen:
   - Doku-Punkte 1A und 3a–d
   - `TaskInfo`-Grenzen (2)
   - Fix F4-17/F4-18 (5B)
   - Register-Nachträge
   - Review, MR, CI
4. rc2 taggen, Loadsuite-Smokes, 0.13.0 final.
5. Danach 0.13.1: 1B, 6C, Smoke mit langem Ausfall (9).
6. Später 0.14: 7c, 8 mit PF-1.
