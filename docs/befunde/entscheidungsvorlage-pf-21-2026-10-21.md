# Entscheidungsvorlage PF-21: Delay-Stufe nach transientem Fehler (2026-10-21)

Grundlage: `main` @ `41c10d0` (0.14.2rc1). Register
`docs/befunde/fehlersuche-runde-4-2026-10.md` (PF-21, Status vom 2026-10-20),
Plan `docs/plans/2026-10-20-2100-fix-swarmq-0-14-2-plan.md` (R5, R6, KTD5,
KTD6, Befund 6 und 7, offene Fragen 4 bis 6). Jede Aussage zum Ist-Verhalten
ist gegen den Code geprüft, nur gelesen, ohne Probe gegen einen Broker.
Zeilen gelten für `41c10d0`, Pfade unter `src/swarmq/`.

Antwortformat: je Nummer die Option, z. B. „1 D, 2 ja, 3 ja …“.

## 1. Das Problem in Alltagssprache

Ein Worker verarbeitet die Queue `tasks`. Ein Task scheitert und soll in
10 s wiederholt werden. Die Kopie geht in die Delay-Stufe `tasks.DQ.15s`.

Jemand hat `tasks.DQ.15s` gelöscht, während der Worker lief (Aufräumen von
Hand, ein falsches Skript). Der Broker gibt die Kopie zurück. Der Worker
merkt das und legt die Stufe neu an. Genau in diesem Moment reißt die
Verbindung ab, oder der Broker braucht unter Last länger als die
Publish-Frist (120 s), um die Quorum-Queue anzulegen.

Was dann passiert:

- Diese eine Wiederholung scheitert laut (ERROR `retry_publish_failed`), die
  Nachricht wird neu zugestellt.
- Ab jetzt gehen alle Rückstellungen dieser Stufe still nach `tasks.DQ`, bis
  der Worker neu startet. Es gibt keine WARNING und keinen Wert im Gauge.
- In `tasks.DQ` gilt Head-of-Line-Blocking (E-026): Steht dort vorne eine
  Rückstellung über 2 h, wartet die 10-s-Wiederholung dahinter 2 h.
- Die Stufe existiert nach dem Abriss oft sogar wieder (der Broker hat das
  Anlegen zu Ende gebracht). Dieser Prozess nutzt sie trotzdem nicht.

Wie oft das vorkommt: selten. Es braucht eine gelöschte Stufe bei laufendem
Worker **und** einen Fehler genau beim Neu-Anlegen. Die Loadsuite erreicht
den Pfad nicht (Plan 0.14.2, Befund 8). Schaden: kein Verlust, aber stille,
unter Umständen stundenlange Verzögerung von Wiederholungen.

## 2. Ist-Verhalten (geprüft)

1. **Ziel einer Rückstellung.** `retry_later` wählt das Ziel über
   `_delay_target` (`broker/rabbitmq/delivery.py:690-703`). Eine Stufe ist
   Ziel nur, wenn sie in `_ready_delay_tiers` steht
   (`broker/rabbitmq/declarations.py:119-130`). Sonst gilt `{q}.DQ`.
2. **Unroutbar.** Ein `basic.return` ruft `invalidate_declaration(tier)`
   (`delivery.py:483`). Das nimmt die Stufe aus `_ready_delay_tiers`
   (`declarations.py:243-251`).
3. **Neu-Deklarieren im Hot-Path.** `publish_redeclaring_once`
   (`broker/rabbitmq/redeclare.py:30-49`) ruft `redeclare_on_throwaway_channel`.
   Das deckelt die Stufen-Deklaration mit der Publish-Frist
   (`redeclare.py:61`, `_effective_publish_timeout`, Default 120 s,
   `config.py:262`). Das eigentliche `Queue.Declare` hat sonst keine
   Client-Frist (`broker/rabbitmq/arg_conflict.py:108-110`); nur das Öffnen
   des Wegwerf-Kanals ist auf 10 s begrenzt (`broker/rabbitmq/connection.py:49`,
   `arg_conflict.py:179-204`).
4. **Was als Ablehnung gilt.** `_declare_delay_tier`
   (`declarations.py:137-172`) fängt nur `AMQPChannelError` und
   `QueueArgumentsMismatch` (`declarations.py:147`). Dann: Stufe in
   `_unavailable_delay_tiers` (`:157`), WARNING `delay_tier_unavailable`
   (`:158-169`), Gauge über `delay_tiers_unavailable` (`:132-135`,
   `worker/lifecycle.py:602-614`). Das ist E-026.
5. **Was durchgeht („transient“).** Alles andere: `ConnectionClosed`,
   `RuntimeError`/`ChannelInvalidStateError`, `BrokerConnectionError` aus dem
   Öffnen des Wegwerf-Kanals (`arg_conflict.py:200-204`), `TimeoutError` der
   Publish-Frist. **Dazu der strenge Mismatch:** Im Modus
   `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=at-least-once` wird ein
   `QueueArgumentsMismatch` auf `x-dead-letter-strategy`/`x-overflow` wieder
   geworfen (`declarations.py:148-152`). `redeclare.py:47-48` macht aus allem
   einen `BrokerConnectionError`.
6. **Folge für die laufende Rückstellung.** `worker/retry.py:180-185` loggt ERROR
   `retry_publish_failed` und stellt das Original neu zu
   (`redeliver_in_place`). Jede Neuzustellung zählt gegen `x-delivery-limit`
   (Default 20, `config.py:280`). Die Gates nutzen denselben Weg.
7. **Folge für spätere Rückstellungen.** Die Stufe steht in keiner der beiden
   Mengen. `_delay_target` liefert `{q}.DQ`. Kein Log, Gauge 0.
8. **Wann es heilt.** Beim Neustart (Deklaration beim Start,
   `worker/lifecycle.py:366`). Außerdem, wenn derselbe Prozess die Familie neu
   deklariert: Die Stufen-Schleife deklariert jede Stufe mit, die weder bereit
   noch abgelehnt ist (`declarations.py:115-117`). Das passiert nur nach einem
   unroutbaren Publish an `{q}.DQ` oder `{q}.XQ` (`redeclare.py:66-67`), also
   selten. „Bis Prozessende“ im Register stimmt darum fast, aber nicht ganz.
9. **Beim Start** ist ein transienter Fehler einer Stufe fatal für den Start
   (er läuft durch `declare_queue`, Exit 1, der Supervisor startet neu,
   E-057). Das soll so bleiben. Jede Änderung gilt nur für das
   Neu-Deklarieren im Betrieb (`redeclare_delay_tier`, `declarations.py:254-259`).

## 3. Was die Review-Punkte wirklich treffen

**P2 (hängendes Deklarieren, bis 120 s je Rückstellung): bestätigt.** Heute
zahlt nur die erste Rückstellung die Frist. Bei Option B zahlt sie jede
Rückstellung an die Stufe, solange das Anlegen hängt. Jede belegt so lange
einen Slot und stellt danach neu zu.

**P1 (Dauerschleife bis `.XQ`): im Kern bestätigt, Auslöser präzisiert.**

- Die Schleife bei B entsteht, wenn die Stufe fehlt und das Neu-Anlegen
  **dauerhaft** an einem Fehler scheitert, den der Code „transient“ nennt.
  Realistisch ist ein Anlegen, das wiederholt die Frist reißt (Quorum-Queue
  unter Last oder bei fehlendem Quorum). Dann gilt je Rückstellung:
  unroutbar, Neu-Deklarieren scheitert, ERROR, Neuzustellung, die Nachricht
  läuft erneut durch die Pipeline. Nach `x-delivery-limit` Zustellungen
  landet sie in `{q}.XQ`. Das ist P1 in der Sache.
- **Der strenge Mismatch erzeugt diese Schleife nicht.** Er tritt nur auf,
  wenn die Stufe beim Neu-Deklarieren **existiert**, also ein anderer Prozess
  sie inzwischen at-most-once angelegt hat (z. B. ein Worker im Modus
  `legacy`, oder von Hand). Dann ist der nächste Publish an die Stufe
  routbar und es wird gar nicht neu deklariert. Bei B ginge die Rückstellung
  also still in eine at-most-once-Stufe. **Der strenge Modus würde
  umgangen**, ohne Meldung. Das ist ein eigenes Risiko von B, aber keine
  Schleife.
- Heute (ohne B) endet der strenge Mismatch im Betrieb nicht mit Exit 78,
  sondern wie ein Abriss: eine Neuzustellung, danach still `{q}.DQ`.
  `{q}.DQ` ist beim Start im strengen Modus geprüft. Das ist also harmlos,
  aber nicht sichtbar.

Das Plan-Review hatte zu PF-21 nur diese zwei Punkte (P1, P2). Die
Erwähnung von „P3, P4“ in der Übergabe vom 2026-10-21 war ein Versehen des
Orchestrators; im Protokoll des Reviews gibt es dazu nichts.

## 4. Optionen

### A – dokumentieren

- **Verhalten:** wie heute. E-026 bekommt einen Nachtrag: Nach einem
  transienten Fehler beim Neu-Deklarieren bleibt die Stufe bis zum Neustart
  aus, ohne WARNING und ohne Gauge. Doku `delivery-semantics.md:783-785`
  wird genauer.
- **P1/P2:** treten nicht auf (es gibt keine Wiederholung).
- **Folgen:** stille Verzögerung bis zum Neustart bleibt.
- **Aufwand:** etwa 1 h, kein Code.
- **Tests:** keine Änderung. E-026 hätte damit eine stille Ausnahme von
  seinem eigenen Satz „WARNING, Gauge“ (siehe C).

### B – Stufe bleibt Ziel, nächster Bedarf deklariert neu (Plan 0.14.2, KTD5/KTD6)

- **Verhalten:** neuer Zustand „ausstehend“. Jede Rückstellung an die Stufe
  publiziert dorthin. Ist sie unroutbar, wird einmal neu deklariert.
- **P1:** **nicht gelöst** (Schleife bei dauerhaftem Fristriss, siehe oben).
- **P2:** **nicht gelöst** (bis 120 s je Rückstellung).
- **Weiteres Risiko:** strenger Modus still umgangen (siehe P1).
- **Aufwand:** ½–1 Tag (Plan U1, U3).
- **Empfehlung:** nicht umsetzen.

### C – transient wie Ablehnung

- **Verhalten:** Ein transienter Fehler beim Neu-Deklarieren im Betrieb
  macht die Stufe für den Prozess „nicht verfügbar“: WARNING
  `delay_tier_unavailable`, Gauge +1, `{q}.DQ` bis zum Neustart.
- **P1/P2:** gelöst, je Stufe genau ein Versuch.
- **Folgen:** Ein kurzer Netzaussetzer schaltet die Stufe bis zum Neustart ab.
  Sichtbar, aber nicht selbstheilend. Der Gauge-Hinweis „Ursache beheben und
  neu starten“ passt bei einem Abriss schlecht: Es gibt nichts zu beheben.
- **Regel:** Das ist die wörtliche Lesart von E-026 („Ein Fehler beim
  Deklarieren einer Stufe ist nicht fatal: Rückfall …, WARNING, Gauge“).
- **Aufwand:** 2–3 h. Den Fang nur in `redeclare_delay_tier`, nicht in
  `_declare_delay_tier`, sonst ändert sich der Start (Abschnitt 2, Punkt 9).
- **Tests:** keine Marker-Tests betroffen.

### D – Selbstheilung mit Mindestpause (Vorschlag des Orchestrators, präzisiert)

Der Vorschlag trägt, braucht aber vier Präzisierungen, damit P1 und P2
wirklich gelöst sind:

1. **Zustand „ausstehend“ mit Zeitpunkt des nächsten Versuchs**, je Stufe
   und Prozess. Nach einem unroutbaren Publish gibt es einen Versuch sofort,
   wie heute.
2. **Höchstens ein Versuch gleichzeitig je Stufe** (single-flight). Läuft
   einer, gehen parallele Rückstellungen sofort an `{q}.DQ` und warten nicht.
   Ohne das blockieren bei `concurrency` N bis zu N Slots gleichzeitig.
3. **Eigene, kurze Frist je Versuch:** 10 s (`declare_probe_timeout_s`), nicht
   die Publish-Frist von 120 s. Ein abgebrochenes Anlegen läuft beim Broker
   weiter. Der nächste Versuch findet die Stufe dann per passivem Declare.
   Während eines Shutdowns gilt die kürzere Restfrist.
4. **Die laufende Rückstellung geht bei einem Fehlschlag an `{q}.DQ`.**
   Achtung: Das ist **neu**, nicht „wie heute“. Heute scheitert sie mit
   `BrokerConnectionError` und wird neu zugestellt (Abschnitt 2, Punkt 6).
   Erst dieser Schritt beendet P1: kein Neulauf, kein Zustellzähler, keine
   Schleife. Doppelt wird nichts: Die erste Kopie hat der Broker
   zurückgegeben, die `.DQ`-Kopie ist die einzige. Ist die Verbindung ganz
   weg, scheitert auch der `.DQ`-Publish, und es bleibt beim heutigen
   `BrokerConnectionError`.

Dazu:

- **Pause:** Nach einem Fehlschlag gehen Rückstellungen an diese Stufe X s
  lang direkt an `{q}.DQ`, ohne Versuch. Die erste Rückstellung nach der
  Pause versucht es erneut, und zwar **erst Neu-Deklarieren, dann
  Publish**. So prüft die Kette auch eine Stufe, die ein anderer Prozess
  inzwischen angelegt hat (strenger Modus, Übernahme, Meldung
  `queue_dead_letter_at_most_once`). Ohne Rückstellungen gibt es keine
  Versuche.
- **Strenger Mismatch im Betrieb:** gilt als Ablehnung, wie E-026: Stufe aus,
  WARNING `delay_tier_unavailable` mit `error_type=QueueArgumentsMismatch`,
  Gauge +1, `{q}.DQ`. Nicht fatal. Exit 78 kommt beim nächsten Start (E-017).
  Ein Prozessende mitten im Betrieb gibt es in swarmq sonst nicht. E-083 hat
  ein automatisches Herunterfahren aus demselben Grund verworfen.
- **403 und andere Kanalfehler:** unverändert Ablehnung (E-026).
- **Sichtbarkeit:** neue WARNING `delay_tier_redeclare_failed` (`queue`,
  `tier`, `fallback`, `error_type`, `retry_in_s`) je gescheitertem Versuch,
  INFO `delay_tier_restored` bei Erfolg nach einem Fehlschlag. Der Gauge
  zählt weiter nur abgelehnte Stufen (Bedeutung laut `metrics.py:27-28`
  bleibt).

**Prüfung gegen die Review-Punkte:**

| Punkt | Ergebnis mit D |
|---|---|
| P1 Schleife bis `.XQ` | gelöst durch 4: keine Neuzustellung wegen der Stufe |
| P2 Slot je Rückstellung | gelöst durch 2, 3 und Pause: höchstens ein Slot für höchstens 10 s je X s, Stufe und Prozess |
| Begrenzung je Publish | höchstens ein Neu-Deklarieren je Publish, wie heute |
| Log-Flut | höchstens eine WARNING je X s, Stufe und Prozess; bei 7 Stufen (21 mit Prioritäten) und P Prozessen höchstens 21·P je X s, real fast immer eine Stufe |
| Rechte 403 | Ablehnung wie heute, E-16-Test bleibt grün |
| Mehrere Prozesse | Zustand je Prozess; jeder versucht selbst, Deklarationen sind idempotent. Höchstens P Versuche je X s und Stufe im ganzen Betrieb |
| Strenger Modus | erkannt, weil jeder Versuch die Kette läuft; kein stilles Umgehen wie bei B |
| Mischbetrieb 0.14.x | nur lokaler Zustand, kein Format, kein Header, kein Valkey-Schlüssel |

**Restrisiken von D:**

- In der Pause gilt `{q}.DQ` mit Head-of-Line-Blocking, höchstens X s am
  Stück, solange der Fehler nicht dauerhaft ist.
- Ein dauerhafter Fehler bedeutet `{q}.DQ` auf Dauer, aber mit einer WARNING
  alle X s statt still.
- Die Familien-Deklaration (`declarations.py:115-117`) deklariert eine
  ausstehende Stufe mit. Das ist gewollt (heilt mit). Ein transienter Fehler
  dort lässt wie heute die Familien-Deklaration scheitern. Ob sie Stufen in
  der Pause überspringt, klärt der Plan.

**Aufwand:** etwa 1 Tag: Zustand und Uhr (`self.clock()`, testbar ohne
`sleep`), Fang nur im Betrieb, Rückfall der laufenden Rückstellung, zwei
Logsätze mit Katalog EN/DE, `delivery-semantics.md` EN/DE, Changelog,
Nachtrag E-026. Abnahme nach Staffelung: Smokes und `stufe1-1h`
(Laufzeitpfad `retry_later`).

### E – Heilung im Hintergrund (Alternative, nicht empfohlen)

- **Verhalten:** Im Betrieb deklariert der Hot-Path keine Stufe mehr. Eine
  unroutbare Rückstellung geht sofort an `{q}.DQ`. Eine Hintergrundaufgabe
  je Prozess deklariert ausstehende Stufen alle X s neu, bis Erfolg oder
  Ablehnung.
- **Vorteile:** null Slot-Kosten. Passt am besten zu Review MR 10, P1-2
  („der Hot-Path kennt keine Kette“). Heilt auch ohne neue Rückstellungen.
- **Nachteile:** eine neue Hintergrundaufgabe mit Start, Abbruch und Shutdown;
  genau dort lagen viele frühere Befunde. E-002 sagt heute „Der Worker
  deklariert seine eigene Queue einmal neu und versucht es ein zweites Mal“.
  Für Stufen bräuchte das einen Nachtrag zu E-002 (die E-002-Marker-Tests
  betreffen nur `.DQ`, sie blieben grün). Die erste Rückstellung nach dem
  Löschen ginge immer an `{q}.DQ`, auch wenn das Neu-Anlegen sofort klappt.
- **Aufwand:** 1–1½ Tage.

### Übersicht

| | P1 | P2 | heilt selbst | sichtbar | strenger Modus | Aufwand |
|---|---|---|---|---|---|---|
| A | – | – | nein | nein | ok (still `.DQ`) | 1 h |
| B | **nein** | **nein** | ja | teils | **umgangen** | ½–1 Tag |
| C | ja | ja | nein | ja | ok | 2–3 h |
| **D** | ja | ja | ja | ja | ok | 1 Tag |
| E | ja | ja | ja | ja | ok | 1–1½ Tage |

## 5. Betroffene Regeln und Tests

**Tests mit `entscheidung`-Marker (dürfen nicht geändert werden).** Keine
Option ändert einen davon. Geprüft für D:

- E-026, Unit (`tests/unit/broker/test_delay_tiers.py`):
  `test_tier_that_cannot_be_declared_falls_back_to_the_delay_queue_with_one_warning`
  (Start, unverändert), `test_delay_over_one_hour_stays_in_the_delay_queue`,
  `test_tier_refused_after_an_unroutable_publish_sends_this_and_later_deferrals_to_the_delay_queue`
  (403 bleibt Ablehnung, Routing `[Stufe, .DQ, .DQ]` bleibt),
  `test_declaring_the_family_again_skips_ready_and_refused_tiers` (berührt
  keine ausstehende Stufe).
- E-026, Integration: `tests/integration/broker/test_delay_tiers.py::test_refused_tier_falls_back_to_the_delay_queue`.
- E-026, Gauge: `tests/unit/worker/test_delay_metrics.py::test_unavailable_delay_tiers_gauge_per_consumed_queue`
  (nur bei D und E unverändert, weil der Gauge seine Bedeutung behält).
- E-002 (`tests/unit/broker/test_unroutable.py`, drei Tests): betreffen die
  Familie über `.DQ`, nicht Stufen.

**Tests ohne Marker, die sich mit der Entscheidung ändern würden:**

- D (Punkt 4) und E: `test_connection_loss_while_declaring_a_tier_leaves_the_original_unacked`
  und `test_hanging_tier_declaration_is_bounded_by_the_publish_deadline`
  erwarten `BrokerConnectionError`. Mit D geht die Kopie an `{q}.DQ` und
  das Original wird bestätigt.
- Nur E: `test_unroutable_tier_is_declared_again_and_published_to_once_more`,
  `test_unroutable_tier_redeclares_only_the_tier_and_names_it` und
  `tests/integration/broker/test_delay_tiers.py::test_deleted_tier_is_declared_again_by_the_next_retry`.

**Regeln:**

- E-026: Nachtrag bei B, C, D, E (bei A ebenfalls, als bewusste Folge). Neue
  Marker-Tests mit `entscheidung("E-026")` und `befund("PF-21")`.
- E-002: Nachtrag nur bei E.
- E-017 (strenger Modus): bei D unverändert, der Nachtrag zu E-026 nennt den
  Betriebsfall. Bei B würde E-017 still verletzt.
- E-057 (Exit-Codes): unverändert; der Start bleibt wie heute.
- E-083: Vorbild für „kein automatisches Herunterfahren“.

**Doku:** `website/explanation/delivery-semantics.md:783-785` („if that
fails, `{queue}.DQ` is used“) ist heute ungenau: Die laufende Rückstellung
scheitert, erst die späteren gehen an `{q}.DQ`. Der Logkatalog
(`website/reference/logging.md:682`) nennt `delay_tier_unavailable` nur „at
worker start“, es kommt aber auch nach einem unroutbaren Publish. Beides
ziehen B bis E ohnehin nach; bei A gehört es zum Doku-Nachtrag.

## 6. Empfehlung

**D in der präzisierten Form, als eigener Patch nach 0.14.2 final.**

- D ist die einzige Option, die selbst heilt und dabei P1 und P2 schließt.
  Den Ausschlag gibt Punkt 4 (laufende Rückstellung an `{q}.DQ`): Er
  entfernt die Neuzustellung, die bei B die Schleife trägt.
- Gegenüber C: Ein Netzaussetzer soll eine Stufe nicht bis zum Neustart
  abschalten. Der Betreiber hätte nichts zu beheben.
- Gegenüber E: gleicher Nutzen für weniger neue Maschinerie. Die
  Slot-Kosten von D sind klein und begrenzt.
- Gegenüber A: E-026 verspricht für einen Fehler beim Deklarieren eine
  WARNING und einen Gauge. Heute fehlt beides. Stilles Verhalten über Stunden
  passt nicht zur Linie der letzten Runden.
- Nicht in 0.14.2: rc1 ist abgenommen bzw. in Abnahme, PF-21 ist P3 und
  selten.

## 7. Fragen an Georg

1. **Welche Option?** Empfehlung: **D** (präzisiert wie in Abschnitt 4).
2. **Laufende Rückstellung bei einem Fehlschlag an `{q}.DQ` statt
   `BrokerConnectionError` mit Neuzustellung?** Empfehlung: **ja**. Das löst
   P1, erzeugt keine Doppelkopie und ändert nur zwei Tests ohne Marker. (Die
   offene Frage 6 des Plans 0.14.2 hatte „nein“ empfohlen; damals ohne Pause
   und ohne P1 im Blick.)
3. **Mindestpause fest 30 s, ohne Konfigurationsfeld?** Empfehlung: **ja**.
   Ein neues Feld bräuchte Validierung (E-063) und Doku, ohne dass jemand
   es braucht.
4. **Frist je Versuch 10 s (`declare_probe_timeout_s`) statt der
   Publish-Frist?** Empfehlung: **ja**. Ein langsames Anlegen läuft beim
   Broker weiter, der nächste Versuch findet die Stufe.
5. **Strenger Mismatch im Betrieb wie eine Ablehnung (Stufe aus, WARNING,
   Gauge, nicht fatal), Exit 78 erst beim nächsten Start?** Empfehlung:
   **ja**, wie E-083 kein Herunterfahren im Betrieb.
6. **Sichtbarkeit: WARNING `delay_tier_redeclare_failed` je gescheitertem
   Versuch, INFO `delay_tier_restored`, Gauge zählt weiter nur abgelehnte
   Stufen?** Empfehlung: **ja**.
7. **Register: Nachtrag zu E-026 statt neuer Regel E-085?** Empfehlung:
   **Nachtrag E-026**. Dieselbe Regel beschreibt schon den Rückfall; der
   Nachtrag trennt „abgelehnt“ von „vorübergehend gescheitert“.
8. **Zeitpunkt: eigener Patch nach 0.14.2 final, Abnahme mit Smokes und
   `stufe1-1h`?** Empfehlung: **ja**. Der Pfad ist in der Loadsuite nicht
   erreichbar; die Läufe sichern den unveränderten Normalweg von
   `retry_later`.

## 8. Abweichungen von bisherigen Beschreibungen

- **„Bis Prozessende“** (Register PF-21): fast richtig. Auch eine erneute
  Deklaration der Familie durch denselben Prozess heilt die Stufe
  (`declarations.py:115-117`, `redeclare.py:66-67`).
- **„Die aktuelle Rückstellung geht einmal nach `.DQ` (wie heute)“** (Idee
  zu D): Heute geht sie nicht an `.DQ`, sie scheitert mit
  `BrokerConnectionError` und wird neu zugestellt (`redeclare.py:47-48`,
  `worker/retry.py:180-185`).
- **P1 „strikter Mismatch → Dauerschleife“:** Die Schleife bei B entsteht
  durch ein dauerhaft scheiterndes Anlegen einer fehlenden Stufe. Ein
  strenger Mismatch setzt eine vorhandene Stufe voraus; dann wäre der
  Publish bei B routbar und der strenge Modus würde still umgangen
  (Abschnitt 3).
- **„Ohne WARNING“:** Für die erste, gescheiterte Rückstellung gibt es heute
  WARNING `own_queue_unroutable_redeclaring` und ERROR `retry_publish_failed`.
  Still sind die späteren Rückstellungen.

## 9. Entscheidung (2026-10-22)

Georg: „ich folge deiner empfehlung bei pf-21“. Damit gilt je Frage die
Empfehlung:

1 D (präzisiert), 2 ja, 3 ja (fest 30 s), 4 ja (10 s), 5 ja, 6 ja,
7 Nachtrag zu E-026, 8 ja (Patch 0.14.3, Abnahme mit Smokes und
`stufe1-1h`).

Offen aus Abschnitt 4 (Restrisiken), vom Orchestrator für die Umsetzung
festgelegt: Die Familien-Deklaration überspringt eine ausstehende Stufe in
der Pause **nicht**; sie deklariert sie wie heute mit und heilt sie bei
Erfolg (INFO `delay_tier_restored`). Ein transienter Fehler dort lässt wie
heute die Familien-Deklaration scheitern.
