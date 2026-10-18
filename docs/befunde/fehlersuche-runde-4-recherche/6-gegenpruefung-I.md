# Fehlersuche Runde 4 – Gegenprüfung Spur I (Injektion unter Last, U17)

Stand 2026-10-17, Worktree `test/fehlersuche-runde-4` (`src/swarmq/` = 0.12.3). Der Prüfer hat die Verdachte
nicht gefunden. Auftrag nach `docs/befunde/README.md`: jeden Verdacht widerlegen (Entscheidungs-Abgleich, Quelle
der Erwartung, Realismus), dann genau eine Einstufung. Grundlage: `2c-injektion.md` (U14), Rohlogs und Artefakte
im Loadsuite-Worktree `swarmq-loadsuite-stoerungen` (`runs/stoerung-smoke-20261017T132823*`,
`runs/stoerung-1h-20261017T135831*`), `docs/debug-feature-backlog.md` B26–B29.

Eigene Prüfungen: Code-Lesen in swarmq, valkey-py 6.1.1 und aiormq 7.0.0 (Projekt-venv); drei lokale Proben
gegen einen Fake-RESP-Server auf `127.0.0.1:0` (keine Testdienste, keine Testports, keine Sperre nötig), Skripte
und Ausgaben im Scratchpad der Sitzung unter `…/scratchpad/u17i/` (`probe_store_retry*.py`). Libvalkey wurde nur
in einer flüchtigen Umgebung (`uv run --with libvalkey==4.1.0`) zugeschaltet, das Projekt-venv ist unverändert.
Keine Integrations- oder Chaos-Läufe: Für R4I-02 und R4I-04 gibt es keinen bestehenden Test, der den Ablauf
nachstellt, und neue Tests anzulegen war nicht Teil des Auftrags.

## Ergebnis

| Verdacht | Einstufung | Prio | Kern |
|---|---|---|---|
| R4I-01 (Teil a, neu abgegrenzt) | FEHLER | P3 | Die Befehls-Wiederholung des Valkey-Clients (~3,1 s) greift nicht, wenn eine Pool-Verbindung tot ist und der sofortige Neuaufbau angenommen, aber im Handshake zurückgesetzt wird (Proxy vor einem neu startenden Valkey). Das Speichern scheitert nach 1–3 ms. Erklärt 6 der 11 Duplikate (beide Fenster mit Persistenz); Valkey war nach 0,8 bzw. 1,2 s zurück, die zugesagte Wiederholung hätte gereicht |
| R4I-01b (Rest) | ENTSCHEIDUNG | – (Überdenken: P3) | Scheitert das Speichern auch nach dem Wiederholfenster, wird die Nachricht zurückgegeben und der Task läuft komplett erneut. Das ist als Ack-nach-Store mit Neuzustellung dokumentiert (`architecture.md:216`, `logging.md:676`, Code-Kommentar KTD11). Erklärt die 5 Duplikate der Fenster ohne Persistenz (Ausfall 4,1–4,4 s > 3,1 s) |
| R4I-02 | LÜCKE | P3 | Dass Retry-Kopien im Alarm ihren Slot bis zur Publish-Frist halten und der Konsum einbricht, folgt aus E-025/E-041, steht aber in keiner Bewussten Folge und keiner Doku. Eine Zusage zur Konsumrate gibt es nicht |
| R4I-03 | FEHLER | P3 | Auf `SettleFailedAfterPublish` loggt `RetryHandler` ERROR `retry_publish_failed` („Retry could not be published“), obwohl die Kopie bestätigt liegt. Das widerspricht der Level-Regel aus 0.8.0 (`changelog.md:2574-2576`) und dem Katalogtext `logging.md:660` |
| R4I-04 | LÜCKE | P4 | Eine aiormq-ERROR-Zeile (`Got frame for closed channel`) nach dem Verwerfen des Publish-Kanals im Alarm, kein falscher Ausgang. Die Teilthese „Kanalnummer wird wiederverwendet“ ist NICHT HALTBAR als Befund: nicht gemessen, im gemessenen Ablauf nicht eingetreten, Folge nach Code harmlos |

Kein Verdacht widerspricht einem Test mit `entscheidung`-Marker. Berührt werden PF-16 und F3-33 (gleiche
Retry-Politik des Valkey-Clients, R4I-01a) sowie E-025/E-041 (R4I-02), E-043 (R4I-03), E-042 Nachtrag FS-08
(R4I-04). Für drei Punkte in `2c-injektion.md` ist die Sachlage anders als dort beschrieben (Abschnitt
„Korrekturen an 2c“).

---

## R4I-01 – Erfolgreicher Task läuft nach gescheitertem Ergebnis-Speichern komplett erneut

Die Gegenprüfung teilt den Verdacht in zwei Teile, weil zwei Ursachen zu den 11 Duplikaten führen.

### Neue Messung: welche Duplikate aus welchem Pfad

Alle 11 `backend_connection_error` der beiden Läufe, aus den Worker-Logs (`runs/*.logs/worker-*.log`):

| Lauf, Fenster | Valkey weg (Log) | Valkey bereit | Fehlerzeitpunkt(e) | Fehlertext | Dauer bis zum Fehler |
|---|---|---|---|---|---|
| Smoke, mit Persistenz | 13:49:38.618 „ready to exit“ | 13:49:39.427 | .639, .654, .730 | `Error while reading from toxiproxy:6379 : (104, 'Connection reset by peer')` | 3 ms nach `task_succeeded` (Beispiel `01a14a20-12ce…`: .636 → .639) |
| Smoke, ohne Persistenz | Proxy aus 13:52:08 | Fensterende | 13:52:11.67–.81 | `Error 111 connecting to toxiproxy:6379` | ≈ 3,1 s |
| 1 h, mit Persistenz | 14:42:47.917 | 14:42:49.144 | 14:42:47.952, .960, .973 | `Error while reading … (104, …)` | 35–56 ms nach Valkey-Ende |
| 1 h, ohne Persistenz | Proxy aus 14:53:47 | Fensterende | 14:53:50.975, 51.040 | `Error 111 connecting …` | ≈ 3,1 s |

Der swarmq-eigene Client hat eine Befehls-Wiederholung: `Retry(ExponentialBackoff(cap=2.0, base=0.05),
retries=5)` mit `retry_on_error=_TRANSIENT_ERRORS` (`backend/valkey/connection.py:241-249`), Backoff 0,1 + 0,2 +
0,4 + 0,8 + 1,6 s ≈ 3,1 s. In den Fenstern ohne Persistenz ist sie sichtbar abgelaufen (Fehler nach 3,1 s). In
den Fenstern mit Persistenz kam der Fehler nach wenigen Millisekunden, ohne jede Wiederholung.

### R4I-01a – Befehls-Wiederholung übersprungen, wenn der Neuaufbau im Handshake scheitert

**Einstufung: FEHLER, P3.**

**Mechanismus (valkey-py 6.1.1, gelesen und nachgestellt).** `Valkey.execute_command`
(`valkey/asyncio/client.py:622-651`) holt die Verbindung mit `pool.get_connection(...)` **vor**
`conn.retry.call_with_retry(...)`. `get_connection` → `ensure_connection` (`asyncio/connection.py:1105-1125`)
prüft eine Pool-Verbindung mit `can_read_destructive()`; ist sie tot (Server hat geschlossen), folgen
`disconnect()` und `connect()`. `connect()` (`:306-337`) wiederholt nur den TCP-Aufbau `_connect()`; der
Handshake `on_connect()` (CLIENT SETINFO, `:428-444`) liegt außerhalb jeder Wiederholung. Nimmt ein Proxy die
TCP-Verbindung an und setzt sie zurück, weil sein Upstream fehlt, wirft `on_connect` sofort
`ConnectionError`, und der Befehl wurde nie in die Retry-Schleife gegeben. Traceback der Probe:
`execute_command:628 → get_connection:1074 → ensure_connection:1123 → connect:326 → on_connect:444 →
read_response:601`.

**Probe** (`…/scratchpad/u17i/probe_store_retry*.py`, `ValkeyBackend.store_result` mit `op_timeout=5.0` gegen
Fake-Server; nach dem Basis-Speichern schließt der Server die Pool-Verbindung, neue Verbindungen nimmt er an und
setzt sie zurück):

| alte Pool-Verbindung | neue Verbindungen | Python-Parser | libvalkey 4.1.0 |
|---|---|---|---|
| per RST getrennt | angenommen, sofort RST | Fehler nach 3,11 s (wiederholt) | **Fehler nach 0,002 s** |
| per FIN geschlossen (wie Valkey beim Shutdown) | angenommen, sofort RST | **Fehler nach 0,001 s** | **Fehler nach 0,001 s** |
| per RST getrennt | Port abgewiesen (111) | – | Fehler nach 3,11 s (wiederholt) |
| Client-seitig abgebrochen | angenommen, sofort RST | Fehler nach 3,11 s | Fehler nach 3,11 s |

Die Fehlertexte stimmen mit dem Lauf überein (`Error while reading from <host> : (104, 'Connection reset by
peer')`). Der Loadsuite-Worker hat libvalkey installiert (`swarmq-loadsuite-stoerungen/pyproject.toml:9-10`),
nötig ist es aber nicht: Mit FIN auf der alten Verbindung scheitert auch der Python-Parser sofort. Ohne Proxy
(abgewiesener Port) greift die Wiederholung von `_connect()`, der Fehler tritt nicht auf.

**Quelle der Erwartung.**
- `website/reference/errors.md:198-201` (`BackendConnectionError`, Recovery): „Runtime connection losses are
  recovered automatically by the valkey-py reconnect loop — this exception fires only when the retry window is
  exhausted.“ Hier kommt die Ausnahme nach 1–3 ms, das Fenster wurde nicht einmal begonnen.
- `backend/valkey/connection.py:241-244`: „Reliability hardening (2026-04-17 toxiproxy spike): command-level
  retry on transient transport errors with exponential backoff capped at 2s.“
- Spike `docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md:44-50` (Spike 2): mit dieser
  Retry-Politik „sind SET/GET transparent gegen Outages … 0 Fehler“. Der Spike prüfte nur `proxy.down()/up()`
  (abgewiesener Port), so auch PF-16: „Entschieden ist nur die Befehls-Retry-Politik (Spike …, kurze Ausfälle
  mit abweisendem Proxy)“. Die Zusage in `errors.md` macht keine solche Einschränkung.

**Entscheidungs-Abgleich.** Kein Eintrag im Register regelt das Wiederholfenster des Valkey-Clients
(grep über `applies_when`/`tags`/`title` nach `retry`, `reconnect`, `valkey`, `backend`: nur E-005, E-012,
E-044, E-046 Nachtrag „Backend-Ausfall-Kopie“, E-052 – keiner betrifft das). Kein Test mit
`entscheidung`-Marker berührt den Pfad. CI installiert libvalkey nicht (`.gitlab-ci.yml:23-25`), und kein Test
prüft den Handshake-Fall.

**Realismus.** Mittel. Voraussetzung ist ein Zwischenglied, das annimmt und dann trennt, während Valkey neu
startet oder umschaltet: Toxiproxy, ein Service-Mesh-Sidecar (Envoy, Linkerd), HAProxy, ein Valkey-Proxy oder
ein Cloud-Endpunkt mit Proxy. Ohne Zwischenglied (abgewiesener Port) tritt es nicht auf. Folge je betroffenem
Aufruf: Speichern scheitert → vollständiger zweiter Lauf (6 von 11 Duplikaten der Läufe); Prüfungen vor der
Ausführung werden zurückgestellt (im Log `task_deferred_backend_unavailable` nach wenigen ms, harmlos);
Lock-Freigaben, Marker und Leader-Renews scheitern sofort statt nach 3,1 s.

**Widerlegungsversuche.**
1. *Artefakt von Toxiproxy?* Teilweise: Toxiproxy ist das annehmende Zwischenglied. Dieselbe Rolle haben
   Sidecars und Proxies in Produktion; die Zusage in `errors.md` gilt ohne Einschränkung.
2. *Nur mit libvalkey?* Nein, mit FIN auf der Pool-Verbindung (Valkey beendet Client-Verbindungen beim Shutdown
   regulär) scheitert auch der Python-Parser sofort (Probe).
3. *Hätte die Wiederholung überhaupt geholfen?* Ja: Valkey war nach 0,81 s (Smoke) bzw. 1,23 s (1 h) wieder
   bereit, das Fenster beträgt 3,1 s, und `op_timeout` ist 5 s.
4. *valkey-py-Fehler, nicht swarmq?* Die Ursache liegt in der Bibliothek, die Zusage gibt swarmq. Wie bei den
   Bibliotheks-Lecks TS-01..03 gehört der Fall ins Register, der Fix kann in swarmq liegen.

**Vorschlag Register-Zeile (P3 – FEHLER, ID ab F4-14).**
`F4-14 | Der Valkey-Client wiederholt einen Befehl nicht, wenn eine Pool-Verbindung tot ist und der sofortige
Neuaufbau angenommen, aber im Handshake getrennt wird (Proxy oder Sidecar vor einem neu startenden Valkey):
valkey-py führt get_connection/ensure_connection/on_connect außerhalb von call_with_retry aus
(asyncio/client.py:628, connection.py:1105-1125, :326). Der Fehler kommt nach 1–3 ms statt nach dem
Wiederholfenster von ~3,1 s. Im Störungslauf scheiterte so das Ergebnis-Speichern bei Valkey-Neustarts mit
Persistenz (Valkey nach 0,8/1,2 s zurück); 6 erfolgreiche Tasks liefen komplett erneut. Unabhängig vom Parser
(Python-Parser nach FIN, libvalkey auch nach RST) | errors.md:198-201 („this exception fires only when the retry
window is exhausted“); connection.py:241-244; Spike 2026-04-17:44-50 | Code: Backend-Aufrufe bei
BackendConnectionError aus dem Verbindungsaufbau im selben Fenster wiederholen (z. B. Pool-Unterklasse, die
ensure_connection unter conn.retry ausführt, oder Wrapper in ValkeyBackend._run nur für Fehler vor dem Senden);
zusammen mit PF-16 (b) planen | R4I-01 (Teil a)`.

**Nachweis-Test (Vorschlag).** `tests/integration/backend/test_befund_f4_14_reconnect_handshake_retried.py`:
Fake-RESP-Server auf `127.0.0.1:0` (kein Teststack), Basis-`store_result`, Pool-Verbindung serverseitig per FIN
schließen, 0,5 s lang jede neue Verbindung annehmen und zurücksetzen, danach normal bedienen. Fachliche
Assertion: `store_result` gelingt. Heute `BackendConnectionError` nach ~1 ms. Zweiter Test mit RST statt FIN,
der nur mit libvalkey rot ist (`pytest.importorskip("libvalkey")` reicht nicht als Ticket-Skip; besser in der
Chaos-Suite, siehe unten). Integration statt Unit, weil der grüne Fall bis zu 3 s wartet. Chaos-Variante
(`tests/chaos`, Toxiproxy): Valkey-Container hinter dem Proxy neu starten, Task-Körper zählt Ausführungen,
erwartet 1.

### R4I-01b – Speichern scheitert über das Wiederholfenster hinaus, der Task läuft komplett erneut

**Einstufung: ENTSCHEIDUNG** (dokumentierte Pipeline-Regel Ack-nach-Store), nicht im Register.
„Entscheidung überdenken?“ mit neuem Argument: P3.

**Sachverhalt bestätigt.** `ResultTail.finish` (`worker/results.py:305-321`): `store_result` wirft
`BackendConnectionError` → bei `AT_LEAST_ONCE` WARNING `backend_connection_error` und `redeliver_in_place`,
Outcome `Redelivered`; bei `FIRE_AND_FORGET` ERROR, Ergebnis verloren. `GuardedBroker.redeliver` gibt den
Running-Marker vor dem `nack` frei (`worker/guarded_broker.py:69-73`), die Neuzustellung startet frisch. Kein
Done-Marker, weil das Ack fehlt. Die 5 Duplikate der Fenster ohne Persistenz folgen genau diesem Pfad, der Ausfall
(4,1–4,4 s) war länger als das Wiederholfenster.

**Entscheidungs-Abgleich.**
- Register: keine Regel. E-046 (at-least-once) gilt laut `applies_when` nur für Schedfires, E-009 für Replays,
  E-043 für die DLQ-Kopie, E-041 für unbestimmte Kopien, E-044 für Fortschritt. Der Finder hat recht, dass
  keine E-Regel den Fall nennt.
- Nutzer-Doku, ausdrücklich: `website/explanation/architecture.md:216` (Failure domains, „Worker loses backend
  connection“): „a failed result write requeues the message, and the task runs again“ (seit 28bd8c4, Doku 0.9.0).
  `website/reference/logging.md:676` (`backend_connection_error`): „WARNING for `AT_LEAST_ONCE`: the message is
  redelivered and the task runs again. ERROR for `FIRE_AND_FORGET`: the result is lost.“ (seit 24b74b0, B23).
- Pipeline-Grundsatz: `website/explanation/delivery-semantics.md:67-91` („SwarmQ acks after the task's terminal
  state is durably stored … The ack-after-store sequence is the reason for the delivery guarantee“) und Vertrag
  `:1-38` („at-least-once … It does not promise: That the task runs exactly once“); `:607-630` „Idempotency
  expectations“.
- Code-Kommentar an der Fundstelle: `results.py:313` „Selbstheilend (KTD11): die Neuzustellung speichert erneut“,
  `:315-316` „Non-terminal — redelivery will re-run the task from scratch; keep the flag so next terminal path
  can release“ (Level-Regel KTD11 aus Plan `2026-10-06-0831-refactor-logging-haertung-plan.md:221-225`, umgesetzt
  in 24b74b0).
- Gegenstimme (Doku-Lücke, kein Widerspruch zur Regel): `delivery-semantics.md:233-234` „Duplicates appear in
  three situations“ nennt den Fall nicht; die Aufzählung ist unvollständig.

**Quelle der Erwartung.** Keine Zusage, dass ein erfolgreicher Task nach einem Speicherfehler nicht erneut läuft.
Die Zusage `delivery-semantics.md:26-28` („The task's result … will be written to Valkey“) erfüllt die
Neuzustellung gerade dadurch, dass sie erneut speichert.

**Ist eine längere Wiederholung des Speicherns mit der Pipeline vereinbar?** Ja, geprüft gegen
`docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md`:
1. Ack-Ordering („kein Broker-Verb vor dem letzten Gate, genau ein Verb pro Zustellung“): Eine Wiederholung in
   `finish` vor dem ersten Verb ändert die Zahl der Verben nicht.
2. Terminal-Outcome (`worker/outcome.py`): unverändert; Erfolg → `Terminal`, endgültiges Scheitern →
   `Redelivered` wie heute.
3. Owner-Checks, publish-then-mark: nicht berührt (Workflow-Advance folgt erst nach dem Speichern).
Grenzen, die eine solche Wiederholung einhalten muss: Slot bleibt belegt (wie die `nack`-Pause,
`delivery-semantics.md:148-152`); Frist unter `SWARMQ_CONSUMER_TIMEOUT_S`; im Drain endet sie am Drain-Ende
(E-018, E-001 Nachtrag); `SET … PX` ist idempotent, die Frage aus F3-33 (wiederholtes Skript nach verlorener
Antwort) stellt sich hier nicht; der Running-Marker läuft während eines langen Valkey-Ausfalls ohnehin ab
(`execution_guard_lease_lost`), das ändert die Wiederholung nicht.

**Realismus.** Hoch in der Häufigkeit (jeder Valkey-Ausfall über ~3 s trifft die gerade fertig werdenden Tasks,
B23: 40 Duplikate bei 30 s Ausfall), begrenzt in der Menge (Slots). Mit dem Fix von F4-14 bleiben nur Ausfälle
über dem Fenster.

**Widerlegungsversuche.**
1. *Ist der zweite Lauf vermeidbar ohne Änderung der Regel?* Nur über ein längeres Speicherfenster; das ist eine
   Produktentscheidung, keine Fehlerbehebung.
2. *Widerspricht `delivery-semantics.md:233` („three situations“)?* Die Liste ist eine Häufigkeitsaufzählung
   ohne Vollständigkeitszusage für das Verhalten; sie ist ungenau, nicht die Regel falsch. Doku-Fix.

**Vorschlag Register-Zeile (Abschnitt ENTSCHEIDUNG).**
`R4I-01b | Scheitert das Speichern des Ergebnisses nach dem Wiederholfenster des Clients (AT_LEAST_ONCE), wird
die Nachricht zurückgegeben und der erfolgreiche Task läuft vollständig erneut (Störungslauf ohne Persistenz:
5 Duplikate) | architecture.md:216 „a failed result write requeues the message, and the task runs again“;
logging.md:676; delivery-semantics.md:67-91 (ack-after-store); results.py:313-316 (KTD11, 24b74b0) | Doku:
delivery-semantics.md „Redelivery scenarios“ um den Fall ergänzen (vierte Situation, D-2), „three situations“
ändern; Regel als E-nnn ins Register (Ack-nach-Store, Neuzustellung bei Speicherfehler) | ja (PF-17):
Wiederholt wird nicht das Speichern, sondern die ganze Ausführung samt Nebenwirkung, für einen Fehler, der nur
die Ablage betrifft; eine begrenzte Wiederholung in finish ist mit den Pipeline-Invarianten vereinbar`.

**Produktfrage PF-17 (Überdenken).** Wie lange soll der Worker das Speichern eines erfolgreichen Ergebnisses
wiederholen, bevor er die Nachricht zurückgibt?
- (a) Nur das Client-Fenster (~3,1 s), nach dem Fix von F4-14. Regel ins Register, Doku ergänzen.
- (b) Zusätzlich eine eigene, begrenzte Wiederholung in `ResultTail.finish` mit dem Backoff der Zurückstellung
  (`SWARMQ_BACKEND_DEFER_BASE_DELAY` 1 s, verdoppelnd) bis etwa `SWARMQ_BACKEND_DEFER_MAX_DELAY` (60 s), gedeckelt
  durch Drain-Ende und Consumer-Timeout; Slot bleibt belegt.
- (c) Zurückstellung einer Kopie, die das Ergebnis trägt (neuer Header, Größenfrage, eigener Pfad im Worker).
- Empfehlung: (a) sofort mit F4-14; (b) als Entscheidung für Georg mit einer kurzen Obergrenze (etwa 10–15 s),
  weil sie die Nebenwirkung schützt und Valkey-Neustarts, Failover und Sidecar-Neustarts typischerweise
  darunter bleiben. (c) nicht, der Aufwand steht in keinem Verhältnis.

---

## R4I-02 – Retry-Kopien halten im Speicheralarm ihren Slot bis zur Publish-Frist; der Konsum bricht ein

**Einstufung: LÜCKE, P3.**

**Sachverhalt bestätigt.** `RetryHandler` publiziert die Retry-Kopie im Handler, also innerhalb des Slots
(`worker/retry.py:163-171`; Semaphor `worker/pipeline.py:723` bis `:795`). Bei blockierter Verbindung wartet
der Publish in `_wait_while_blocked` bis zur Aufhebung oder Frist (`broker/rabbitmq/delivery.py:332, 391-413`)
und endet mit `BrokerPublishBlocked`; dann `redeliver_in_place` (Pause, `nack`), das Original läuft erneut
(E-041). Rechnung und Messung des Finders stimmen (Frist 20 s: erwartet ≈ 33/s, gemessen 13–59/s gegenüber ≈ 190/s
vorher; mit 120 s rechnerisch ≈ 6,5/s, rund 3 %).

**Entscheidungs-Abgleich.**
- E-025 (`e025-blockierter-broker-publish-wartet.md`): „Ist die Verbindung blockiert, sendet ein Publish nichts,
  wartet bis zur Publish-Frist und scheitert dann mit `BrokerPublishBlocked` … Mit getrennten Verbindungen können
  Worker weiter quittieren.“ Bewusste Folgen nennen nur die `PublishIndeterminate`-Grenze und Verbindungslimits.
- E-041: „Die Publish-Deadline … deckelt den gesamten Publish-Vorgang“, „Scheitert so ein Retry- oder
  DLQ-Publish, bleibt die auslösende Nachricht unbestätigt und wird neu zugestellt“, Bewusste Folge
  „Zusatzlauf“. Das deckt den erneuten Lauf, nicht die Slot-Belegung.
- Quelle von E-025: Plan `2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:77` (Nachtrag MR 16, Georg
  2026-10-11) und `:584-594`: keine Abwägung von Durchsatz oder Slots.
- E-013 (Leader zählt Alarm nicht) und E-070 (Rate-Limit-Slot) betreffen andere Slots.
- Doku: `architecture.md:215` („a retry or dead-letter copy waits up to `SWARMQ_PUBLISH_TIMEOUT_S`, then the
  message is handed back“), `configuration.md:48` („so acknowledgements keep flowing“), `errors.md:561-583`
  (`BrokerPublishBlocked`), `deployment.md:240-244`. Keine Aussage zur Konsumrate oder zum Slot. Nur für die
  `nack`-Pause steht ausdrücklich „The task slot stays taken while it waits“ (`delivery-semantics.md:152`).

**Quelle der Erwartung.** Keine verletzte Zusage. Versprochen ist, dass Acks weiterlaufen; das traf zu (kein
Stillstand, Rückstand nach der Aufhebung abgebaut). Die Wirkung auf den Durchsatz ist eine ungenannte Folge
der Regeln E-025 + E-041, also eine LÜCKE, keine ENTSCHEIDUNG.

**Realismus.** Mittel. Ressourcenalarme sind selten, dauern aber oft Minuten; jede Fehlerrate über null reicht,
und die Wirkung wächst mit der Frist (Default 120 s).

**Widerlegungsversuche.**
1. *Hängende Acks (2b-Kriterium)?* Nein, gemessen liefen Acks; deshalb kein FEHLER gegen E-025.
2. *Würde ein sofortiges Scheitern helfen?* Es gäbe den Slot frei, aber das Original liefe dann sofort erneut
   (E-041) und wiederholte seine Nebenwirkung öfter. Kein eindeutig besserer Ausgang, also Produktfrage.
3. *Folgefrage Zustelllimit:* Die Rechnung des Finders („ein Task, der nur einmal gescheitert ist, erreicht nach
   rund 19 Runden das Zustelllimit“) stimmt so nicht. Läuft das Original nach `BrokerPublishBlocked` erneut und
   gelingt, wird es gespeichert und bestätigt (Acks laufen). Das Limit (`RedeliveryLimitError`, Gate eine Runde
   vor `x-delivery-limit` 20) erreicht nur ein Task, der bei **jedem** Lauf wieder scheitert, etwa weil eine
   externe Abhängigkeit ausgefallen ist; dann nach rund 19 × (Taskdauer + Frist + Pause), mit 120 s ≈ 38 min.
   Das ist realistisch (Retries sind oft Folge eines Ausfalls), aber nicht gemessen.

**Neues Argument für die Produktfrage.** Im Speicheralarm ist Konsum genau das, was dem Broker Speicher
zurückgibt (Queues leeren sich). Ein Konsum auf 3–25 % verlängert den Alarm, den E-025 überbrücken soll.

**Vorschlag Register-Zeile (LÜCKE).**
`PF-18 (R4I-02) | P3 | Was gilt für den Konsum eines Workers im Ressourcenalarm? Retry- und DLQ-Kopien warten im
Slot bis zur Publish-Frist (E-025), danach läuft das Original erneut (E-041). Bei jeder Fehlerrate über null fällt
der Konsum auf ≈ Slots / (Taskdauer + Fehlerrate × Frist): gemessen mit Frist 20 s auf ein Viertel, mit 120 s
rechnerisch rund 3 %. Ein Task, der bei jedem Lauf scheitert, geht nach rund 19 Runden als RedeliveryLimitError
in die DLQ (mit 120 s ≈ 38 min, nicht gemessen). Konsum leert im Alarm die Queues, ein Einbruch verlängert ihn |
(a) belassen, als Bewusste Folge in E-025 aufnehmen, in architecture.md/errors.md dokumentieren und eine kürzere
Worker-Frist empfehlen (Abwägung: mehr Zusatzläufe); (b) im Worker vor dem Publish den Blocked-Zustand prüfen und
die Kopie ohne Slot warten lassen (Nachricht bleibt unbestätigt, Prefetch begrenzt, Slot frei); (c) im Alarm den
Konsum der betroffenen Worker gezielt drosseln (Prefetch senken) statt Slots zu binden | (a) sofort (Doku und
E-025-Nachtrag); (b) prüfen, weil es Slot und Nebenwirkung schützt; mit einem Integrationstest auf der Fixture
memory_alarm (test_befund_bz16_connection_blocked.py) bewerten | offen (Georg)`.
Fix-Art: Register + Doku sofort, Code nur nach Entscheidung. Kein Nachweis-Test vor der Entscheidung; der
Reproduktionsvorschlag des Finders (`concurrency=2`, Frist 10 s, zwei scheiternde und zehn schnelle Tasks im
Alarm) taugt als festschreibender Test.

---

## R4I-03 – `retry_publish_failed` auf ERROR, obwohl die Retry-Kopie bestätigt in der Delay-Queue liegt

**Einstufung: FEHLER, P3.**

**Sachverhalt bestätigt.** `RetryHandler` fängt jede Ausnahme von `broker.retry_later` mit `except Exception`
und loggt `logger.error("retry_publish_failed", …)`, danach `redeliver_in_place` (`worker/retry.py:163-171`).
`SettleFailedAfterPublish` ist eine Unterklasse von `BrokerConnectionError` und bedeutet laut Docstring
(`exceptions.py:83-91`): „die Kopie ist veroeffentlicht (Publisher-Confirm), nur das Bestaetigen des Originals
scheiterte“. `GuardedBroker.retry_later` setzt dafür den Superseded-Marker mit Rückstands-TTL und wirft weiter
(`guarded_broker.py:76-87`). Ausgang korrekt: Kopie lief mit `attempt 2`, Original 14 ms später als `superseded`
übersprungen (Log des Finders). `redeliver_in_place` trifft den toten Kanal; die Lease ist schon genommen, der
Marker bleibt. Signal `TASK_RETRYING` und `on_retry` feuern vor dem Publish (`retry.py:136-144`), es fehlt nur die
INFO `task_retrying`.

**Entscheidungs-Abgleich.**
- E-043 regelt den gleichen Fall für die DLQ-Kopie: „Hat der Broker die DLQ-Kopie bestätigt und scheitert danach
  nur das Ack des Originals … Warnung `dlq_ack_failed_after_publish`“. Für die Retry-Kopie keine Regel, auch kein
  Nachtrag; Pläne 2026-10-12-1105 und 2026-10-14-0748 behandeln `SettleFailedAfterPublish` nur für DLQ/Expiry/Lock.
- E-001 Nachtrag FS-02a nimmt ERROR `retry_publish_failed` am Drain-Ende hin; dort ist die Kopie **nicht**
  gesendet. Anderer Fall.
- Kein Test prüft `retry_later` mit `SettleFailedAfterPublish` im `RetryHandler` (grep `tests/`: nur die
  Zustandsmaschine `retry_chain_machine.py:437` mit Fokus auf Marker, nicht Log).

**Quelle der Erwartung.**
- Changelog 0.8.0, `website/reference/changelog.md:2574-2576`: „Log levels follow one rule (ERROR: data loss or a
  failed task or process; WARNING: degradation that heals itself …)“. Hier: kein Verlust, kein zusätzlicher
  Task-Fehler (den meldet schon `task_failed`), der Retry ist erfolgt.
- Katalogtext `website/reference/logging.md:660`: „Retry could not be published; the message is redelivered.“
  Beides trifft nicht zu: Die Kopie liegt, und die Neuzustellung des Originals wird übersprungen.
- Gleiche Regel an anderer Stelle: E-043 / `dlq_ack_failed_after_publish` (WARNING, `logging.md:662`).

**Realismus.** Hoch bei jeder Broker-Trennung unter Last mit Retries im Flug (hier 1 Fall im `toxic_down` von
5 s). Wirkung: eine irreführende ERROR-Zeile je Fall, die Alarmierung auf ERROR auslöst und einen nicht
gesendeten Retry behauptet.

**Widerlegungsversuche.**
1. *„Bestehende Handler fangen sie weiter“ (`exceptions.py:90-91`) als bewusste Gleichbehandlung?* Der Satz
   begründet die Klassenhierarchie (kein Bruch für Aufrufer), nicht die Log-Stufe; für DLQ wurde derselbe Fall
   ausdrücklich anders behandelt.
2. *Ist ERROR wegen „a failed task“ gedeckt?* Der fehlgeschlagene Lauf ist mit `task_failed` gemeldet;
   `retry_publish_failed` meldet einen Publish-Fehler, den es nicht gab.

**Vorschlag Register-Zeile (P3 – FEHLER, ID F4-15).**
`F4-15 | RetryHandler (worker/retry.py:163-171) behandelt SettleFailedAfterPublish aus retry_later wie einen
gescheiterten Publish: ERROR retry_publish_failed („Retry could not be published; the message is redelivered“),
redeliver_in_place auf dem toten Kanal, kein task_retrying. Die Kopie liegt bestätigt in .DQ, der Ausführungsschutz
überspringt das Original als superseded (guarded_broker.py:85-87). Gemessen im toxic_down des 1-h-Laufs | Level-Regel
0.8.0 (changelog.md:2574-2576: ERROR nur für Datenverlust oder gescheiterten Task/Prozess); logging.md:660; gleiche
Regel E-043 (dlq_ack_failed_after_publish, WARNING) | Code: SettleFailedAfterPublish vor except Exception fangen,
WARNING (z. B. retry_ack_failed_after_publish), task_retrying loggen, kein redeliver_in_place, Outcome Redelivered;
Katalogzeile in logging.md (EN + DE); optional Nachtrag zu E-043 („gilt für die Retry-Kopie entsprechend“) | R4I-03`.

**Nachweis-Test.** `tests/unit/worker/test_befund_f4_15_retry_ack_failed_after_publish.py` nach dem Muster
`tests/unit/worker/test_retry.py:568` (`broker.retry_later = AsyncMock(side_effect=SettleFailedAfterPublish(...))`):
fachliche Assertionen „kein ERROR `retry_publish_failed`“, „`task_retrying` geloggt“, „`broker.redeliver` nicht
aufgerufen“.

---

## R4I-04 – aiormq-ERROR „Got frame for closed channel“ nach dem Verwerfen des Publish-Kanals im Alarm

**Einstufung: LÜCKE, P4.** Teilthese Kanalnummer-Wiederverwendung: NICHT HALTBAR als Befund.

**Sachverhalt bestätigt.** Nach `PublishIndeterminate` verwirft swarmq den Publish-Kanal und schließt ihn
entkoppelt mit 5 s Frist (`broker/rabbitmq/connection.py:535-548`, `_CHANNEL_CLOSE_TIMEOUT_S` `:75-77`). Im Alarm
liest der Broker die Verbindung nicht, das `Channel.Close` bleibt liegen; nach 5 s bricht aiormq das RPC ab
(WARNING „Closing channel … because RPC call <Channel.Close> cancelled“), entfernt den Kanal aus
`connection.channels` und swarmq loggt WARNING `publish_channel_close_incomplete` mit Traceback. Bei der Aufhebung
liest der Broker das Close und antwortet `CloseOk`; aiormq findet keinen Kanal und loggt ERROR
(`aiormq/connection.py:745-750`). Rohlog 1 h: io-1 Kanal 1 (`14:20:25.834`, 0,9 ms nach `unblocked`), io-2 Kanal 2
(`14:20:25.837`, 4,7 ms nach `unblocked`).

**Entscheidungs-Abgleich.** E-042 Nachtrag FS-08 und Commit 67cb34b: Kanal verwerfen ist gewollt. Die Annahme an
der Konstante (`connection.py:75-76`: „Kurz: gelingt es nicht, ist der Transport ohnehin tot und der Kanal
verwaist“) trifft im Alarm nicht zu, der Transport lebt. Keine Regel zu Fremdbibliotheks-Zeilen auf ERROR; B11
(Loadsuite) behandelt Formatierung, nicht Stufe. TS-01..03 (`teststrenge-2026-10.md:8-10`) sind Socket-Lecks
nach Verbindungsende, `aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md` verwaiste Reader-Tasks
zwischen Benchmark-Szenarien; beides ist ein anderer Mechanismus. Ein Leck entsteht hier nicht: aiormq entfernt
den Kanal, der Broker schließt ihn mit dem `CloseOk`.

**Quelle der Erwartung.** Keine. Die Level-Regel aus 0.8.0 gilt für swarmqs eigene Ereignisse; `logging.md:4-13,
39` beschreibt nur, dass Zeilen ab WARNING bei fehlender Konfiguration über `lastResort` erscheinen. Der Finder
schreibt, `publish_channel_close_incomplete` sei dokumentiert: Das stimmt nicht (`grep` über `website/` ohne
Treffer); der Katalog ist aber ausdrücklich eine Auswahl („They are a selection, not every event SwarmQ emits“,
`logging.md:604-605`), also kein Widerspruch.

**Kanalnummer-Wiederverwendung.** aiormq vergibt die Nummer als `max(channels) + 1`, bei leerer Tabelle den
zuletzt vergebenen Wert (`aiormq/connection.py:929-943`). Im Alarm warten neue Publishes in
`_wait_while_blocked`, **bevor** sie einen Kanal öffnen (`delivery.py:332-333` vor `:453`). Auf einer getrennten
Publish-Verbindung mit nur diesem Kanal ist die Tabelle nach dem Abbruch leer, die nächste Öffnung nach der
Aufhebung bekäme dieselbe Nummer. Ob die Öffnung vor dem `CloseOk` registriert wird, entscheidet ein Rennen von
wenigen Millisekunden. In beiden gemessenen Fällen kam das `CloseOk` zuerst (sonst gäbe es keine ERROR-Zeile).
Nach dem aiormq-Code wäre die Folge, dass das `CloseOk` den neuen Kanal schließt, dessen `open` scheitert und
E-042 auf einem neuen Kanal wiederholt (Nummer + 1, weil der Schlüssel mit `None` stehen bleibt); der Broker
hielte einen verwaisten Kanal bis zum Verbindungsende. Kein falscher Ausgang, keine Messung: als Befund nicht
haltbar, als Hinweis für einen künftigen Alarm-Test behalten.

**Realismus.** Mittel (jeder Alarm mit einem unbestimmten Publish je Verbindung). Wirkung: eine ERROR-Zeile ohne
Task-Kontext je Verbindung, eine WARNING mit Traceback.

**Vorschlag Register-Zeile (LÜCKE).**
`PF-19 (R4I-04) | P4 | Soll swarmq das Schließen eines verworfenen Publish-Kanals bei blockierter Verbindung bis zur
Aufhebung zurückstellen? Heute läuft das Close in die 5-s-Frist (WARNING publish_channel_close_incomplete mit
Traceback), aiormq loggt bei der Aufhebung ERROR „Got frame for closed channel N: <Channel.CloseOk>“; Ausgang
korrekt, kein Leck. Nebenhinweis ohne Messung: Kanalnummer kann wiederverwendet werden, Folge nach Code ein
gescheiterter Öffnungsversuch | (a) Close im blockierten Zustand erst nach unblocked senden bzw. die Frist dann
erst starten; (b) belassen und die aiormq-Zeile im Event-Katalog erklären (sowie publish_channel_close_incomplete
aufnehmen, ohne Traceback bei TimeoutError im Alarm); (c) aiormq-Logger für diese Meldung filtern | (b) sofort
(Doku), (a) mit einem Integrationstest auf memory_alarm, der auch die Nummernvergabe prüft | offen (Georg)`.
Fix-Art: Doku; Code nur nach Entscheidung. Kommentar `connection.py:75-76` in jedem Fall berichtigen.

---

## Tabellenkorrekturen T-1 bis T-6 und Doku-Hinweis D-1

| Punkt | Bewertung | Begründung |
|---|---|---|
| T-1 `broker_alarm` | **richtig im Ergebnis, Begründung korrigieren** | Gemessen: Smoke-Runner 0 `PublishIndeterminate`, 1-h-Runner 3, Worker je der erste. Die Erklärung „eine publizierende Verbindung bekommt die Notifikation sofort, ohne Publish“ hält aber nicht: Im Smoke begann der Runner den Publish `…cb0604` 0,5 ms **vor** dem Alarm-Logeintrag des Brokers (`dispatch.jsonl`, `pending` −0,5 ms), `connection.blocked` kam 0,6 ms nach dem Alarm, und genau dieser Publish wurde 52,7 ms später **bestätigt** (`published`). Der auslösende Publish kann also vollständig gelesen und bestätigt werden. Formulierungsvorschlag: „Der Broker blockiert eine Verbindung, sobald sie im Alarm publiziert. Der auslösende Publish kann noch bestätigt werden (Smoke) oder bis zur Frist hängen (Worker); Publishes, die gesendet wurden, bevor `connection.blocked` beim Client ankam, enden als `PublishIndeterminate`. Je Verbindung also null, einer oder mehrere.“ |
| T-2 E-041-Folgelauf | richtig | Deckt sich mit E-041 („mindestens ein weiteres Mal“) und den Logs (`duplicate_delivery_skipped reason=done`). Ergänzen: ein zweiter Erfolg auch, wenn der Done-Marker vor der verspäteten Kopie abläuft (E-010). |
| T-3 Leader-Rücktritt | richtig | E-048: `N = max(1, 30 // 10 − 1) = 2`; Zeitgrenze `Start + lock_ttl − Marge`, Marge `min(backend_op_timeout, (ttl − heartbeat)/2) = 5 s` → 25 s (`scheduler/loop.py:196-213`). Ergänzen: Ein Renew-Fehler zählt nach dem Wiederholfenster des Clients (3,1 s), mit F4-14 hinter einem Proxy schon nach Millisekunden. |
| T-4 `valkey_data_loss_detected` | richtig | `_report_loop` → `_report_crons` → `_observe_epoch` alle `scheduler_seen_interval_s` = 60 s (`scheduler/loop.py:324-355`, `config.py:305`); keine Zeitzusage in `configuration.md:279-285` oder `logging.md:632`. |
| T-5 Duplikate bei `valkey_restart` | **teilen** | Mit Persistenz (Ausfall < 3,1 s): erwartet **keine** Duplikate; beobachtete sind F4-14. Ohne Persistenz bzw. Ausfall > 3,1 s: je `backend_connection_error` ein zweiter Lauf, hingenommen nach R4I-01b (bis zur Antwort auf PF-17 getrennt ausweisen). |
| T-6 `queue_max_length` | richtig | `overflow: reject-publish` lehnt erst ab, wenn die Queue `max-length` bereite Nachrichten hält; unbestätigte zählen nicht. Die Alternative `max_length: 0` vor Gebrauch prüfen, wie vom Finder vermerkt. |
| D-1 E-025 „Grenze“, `errors.md:573-576` | **richtig, enger fassen** | Der erste Halbsatz von E-025 („RabbitMQ meldet die Blockade erst beim ersten Publish während des Alarms“) passt zu den Messungen. Falsch ist der zweite („dieser liegt schon auf dem Socket und endet als `PublishIndeterminate`“): Smoke-Runner 0 (auslösender Publish bestätigt), 1-h-Runner 3. Vorschlag: Nachtrag zu E-025 und `errors.md`/`errors.de.md` mit der Formulierung aus T-1; nur nach Georgs Ok (Register). |

## Suite-Artefakte S-1 bis S-7

| Punkt | Zu Recht kein swarmq-Verdacht? | Begründung |
|---|---|---|
| S-1 Gate-(c)-Riss | ja | Die Spitze selbst erholte sich im Budget (540/569 s von 600); die Überschreitungen folgen dem Abstand Fensterende → Spitzenende. Messart der Suite. |
| S-2 Alarm ohne Backlog | ja | Ohne Rückstand publizieren Worker keine Kopien; die Ack-Seite von E-025 bleibt ungeprüft. |
| S-3 `queue_max_length` ohne Überlast | ja | RabbitMQ-Semantik (T-6). |
| S-4 Leader-Wechsel nicht sichtbar | ja | swarmq loggt den Erwerb nicht (nur `scheduler_lock_lost`, `scheduler_acquire_error`, `loop.py:423, 448, 517`) und verspricht das nirgends; Signale existieren. Beobachtbarkeitswunsch, keine Zusage. |
| S-5 DLQ aus Mock-Fehlerrate | ja | Erschöpfte Retries gehen gewollt in die DLQ (`delivery-semantics.md:103`). |
| S-6 Runner-Logging über `lastResort` | ja | So zugesagt in `logging.md:4-13, 39`. |
| S-7 Backlog-Minimum | ja | Kalibrierung. |

## Abgleich mit offenen Lücken und Produktfragen

- **PF-16** (Verbindungs- vs. Befehls-Retry, `connect()` bis 2,9 min): F4-14 ist die Gegenseite desselben
  Retry-Aufbaus. Option PF-16 (b) („Verbindungs-Retry-Politik von der Befehls-Retry-Politik trennen“) muss den
  Handshake-Fall abdecken; F4-14, F4-12, F4-13 und PF-16 sollten in einer Änderung am Valkey-Client geplant werden.
  Testlücke Nr. 4 der Sichtung D (Retry-Politik des eigenen Clients) bekommt mit F4-14 einen konkreten Fall.
- **F3-33** (Retry von `lock_release.lua` nach verlorener Antwort): gleiche Retry-Politik; eine längere
  Wiederholung nach PF-17 (b) betrifft nur das idempotente `SET` und verschärft F3-33 nicht.
- **PF-1** (Chaos-Stack 3.13.7): für F4-14 ohne Belang (Valkey); PF-18/PF-19 hängen am Blockierverhalten 4.3.6.
- **Zwölf LÜCKEN aus Runde 2/3** (FS-25, FS-26, FS-29, FS-32, F3-32 bis F3-38, F3-41): außer F3-33 keine berührt.
- **Offene Registerfragen aus 2c** (E-041 für Zurückstellung und Ablauf-DLQ; Ausführungsschutz nach
  Valkey-Datenverlust als Regel): von dieser Gegenprüfung nicht berührt, bleiben bei Georg.

## Korrekturen an 2c-injektion.md

1. R4I-01: „jedes `backend_connection_error` … Eine kurze Wiederholung des Speicherns hätte alle 11 vermieden“ –
   eine Wiederholung existiert bereits (~3,1 s); sie wurde in den Fenstern mit Persistenz übersprungen (F4-14) und
   hätte in den Fenstern ohne Persistenz (4,1–4,4 s) nicht gereicht.
2. Beobachtung 1 / T-1: Im Smoke lag ein Publish an der Alarmgrenze (`…cb0604`), er löste die Blockade aus und
   wurde bestätigt; „ohne dass ein Publish dazwischen lag“ trifft nicht zu.
3. R4I-02 Folgefrage: nicht „ein Task, der nur einmal gescheitert ist“, sondern einer, der bei jedem Lauf scheitert.
4. R4I-04: `publish_channel_close_incomplete` steht nicht in `logging.md`.

## Zusammenfassung für U18

Neu ins Register: F4-14 (R4I-01a, P3, Nachweis Integration mit Fake-Server, optional Chaos), F4-15 (R4I-03, P3,
Nachweis Unit), ENTSCHEIDUNG R4I-01b mit PF-17 (Überdenken), LÜCKEN PF-18 (R4I-02, P3) und PF-19 (R4I-04, P4).
Register-Kandidaten: Ack-nach-Store mit Neuzustellung bei Speicherfehler (E-nnn), E-025-Nachtrag (D-1),
Bewusste Folge Slot/Konsum zu E-025 (je nach PF-18), Nachtrag E-043 für die Retry-Kopie (mit F4-15).
Loadsuite-Backlog: B26 → F4-14 und R4I-01b/PF-17, B27 → PF-18, B28 → F4-15, B29 → PF-19.
