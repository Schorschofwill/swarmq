---
date: 2026-05-30
topic: workflow-primitives
---

# Workflow-Primitive (chain / group / chord + Signatures)

## Summary

Composable Workflow-Primitive für SwarmQ: eine `Signature` als wiederverwendbarer
Baustein (`Task.s()`, `sig("name")`, immutable `.si()`) plus `chain` / `group` /
`chord` mit Operator-Syntax (`|`). Workflows starten fire-and-forget, liefern ein
über eine stabile `workflow_id` wieder-anbindbares Handle (`get(timeout)`,
`cancel()`, Group-Member-Introspektion) und werden dezentral von den Workern
vorangetrieben. Setzt R4.1 aus
`docs/brainstorms/implementation-order-requirements.md` um.

---

## Problem Frame

SwarmQ kann heute einzelne Tasks zuverlässig zustellen, retryen, deduplizieren,
raten-limitieren und canceln — aber jede Komposition mehrerer Tasks muss der
Anwender selbst von Hand verdrahten. Wer eine Pipeline „fetch → parse → store"
oder einen Fan-out „N Downloads parallel, dann ein Merge" bauen will, schreibt
heute eigenen Glue-Code: Task A enqueuen, irgendwie auf das Ergebnis warten,
es als Argument in Task B stecken, bei Fan-in selbst zählen, wann alle parallelen
Tasks fertig sind. Dieses „selbst orchestrieren" ist genau die Stelle, an der
Anwender subtile Fehler einbauen — verlorene Zwischenergebnisse, Races beim
Fan-in-Zählen, kein sauberer Abbruch, kein Wieder-Andocken nach Client-Neustart.

Etablierte Queues (Celery `chain/group/chord`, Dramatiq `pipeline/group`) lösen
das mit Workflow-Primitiven; ihr Fehlen ist die auffälligste funktionale Lücke
von SwarmQ gegenüber dem Stand der Technik. Gleichzeitig sind genau diese
Primitive in Celery die bekannte Quelle fragiler „Canvas"-Bugs (verlorene
Chord-Callbacks bei tiefer Verschachtelung, Memory-Blowups, nicht-deterministische
Completion-Detection). Die Aufgabe ist also nicht nur „Workflows bauen", sondern
„Workflows mit weniger Fußangeln als Celery bauen".

---

## Actors

- A1. Producer/Client: konstruiert Signatures und Workflows, ruft `.apply()` auf,
  hält (oder rekonstruiert) das Handle, fragt Ergebnis/Status ab, kann abbrechen.
- A2. Worker: führt die einzelnen Task-Schritte aus und treibt nach jeder
  Completion den Workflow voran (nächster Chain-Schritt, Chord-Counter, Chord-Body).
- A3. Valkey-Backend: hält Workflow-Topologie, -Status, Zwischenergebnisse und den
  atomaren Chord-Completion-Counter, damit Orchestrierung dezentral und
  client-unabhängig funktioniert.

---

## Key Flows

- F1. Chain (sequenziell)
  - **Trigger:** `await chain(A.s(x), B.s(), C.s()).apply()`
  - **Actors:** A1, A2, A3
  - **Steps:** Client persistiert Topologie + enqueued nur Schritt A → A-Worker
    führt A aus, reicht A-Ergebnis als erstes Argument an B weiter und enqueued B
    → analog B→C → C-Ergebnis wird als Workflow-Endergebnis abgelegt.
  - **Outcome:** `handle.get()` liefert C's Ergebnis; bei Fehler in einem Schritt
    bricht die Kette ab (siehe F4).
  - **Covered by:** R4, R10, R16

- F2. Group (parallel / Fan-out)
  - **Trigger:** `await group(D.s(u) for u in urls).apply()`
  - **Actors:** A1, A2, A3
  - **Steps:** Client enqueued alle Member gleichzeitig (Batch-Publish) →
    Worker führen sie unabhängig aus → jedes Member-Ergebnis wird unter dem
    Group-State abgelegt.
  - **Outcome:** `handle.get()` liefert die geordnete Liste aller Member-Ergebnisse;
    einzelne Member sind über `handle.children` introspizierbar.
  - **Covered by:** R5, R10, R13, R15

- F3. Chord (Fan-out + Fan-in)
  - **Trigger:** `await chord(group(D.s(u) for u in urls), Merge.s()).apply()`
    bzw. `group(...) | Merge.s()`
  - **Actors:** A1, A2, A3
  - **Steps:** Header-Group wird wie F2 enqueued → jeder fertige Header-Task
    erhöht atomar einen Completion-Counter in Valkey → der Worker, der den Counter
    auf „vollständig" setzt, enqueued den Body und übergibt ihm die Liste aller
    Header-Ergebnisse.
  - **Outcome:** `handle.get()` liefert das Body-Ergebnis; scheitert ein
    Header-Member, wird der Body nicht ausgeführt (siehe F4).
  - **Covered by:** R6, R11, R12, R13

- F4. Fehler / Abbruch
  - **Trigger:** Ein Schritt scheitert endgültig (Retries erschöpft) ODER
    `await handle.cancel()`.
  - **Actors:** A1, A2, A3
  - **Steps:** Workflow wird als `failed`/`cancelled` markiert → noch nicht
    enqueued Schritte werden übersprungen → laufende Tasks erhalten das bestehende
    Cancel-Signal → der Chord-Body läuft nicht.
  - **Outcome:** `handle.get()` propagiert die ursprüngliche Exception (failed)
    bzw. signalisiert `cancelled`.
  - **Covered by:** R16, R17

---

## Requirements

**Signatures**
- R1. `Signature` kapselt einen gebundenen Task-Aufruf (Task-Name + vorgebundene
  args/kwargs) als serialisierbares, wiederverwendbares Objekt. Es ist der
  einzige Baustein, aus dem chain/group/chord zusammengesetzt werden.
- R2. Zwei Erzeugungswege, die dasselbe Signature-Objekt liefern: die
  type-safe Klassenmethode `Task.s(*args, **kwargs)` und die string-basierte,
  import-freie Funktion `sig("task_name", *args, **kwargs)` für entkoppeltes
  cross-service-Enqueuing.
- R3. Immutable Signature via `Task.si(...)` / `sig(..., immutable=True)`: in
  einer chain wird das Ergebnis des Vorgängers an eine immutable Signature
  **nicht** als Argument durchgereicht; sie behält ihre fixen Args.

**Primitive**
- R4. `chain(*signatures)` — sequenzielle Ausführung; das Ergebnis jedes Schritts
  wird als erstes Argument des nächsten (mutable) Schritts eingefügt.
- R5. `group(*signatures | iterable)` — parallele Ausführung aller Member;
  Ergebnis ist die geordnete Liste der Member-Ergebnisse (Reihenfolge der
  Definition, nicht der Completion).
- R6. `chord(header_group, body_signature)` — Fan-out + Fan-in: der Body wird
  genau einmal ausgeführt, sobald alle Header-Member erfolgreich abgeschlossen
  sind, und erhält die Liste aller Header-Ergebnisse als erstes Argument.
  Zwischenergebnis-Lebensdauer: Member-Ergebnisse einer group/eines chord-Headers
  müssen mindestens so lange leben wie der Workflow selbst (eigene/erneuerte TTL,
  nicht die kürzere Per-Task-Result-TTL), damit langlaufende Fan-ins nicht auf
  abgelaufene Frühergebnisse treffen. Beim Einsammeln MUSS „expired" von „nie
  geschrieben" unterschieden werden — ein abgelaufenes Member-Ergebnis führt zu
  einem klaren Fehler (`WorkflowResultExpired`), nicht zu einer Liste mit stillen
  `None`-Löchern.
- R7. Operator-Syntax als Zucker über den Funktions-APIs: `A.s() | B.s()` ergibt
  eine chain; `group(...) | body.s()` ergibt einen chord.

**Komposition & Limits**
- R8. Flache Komponierbarkeit (DAG): eine chain darf eine group oder einen chord
  als Schritt enthalten; ein chord-Body darf eine chain/group sein. Beliebig
  tiefe rekursive Verschachtelung (chord-als-Header-eines-anderen-chord,
  group-in-group-in-group) ist **nicht** unterstützt.
- R9. Safety-Limits: `max_workflow_depth` (Default 10) und `max_group_size`
  (Default 1000); Überschreitung wird bei der Konstruktion mit einer klaren
  Exception abgelehnt. Wenn die volle Topologie zur Konstruktionszeit statisch
  bekannt ist (inline komponiert), greift die Validierung dort. Wird ein Schritt
  per `sig("name")` referenziert, der selbst zur Laufzeit in einen Sub-Workflow
  expandiert, ist die effektive Tiefe zur Konstruktionszeit nicht sichtbar — daher
  prüft zusätzlich ein Runtime-Guard vor dem Enqueue eines expandierten Sub-Schritts
  gegen `max_workflow_depth`, damit dynamisch zusammengesetzte Tiefe die flache-
  Komposition-Decke (R8) nicht umgehen kann.

**Ausführung & Orchestrierung**
- R10. `.apply()` ist fire-and-forget: es persistiert die Workflow-Topologie in
  Valkey, enqueued nur die initial lauffähigen Schritte und kehrt sofort mit einem
  Handle zurück. Der Workflow läuft unabhängig vom Client-Prozess weiter.
- R11. Worker treiben den Workflow voran: nach jeder Task-Completion entscheidet
  der ausführende Worker, was als Nächstes zu enqueuen ist (nächster Chain-Schritt,
  Chord-Counter erhöhen, Chord-Body enqueuen). Kein separater Orchestrator-Prozess.
  Workflow-Schritte laufen im `AT_LEAST_ONCE`-Modus (nicht FIRE_AND_FORGET, das vor
  der Ausführung acked und einen Mid-Orchestrierungs-Crash still verlieren würde).
  Der „Enqueue-des-nächsten-Schritts" wird über einen idempotenten Per-Schritt-Marker
  (`next-enqueued`) abgesichert, sodass eine Redelivery des aktuellen Schritts den
  Nachfolger nicht doppelt enqueued. Konsequenz, die dokumentiert werden muss:
  Schritt-Seiteneffekte müssen bei Redelivery idempotent sein (At-least-once-Semantik
  wie für alle SwarmQ-Tasks).
- R12. Chord-Completion-Detection ist atomar und idempotent gegen Redelivery: die
  Completion wird **pro Header-Member** erfasst (Member-Task-ID in ein Completed-Set,
  Kardinalität gegen N geprüft — nicht ein blindes INCR), sodass die Redelivery eines
  bereits gezählten Members ein No-op ist. Der Body-Enqueue ist über ein einmaliges
  `body-enqueued`-Flag abgesichert, das in derselben atomaren Lua-Operation gesetzt
  wird: genau ein Worker enqueued den Body, auch wenn mehrere Member quasi gleichzeitig
  fertig werden oder der letzte Header-Task nach dem Enqueue redelivered wird.

**Handle & Ergebnis**
- R13. `.apply()` liefert ein Handle mit Feldern/Methoden: stabile `workflow_id`,
  `await get(timeout=...)` (blockiert bis Endergebnis oder Timeout),
  `status`, und `cancel()`.
- R14. Reattach: `client.workflow(workflow_id)` rekonstruiert ein funktionsgleiches
  Handle (get/cancel/introspect) aus der `workflow_id`, auch nachdem der
  erzeugende Client-Prozess beendet ist. Fehlerkontrakt bei abgelaufenem oder
  unbekanntem State: `client.workflow(id)` wirft `WorkflowNotFound`, wenn der
  Workflow-State (Topologie/Status) nicht mehr existiert — es wird kein scheinbar
  funktionierendes Handle auf leeren State zurückgegeben. Teilweise abgelaufene
  Zwischenergebnisse werden über R5/R6 (expired-vs-missing) behandelt, nicht still
  als `None` geliefert.
- R15. Group-Member-Introspektion: das Handle einer group/eines chord-Headers
  exponiert die einzelnen Member (`children`) mit ihrem jeweiligen Status/Ergebnis,
  nicht nur das aggregierte Endresultat.

**Fehler & Abbruch**
- R16. Fail-fast: scheitert ein Schritt nach Retry-Erschöpfung endgültig, gilt der
  Workflow als failed; nachfolgende Schritte werden nicht enqueued. In einer group
  laufen bereits enqueued Member zu Ende, aber die Group gilt als failed und ein
  zugehöriger chord-Body wird nicht ausgeführt. `handle.get()` propagiert die
  ursprüngliche Exception. Liveness-Garantie: das endgültige Scheitern eines Schritts
  MUSS synchron im selben Pfad, der das Fehler-Result schreibt, den Workflow-State als
  failed markieren UND wartende `get()`-Aufrufe wecken (Notification), damit ein
  fehlgeschlagener Workflow nicht still in den `get(timeout)`-Timeout läuft. Der
  Fail-fast-Pfad darf nicht von asynchroner DLQ-Beobachtung abhängen.
- R17. `await handle.cancel()` bricht den ganzen Workflow ab: noch nicht enqueued
  Schritte werden übersprungen, laufende Tasks erhalten das bestehende Cancel-Signal
  (Wiederverwendung der vorhandenen Cancellation-Infrastruktur), ein chord-Body wird
  nicht ausgeführt. Race-Absicherung: `cancel()` setzt ein **workflow-level**
  Cancel-Flag (per `workflow_id`), das der Worker atomar in derselben Operation prüft,
  in der er über den nächsten Enqueue entscheidet — nicht nur die bestehenden
  Per-Task-Flags. So ist „noch nicht enqueued" wohldefiniert: ein bereits laufender
  Schritt N darf zu Ende laufen, aber N+1 wird nicht mehr enqueued, sobald das
  Workflow-Flag gesetzt ist.

---

## Acceptance Examples

- AE1. **Covers R4, R16.** Given `chain(A.s(x), B.s(), C.s())`, when B nach
  erschöpften Retries scheitert, then wird C nie enqueued und `handle.get()` wirft
  B's Exception.
- AE2. **Covers R3.** Given `chain(Fetch.s(url), Notify.si("done"))`, when Fetch
  ein Ergebnis liefert, then erhält Notify als Argument nur `"done"` — nicht
  Fetch's Ergebnis.
- AE3. **Covers R5.** Given `group(D.s(u) for u in [u1, u2, u3])`, when alle drei
  in beliebiger Reihenfolge fertig werden, then ist `await handle.get()` die Liste
  `[r_u1, r_u2, r_u3]` in Definitionsreihenfolge.
- AE4. **Covers R6, R12.** Given `chord(group(D.s(u) for u in urls), Merge.s())`
  mit N Headern, die parallel auf mehreren Workern abschließen, when der letzte
  Header fertig wird, then wird Merge **genau einmal** mit der Liste aller
  N Ergebnisse enqueued.
- AE5. **Covers R6, R16.** Given denselben chord, when einer der Header-Member
  endgültig scheitert, then wird Merge nicht ausgeführt und der chord gilt als failed.
- AE6. **Covers R10, R14.** Given ein per `.apply()` gestarteter chain, when der
  erzeugende Client-Prozess endet und ein anderer Prozess `client.workflow(id)`
  aufruft, then liefert das rekonstruierte Handle weiterhin `get()`/`cancel()`.
- AE7. **Covers R9.** Given eine `group(...)` mit mehr als `max_group_size`
  Membern, when sie konstruiert wird, then schlägt die Konstruktion sofort mit
  einer klaren Exception fehl (nicht erst zur Laufzeit).
- AE8. **Covers R17.** Given `chain(A.s(), B.s(), C.s())` mit laufendem A, when
  `handle.cancel()` aufgerufen wird, then wird A gecancelt und B/C nie enqueued.
- AE9. **Covers R1, R2.** Given `Fetch.s(url)` und `sig("fetch", url)`, when beide
  erzeugt werden, then liefern sie äquivalente, serialisierbare Signature-Objekte mit
  identischem Task-Namen und identischen gebundenen Args.
- AE10. **Covers R7.** Given `A.s() | B.s() | C.s()`, when ausgewertet, then ergibt
  sich dieselbe Workflow-Struktur wie `chain(A.s(), B.s(), C.s())`; `group(...) | M.s()`
  ergibt denselben chord wie `chord(group(...), M.s())`.
- AE11. **Covers R8.** Given `chain(A.s(), group(B.s(), C.s()), D.s())`, when
  konstruiert und ausgeführt, then läuft die eingebettete group parallel zwischen A
  und D; eine beliebig tief rekursive Verschachtelung (group-in-group-in-group) wird
  dagegen bei der Konstruktion abgelehnt.
- AE12. **Covers R11, R13.** Given ein chain, dessen Schritt A auf Worker W1 fertig
  wird, when W1 die Completion verarbeitet, then enqueued W1 selbst den nächsten Schritt
  B (kein separater Orchestrator); `.apply()` hatte zuvor sofort ein Handle mit stabiler
  `workflow_id`, `status` und `get(timeout=...)` geliefert.

---

## Success Criteria

- Ein Anwender kann „fetch → parse → store", „N parallel + merge" und „chain mit
  eingebettetem group/chord-Schritt" schreiben, ohne eigenen Orchestrierungs-Glue,
  Ergebnis-Weitergabe oder Fan-in-Zählung von Hand zu bauen.
- Chord-Completion ist unter paralleler Worker-Last race-frei: in Stress-/Chaos-Tests
  wird der Body bei wiederholten Läufen exakt einmal ausgeführt, nie null- oder
  mehrfach.
- Workflows überleben den Tod des erzeugenden Clients und sind per `workflow_id`
  wieder anbindbar.
- `ce-plan` kann ohne Rückfragen zu Produktverhalten implementieren: API-Form,
  Fehlersemantik, Komposition-Grenzen und Handle-Verhalten sind hier festgelegt;
  offen bleiben nur die unten markierten technischen Planungs-Fragen.

---

## Scope Boundaries

- `on_error` / `link_error`-Callbacks (Cleanup-/Alerting-Signaturen im Workflow):
  deferred — Fail-fast deckt v1 ab, Hooks sind ein additives Folge-Feature.
- Voll-rekursive Verschachtelung (chord-als-Header eines chord, beliebig tiefe
  group-in-group): bewusst ausgeschlossen, um die Celery-Canvas-Bug-Klasse zu
  vermeiden. Flache Komposition (R8) ist die bewusste Decke.
- Bulk-Throughput-Optimierung (R4.2 aus
  `docs/brainstorms/implementation-order-requirements.md`, `schedule_many`
  performant): eigenständiges Feature, nicht Teil dieser Primitive.
- `starmap` / `chunks` / `map`-Primitive (Celery-Extras): nicht in v1.
- Weitergabe sehr großer Zwischenergebnisse über Inline-vs-Referenz-Schwelle:
  Mechanismus wird in der Planung festgelegt (siehe Outstanding Questions), ist
  hier aber als Anforderung (Ergebnisse fließen korrekt weiter) bereits gesetzt.

---

## Key Decisions

- Fire-and-forget + Reattach statt blockierend-only: Workflows überleben den
  Client-Tod und skalieren (keine N blockierten Verbindungen). Preis: Topologie und
  State müssen in Valkey leben, nicht im Client-Objekt.
- Flach-komponierbar (DAG) statt voll-rekursiv: bewusst gegen die ursprünglich
  erwogene Celery-Style-Rekursion entschieden, um deren Bug-Klasse nicht
  einzukaufen. Deckt die gängigen realen Muster (chain-of-groups, group-in-chain) ab.
- Reichweite des „weniger Fußangeln als Celery"-Versprechens: Es bezieht sich konkret
  auf die durch flache Komposition vermiedene Klasse (keine verlorenen, tief
  verschachtelten Chord-Callbacks, kein Memory-Blowup). Die *exactly-once*-Ausführung
  des Chord-Bodys unter Redelivery ist NICHT umgangen, sondern das beibehaltene harte
  Problem, das dieses Design über R12 (idempotente Per-Member-Completion) lösen muss —
  kein Footgun, der weggelassen wird.
- Fail-fast statt Continue-best-effort: vorhersehbare, debugbare Semantik; folgende
  Schritte rechnen immer mit gültigem Input.
- Worker-getriebene Orchestrierung statt separatem Orchestrator: konsistent mit dem
  bereits in den Worker eingebetteten Scheduler; keine zusätzlichen Moving-Parts.
- Beide Signature-APIs (`.s()` + `sig()`) plus immutable `.si()`: dünne Wrapper um
  ein Objekt, decken In-Process- (type-safe) und cross-service- (entkoppelt) Nutzung ab.
- Workflow-Cancel über die vorhandene Cancellation-Infrastruktur wiederverwenden,
  statt einen zweiten Cancel-Mechanismus zu bauen.
- Aktivierung von R4.1 ohne treibenden Use-Case (bewusster Override der
  Origin-Bedingung, 2026-05-30): Es existiert kein konkreter Workload, der die
  API-Form klärt. Begründung für den Bau dennoch: API-Vollständigkeit / Parität zu
  Celery/Dramatiq (Schließen der auffälligsten funktionalen Lücke). Die Form orientiert
  sich daher an den etablierten chain/group/chord-Mustern statt an einem eigenen
  Workload. Akzeptiertes Risiko: trifft später ein realer Use-Case auf eine andere
  Form, kann API-Rework nötig werden. Trotz fehlenden Use-Cases wird der **volle**
  v1-Umfang gebaut (inkl. Operator-Syntax R7, immutable `.si()` R3, Group-Introspektion
  R15) — bewusst gegen den schlankeren Kern entschieden.

---

## Dependencies / Assumptions

- Setzt auf vorhandene Infrastruktur auf (im Code verifiziert): `schedule` /
  `schedule_many` mit Publisher-Confirms (`src/swarmq/client.py`), Result-Backend
  mit `store_result` / `get_result` / `get_results_batch` / `publish_notification` /
  `subscribe_and_wait` (`src/swarmq/backend/valkey.py`), bestehende
  Cancellation (`src/swarmq/cancellation.py`) und Lua-Script-Lade-Infrastruktur
  (`src/swarmq/lua/`).
- Ein atomares Chord-Completion-Lua-Script existiert **noch nicht** und ist neu
  zu erstellen (verifiziert: `src/swarmq/lua/` enthält locks/scheduler/unique/
  sliding-window, aber keinen Chord-Counter).
- Signature-, chain-, group-, chord-, `sig`-Symbole existieren heute **nicht** im
  Code (verifiziert) — komplett net-new, inkl. neuer `__init__.py`-Exports.
- Annahme: Workflow-State nutzt das bestehende Result-TTL-Konzept
  (`SWARMQ_RESULT_TTL`); ob Workflow-Topologie eine eigene/längere TTL braucht,
  ist in der Planung zu klären.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R12][Technical] Exakte Lua-Struktur für die atomare
  Chord-Completion-Detection (Counter-Schlüssel, Ergebnis-Sammlung,
  Idempotenz beim Body-Enqueue bei Redelivery des letzten Header-Tasks).
- [Affects R4, R6][Technical] Schwelle und Mechanismus der Ergebnis-Weitergabe
  inline vs. Valkey-Referenz (aufgabe.md nennt ~64KB als Orientierung).
- [Affects R10, R14][Technical] Schema und TTL der persistierten
  Workflow-Topologie/Status in Valkey; Verhalten bei abgelaufenem/teilweise
  abgelaufenem Workflow-State beim Reattach.
- [Affects R16][Technical] Wie wird ein endgültiger Task-Fehler an den
  Workflow-State zurückgemeldet (Hook im Worker-Completion-Pfad vs.
  DLQ-Beobachtung), damit Fail-fast greift, ohne mit dem bestehenden
  Retry/DLQ-Pfad zu kollidieren.
- [Affects R8, R9][Technical] Repräsentation der DAG-Topologie und Stelle der
  Tiefen-/Größen-Validierung (Konstruktionszeit) inkl. klarer Fehlermeldung.
