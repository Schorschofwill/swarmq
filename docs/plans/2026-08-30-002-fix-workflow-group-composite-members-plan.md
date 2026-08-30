---
title: group verliert zusammengesetzte Glieder - Plan
type: fix
date: 2026-08-30
topic: workflow-group-composite-members
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: legacy-requirements
execution: code
---

# `group` verliert zusammengesetzte Glieder - Plan

## Goal Capsule

- **Objective:** Eine `group`, deren Glied selbst eine `chain`, `group` oder ein `chord` ist, liefert wieder das, was ihr Klassenvertrag zusagt — die geordnete Liste **aller** Gliedergebnisse. Heute fehlt das zusammengesetzte Glied stillschweigend, und derselbe Defekt füttert einen Chord-Body mit gekürzter Eingabe.
- **Product authority:** Der Product Contract in diesem Dokument, abgeleitet aus B10 des Debug-Feature-Backlogs (`docs/debug-feature-backlog.md` im Repo `swarmq-loadsuite`, Commit `576279f`) und aus der Verifikation am Code.
- **Klasse des Fehlers:** Stiller Korrektheitsfehler, nicht Fähigkeitslücke. Der Aufrufer kann eine gekürzte Liste von einer vollständigen nicht unterscheiden; der Fehler wandert unbemerkt in die nachgelagerte Rechnung. Deshalb gehört ein Guard dazu, der jede verbleibende Instanz dieser Klasse laut macht, statt sie auszuliefern.
- **Nicht aktiver Scope:** Die übrigen offenen Backlog-Einträge (B1, B2, B3-zweite-Hälfte, B4, B5, B7, B9) sind Kontext, nicht Anforderung. B9 berührt denselben Advancer, ist aber ein eigener Gegenstand (Middleware und Signal statt Ergebnis-Zusammenstellung).
- **Execution profile:** Python ≥ 3.14, uv-Toolchain. Eine Datei trägt die Ursache (`src/swarmq/workflow_orchestration.py`); der Rest ist Testabdeckung, die es für diesen Fall bisher überhaupt nicht gibt. Keine neuen Abhängigkeiten.
- **Execution note:** Aus dem Fehlschlag heraus arbeiten. Für zusammengesetzte Gruppenglieder existiert im gesamten Repo kein einziger Test — der erste Schritt ist ein Test, der den Defekt an `v0.3.0` reproduziert, nicht die Korrektur.
- **Stop conditions:** Stoppen und rückfragen, wenn (a) der Vollständigkeitsguard bestehende Tests bricht, die eine heute gültige Topologie absichern, (b) sich zeigt, dass die Gliedkennung ohne Änderung am Chord-Set nicht crash-sicher zu halten ist, (c) das gewählte Zustandsfeld einen Zustandsformat-Bruch für laufende Workflows erzwingt.
- **Tail ownership:** Nicht Teil dieses Plans — Release-Schnitt und Release-Notes-Text entscheidet Georg nach der Umsetzung.
- **Open blockers:** Keine. KTD1 trägt eine Empfehlung mit Begründung; die Alternative ist dort benannt, falls Georg anders entscheidet.

---

## Product Contract

### Summary

Das Ergebnis eines zusammengesetzten Gruppenglieds bekommt eine im Workflow-Zustand festgehaltene Kennung, damit die Fan-in-Zusammenstellung es findet. Dazu ein Guard, der eine unvollständige Gliederliste als Fehler meldet statt als Ergebnis, und die Testabdeckung, die für diese Topologie-Familie bisher fehlt.

### Problem Frame

`Group` sagt zu, ihr Ergebnis sei „die geordnete Liste der Gliedergebnisse in Definitionsreihenfolge". Für ein Glied, das selbst zusammengesetzt ist, hält swarmq diese Zusage seit `v0.2.0` nicht ein.

Der Weg dorthin, am Code bestätigt:

1. `_frontier` (`src/swarmq/workflow_orchestration.py:287`) legt für ein zusammengesetztes Glied **keinen** Schlüssel `<gruppe>/g<i>` an. Es rekursiert direkt auf dessen Blätter. Aus `group(sig(a), chain(sig(b), sig(c)))` werden die Schritt-Schlüssel `/g0` und `/g1/0` — ein `/g1` entsteht nie.
2. Läuft das Glied fertig, erzeugt `_materialise` (`:642`) zwar eine Kennung für sein Ergebnis, schreibt sie aber **nie** nach `step_tasks`. Sie existiert nur als Rückgabewert auf dem Weg in die Fan-in-Zählung.
3. `_ordered_member_ids` (`:598`) sucht für jedes Glied `i` genau `<gruppe>/g<i>` und **überspringt den fehlenden Schlüssel wortlos**. Die Liste wird kürzer, ohne dass etwas geschieht.

Die Einreihung selbst ist korrekt: Die synthetische Kennung landet im Chord-Set, die Gruppe feuert also zur richtigen Zeit und bei der richtigen Zahl. Kaputt ist ausschließlich die Ergebnis-Zusammenstellung. Genau deshalb meldet `handle.children()` gleichzeitig, dass alle Schritte abgeschlossen sind — die Beobachtung aus dem Backlog-Eintrag.

Zwei Punkte, die der Backlog-Eintrag nicht nennt und die den Fehler schwerer machen als beschrieben:

- **Der Chord-Header ist genauso betroffen — und dort ist es schlimmer.** `_ordered_member_ids` baut auch die Liste, die als `inject` in den Chord-Body wandert (`:461`, `:475`). Ein `chord`, dessen Header eine `group` mit zusammengesetztem Glied ist, füttert den Body mit einer gekürzten Eingabe. Der Fehler steckt dann nicht im Rückgabewert, den man prüfen könnte, sondern in der nachgelagerten Rechnung selbst.
- **Es gibt für diese Topologie-Familie keinerlei Testabdeckung.** Weder `tests/unit/test_workflow_advancer.py` noch `tests/integration/test_workflow_orchestration.py`, `tests/e2e/test_workflows.py` oder `tests/property/test_workflow_advancer_properties.py` konstruieren eine Gruppe mit zusammengesetztem Glied. Deshalb ist der Defekt durch alle bisherigen Härtungsrunden gekommen, und deshalb ist die fehlende Abdeckung selbst Teil des Gegenstands.

Für die Lasttest-Suite heißt das: Die Konformitätsstufe führt den Befund als Ausnahme zu `workflow:chain`, `workflow:group` und `workflow:chord`; 19 von 46 konstruierbaren Mustern sind betroffen. Verschachtelte Gruppen sind bis zur Korrektur nicht benutzbar.

### Key Decisions

- **KD1 — Der Guard gehört dazu, nicht nur der Fix.** Der Backlog nennt das laute Scheitern als Ersatzwunsch („ersatzweise"). Hier ist es beides: die Korrektur **und** der Guard. Begründung: Die Ursache war eine wortlos übersprungene Nachschlage-Lücke. Ein Guard an derselben Stelle verwandelt jede verbleibende oder künftige Instanz dieser Klasse von einem stillen Rechenfehler in einen sofortigen, zuordenbaren Fehler. Die Korrektur allein schließt den heute bekannten Weg; der Guard schließt die Klasse.
- **KD2 — Der Chord-Header ist in Scope.** Er teilt sich Code und Ursache mit der Gruppe und ist der gefährlichere der beiden Fälle. Ihn draußen zu lassen wäre eine willkürliche Grenze.
- **KD3 — Rekursive Verschachtelung ist in Scope, nicht nur eine Ebene.** Der Klassenvertrag kennt keine Tiefenbegrenzung, und die Ursache ist tiefenunabhängig. Eine Korrektur, die nur eine Ebene trägt, wäre dieselbe stille Falle eine Ebene tiefer.

### Requirements

- **R1 —** Eine `group`, deren Glied eine `chain`, `group` oder ein `chord` ist, liefert eine Ergebnisliste mit genau `len(group.members)` Einträgen, in Definitionsreihenfolge.
- **R2 —** Der Eintrag eines zusammengesetzten Glieds ist das Ergebnis, das dieses Glied als Ganzes produziert: bei einer `chain` das Ergebnis ihres letzten Schritts, bei einer `group` die Liste ihrer Gliedergebnisse, bei einem `chord` das Ergebnis seines Bodys.
- **R3 —** Dieselbe Zusage gilt für einen Chord-Header: Der Chord-Body erhält die vollständige, geordnete Header-Liste als Eingabe.
- **R4 —** Die Zusage gilt rekursiv über beliebige Verschachtelungstiefe.
- **R5 —** Kann die Gliederliste dennoch nicht vollständig aufgebaut werden, wird das als Fehler gemeldet, nicht als gekürztes Ergebnis ausgeliefert — weder an den Aufrufer noch als Eingabe in einen Chord-Body.
- **R6 —** Die Zuordnung eines fertigen zusammengesetzten Glieds zu seiner Gruppe überlebt einen Prozesswechsel zwischen dem Abschluss dieses Glieds und dem Abschluss der Gruppe. Ein zweiter Worker, der den letzten Gliedabschluss verarbeitet, baut dieselbe vollständige Liste.
- **R7 —** Ein abgelaufenes Ergebnis eines zusammengesetzten Glieds verhält sich wie ein abgelaufenes Gliedergebnis heute: `WorkflowResultExpired`, kein stilles Loch und keine gekürzte Liste.
- **R8 —** Der dokumentierte Vertrag von `handle.children()` — „eine Abbildung jedes eingereihten Schritts auf Pfad, Status und Ergebnis" — bleibt gültig. Wenn die Umsetzung ihn erweitert, ist die Erweiterung dokumentiert und getestet, nicht beiläufig.
- **R9 —** Ein Workflow-Zustand, der vor dem Upgrade geschrieben wurde und das neue Feld nicht trägt, wird ohne Fehler gelesen. Ein zum Upgrade-Zeitpunkt laufender Workflow verhält sich höchstens wie bisher, nicht schlechter.
- **R10 —** Die Fan-in-Zählung bleibt unverändert: Die Zahl unterscheidbarer Glieder kann durch die Korrektur weder unter noch über `len(group.members)` geraten, und eine erneute Zustellung desselben Gliedabschlusses zählt weiterhin einmal.

### Acceptance Examples

- **AE1 —** `group(sig(a), chain(sig(b), sig(c)))` liefert eine zweielementige Liste `[erg(a), erg(c)]`. (Heute: einelementig.)
- **AE2 —** `group(sig(a), group(sig(b), sig(c)))` liefert `[erg(a), [erg(b), erg(c)]]`.
- **AE3 —** `group(chain(sig(a), sig(b)), chain(sig(c), sig(d)))` liefert `[erg(b), erg(d)]` — kein einziges einfaches Glied, die Liste ist trotzdem vollständig und in Definitionsreihenfolge.
- **AE4 —** `chord(group(sig(a), chain(sig(b), sig(c))), sig(body))` ruft `body` mit `[erg(a), erg(c)]` auf. (Heute: mit einer einelementigen Liste — der Fehler landet in der Rechnung des Bodys.)
- **AE5 —** `group(sig(a), chain(sig(b), group(sig(c), sig(d))))` liefert `[erg(a), [erg(c), erg(d)]]` — zwei Ebenen tief.
- **AE6 —** Das zusammengesetzte Glied wird auf Worker 1 fertig, das letzte einfache Glied auf Worker 2. Worker 2 baut dieselbe vollständige Liste; der Merker des ersten Glieds war beim Gruppenabschluss bereits im Zustand.
- **AE7 —** Der Gliedabschluss eines zusammengesetzten Glieds wird erneut zugestellt. Die Gruppe feuert weiterhin genau einmal und bei genau `len(group.members)` unterscheidbaren Gliedern.
- **AE8 —** Das Ergebnis eines zusammengesetzten Glieds ist vor dem Fan-in abgelaufen. Der Aufrufer sieht `WorkflowResultExpired` mit der bestehenden Meldung, keine kürzere Liste.
- **AE9 —** Wird die Gliederliste aus einem anderen Grund unvollständig, meldet der Guard einen Fehler, der Gruppenpfad, erwartete und tatsächliche Gliederzahl nennt.
- **AE10 —** Ein Zustandsdokument ohne das neue Feld — wie es `v0.3.0` schreibt — wird gelesen, ohne dass ein Schlüsselfehler entsteht.

### Scope Boundaries

**Nicht in Scope:**

- B9 (Workflow-Folgeschritte umgehen Middleware und `TASK_ENQUEUED`). Berührt denselben Advancer, ist aber ein eigener Gegenstand.
- Eine Zustandsformat-Migration für laufende Workflows. R9 verlangt verträgliches Lesen, keine Migration.
- Änderungen an der Fan-in-Zählung, am Chord-Set oder am Lua-Skript. R10 ist eine Erhaltungszusage, kein Arbeitsauftrag.
- Die Ausnahme-Einträge der Konformitätsstufe in der Lasttest-Suite. Deren Rücknahme ist Arbeit im anderen Repo, nachdem dieser Fix veröffentlicht ist.

### Dependencies / Assumptions

- Der Fehler ist an `v0.3.0` (aktueller `main` nach MR !39) unverändert vorhanden; er wurde dort nicht berührt.
- `WORKFLOW_STATE_VERSION` wird geschrieben, aber nirgends gelesen oder geprüft. Ein zusätzliches Zustandsfeld ist damit für sich genommen verträglich, solange jeder Leser einen Vorgabewert kennt.
- Der Integrations- und Ende-zu-Ende-Lauf braucht den isolierten Docker-Stack (RabbitMQ und Valkey auf abweichenden Ports), weil auf dem Rechner die Loadsuite-Umgebung läuft und die Standardports belegt sind.

### Success Criteria

- Die 19 an `v0.2.0`/`v0.3.0` betroffenen Muster der Konformitätsstufe liefern vollständige Ergebnislisten.
- Ein Test, der den Defekt vorher reproduziert, existiert und läuft nachher grün.
- Eine unvollständige Gliederliste ist nach der Umsetzung nicht mehr stillschweigend beobachtbar — an keiner der beiden Verwendungsstellen.

### Outstanding Questions

Keine offenen Produktfragen. Die einzige echte Wahl ist technisch und liegt in KTD1.

### How This Work Fits Together

Der Debug-Feature-Backlog der Lasttest-Suite hat zehn Einträge. Drei waren Fehler und sind mit `0.3.0` behoben (B8, B6 und die erste Hälfte von B3); der Abgleich steht in `docs/plans/2026-08-30-001-fix-inventar-debug-backlog.md`. B10 ist der vierte Fehler und der erste, der aus dem Workflow-Pfad kommt — er fiel auf, weil die Konformitätsstufe zum ersten Mal Workflows durch den Stack fährt. B9 stammt aus demselben Bau und demselben Modulbereich, ist aber ein anderer Gegenstand und bleibt hier draußen.

### Sources / Research

- `docs/debug-feature-backlog.md` (Repo `swarmq-loadsuite`), Eintrag B10, Commit `576279f` vom 2026-08-29.
- Verifikation am Code: `src/swarmq/workflow_orchestration.py` — `_frontier:282`, `_ordered_member_ids:598`, `_materialise:642`, `_on_group_member:443`, `_on_container_complete:484`, `WorkflowHandle.children:124`.
- Abwesenheitsbefund: keine Konstruktion einer Gruppe mit zusammengesetztem Glied in `tests/`.

---

## Planning Contract

### Key Technical Decisions

- **KTD1 — Die Gliedkennung lebt in einem eigenen Zustandsfeld, nicht in `step_tasks`.**
  Empfehlung: ein neues Feld (Arbeitsname `member_results`, Abbildung Topologie-Pfad → Kennung), das ausschließlich Aggregatkennungen zusammengesetzter Glieder trägt. `_ordered_member_ids` liest erst `step_tasks`, dann dieses Feld.
  Begründung: `step_tasks` ist die dokumentierte Quelle der Lese-Rekonstruktion, und `children()` zählt daraus **jeden eingereihten Schritt** auf. Ein Aggregat war nie ein Schritt; es dort einzutragen würde `children()` um Einträge erweitern, denen kein Task entspricht — eine beiläufige Vertragsänderung an einer öffentlichen Introspektions-Schnittstelle (R8).
  Verworfene Alternative A: Eintrag in `step_tasks`. Billiger, aber ändert `children()` still.
  Verworfene Alternative B: Kennung rein aus dem Pfad ableiten, ohne Zustand. Scheitert daran, dass `_materialise` für ein `chain`-Glied die **echte** Kennung des Abschlussschritts zurückgibt und gar keine synthetische schreibt. Ableitbar wäre sie nur, wenn `_materialise` für jedes zusammengesetzte Glied immer synthetisch schriebe — das dupliziert jedes Kettenergebnis ein zweites Mal in den Backend-Speicher.
  Verworfene Alternative C: Kennung aus der Topologie herleiten (für `chain(b,c)` an `/g1` den Pfad `/g1/1` nachschlagen). Trägt für Ketten, scheitert für ein `group`-Glied — dessen Ergebnis ist eine Liste, die unter keiner Schritt-Kennung liegt.

- **KTD2 — Der Merker wird geschrieben **und** persistiert, bevor die Fan-in-Meldung zurückkehrt.**
  Heute kehrt `_on_group_member` früh zurück, wenn die Gruppe noch nicht vollzählig ist — ohne Zustandsschreibung. Bliebe der Merker nur im Arbeitsspeicher, ginge er verloren, sobald der letzte Gliedabschluss auf einem anderen Worker verarbeitet wird (R6, AE6), und die Liste wäre wieder gekürzt. Preis: eine zusätzliche Zustandsschreibung je zusammengesetztem Glied. Das ist der richtige Preis — die Alternative ist ein Fehler, der nur unter Mehrprozess-Betrieb auftritt und damit genau im Lasttest.

- **KTD3 — Der Vollständigkeitsguard sitzt in `_ordered_member_ids` und wirft `WorkflowValidationError`.**
  Eine Stelle deckt beide Verwendungen ab (Gruppenergebnis und Chord-Body-Eingabe), und es ist dieselbe Stelle, an der der Defekt entstand. Fehlertyp: der vorhandene `WorkflowValidationError`, der dort schon für verletzte Topologie-Invarianten geworfen wird (`:468`). Ein neuer Fehlertyp wurde verworfen: Der Fehlerkatalog-Guard verlangt für jede `SwarmQError`-Unterklasse eine Einordnung, und der Erkenntnisgewinn wäre klein.

- **KTD4 — Die Definitionsreihenfolge bleibt über den Gliedindex `i` bestimmt, nicht über die Abschlussreihenfolge.** Unverändert gegenüber heute; die Korrektur ergänzt nur eine zweite Nachschlagequelle für denselben Index.

### High-Level Technical Design

Die Ursache ist eine Lücke zwischen zwei Kennungsräumen. Einfache Glieder tragen eine **Schritt**-Kennung unter `<gruppe>/g<i>`; zusammengesetzte Glieder tragen eine **Aggregat**-Kennung, die heute nirgends abgelegt wird. Die Zusammenstellung kennt nur den ersten Raum.

Die Korrektur macht den zweiten Raum persistent und lehrt die Zusammenstellung, beide zu lesen — in dieser Reihenfolge:

1. Beim Abschluss eines zusammengesetzten Glieds (`_on_container_complete`, Zweig `isinstance(parent, Group)`) wird die von `_materialise` gelieferte Kennung unter dem Gliedpfad im neuen Feld abgelegt und der Zustand geschrieben, **bevor** `_on_group_member` gerufen wird.
2. `_ordered_member_ids` schlägt je Index erst in `step_tasks`, dann im neuen Feld nach.
3. Findet es nach beiden Nachschlagen nicht `expected_n` Kennungen, wirft es — statt eine kurze Liste zu liefern.

Der Schreiber ist eine eigene, benannte Methode, nicht `mark_enqueued`. `mark_enqueued` verlangt bewusst den `PublishReceipt`, weil es die Reihenfolge Veröffentlichen-dann-Markieren strukturell erzwingt (AE5 des 0.3.0-Plans). Ein Gliedabschluss ist keine Einreihung; ihn durch dieselbe Tür zu schicken würde diese Zusage aufweichen.

Die Rekursion (R4) fällt heraus, ohne eigenes Zutun: Ein zusammengesetztes Glied, das selbst zusammengesetzte Glieder enthält, durchläuft denselben Pfad je Ebene — die innere Gruppe legt ihre Merker ab und meldet ihr Aggregat nach oben, wo es erneut als Glied abgelegt wird.

### Assumptions

- `_materialise` liefert für denselben Gliedpfad bei erneuter Zustellung dieselbe Kennung: entweder die deterministische Schritt-Kennung des Abschlussschritts oder das deterministische `{wf_id}:agg:{node_path}`. Ein doppelt verarbeiteter Gliedabschluss schreibt damit denselben Merker (R10, AE7).
- Der Backend-Zugriff auf das Aggregat-Ergebnis unterliegt derselben TTL-Behandlung wie jedes andere Gliedergebnis; R7 verlangt keine neue Mechanik, nur dass der bestehende Pfad erreicht wird.

### Sequencing

U1 zuerst — der reproduzierende Test steht vor der Korrektur. U2 und U3 tragen die Korrektur, U4 den Guard, U5 die verbleibende Abdeckung, U6 die Dokumentation. U4 kann erst nach U2/U3 grün laufen, weil er sonst genau den Defekt meldet, den U2/U3 beheben.

---

## Implementation Units

### U1. Den Defekt als Fehlschlag festhalten

- **Goal:** Ein Test reproduziert an unverändertem Stand, dass eine Gruppe mit zusammengesetztem Glied eine gekürzte Liste liefert — und ein zweiter, dass ein Chord-Body eine gekürzte Eingabe erhält.
- **Requirements:** R1, R3. Belegt AE1 und AE4 zunächst als Fehlschlag.
- **Dependencies:** keine.
- **Files:** `tests/unit/test_workflow_advancer.py`
- **Execution note:** Vom Fehlschlag ausgehen. Diese Tests müssen vor der Korrektur rot sein und den Defekt zeigen, nicht einen Konstruktionsfehler des Tests.
- **Approach:**
  1. Den vorhandenen Advancer-Aufbau des Moduls übernehmen — nicht neu erfinden; die bestehenden Gruppen- und Chord-Tests zeigen, wie Zustand, Chord-Speicher und Backend dort gestellt werden.
  2. Einen Fall `group(sig, chain(sig, sig))` konstruieren, alle Schritte der Reihe nach abschließen lassen und die Länge der Endliste prüfen.
  3. Einen Fall `chord(group(sig, chain(sig, sig)), sig)` konstruieren und die Eingabe prüfen, mit der der Body eingereiht wird.
  4. Beide einmal gegen den ungefixten Stand laufen lassen und den beobachteten Ist-Zustand im Testnamen oder einem knappen Kommentar festhalten, damit später erkennbar ist, wogegen sie schützen.
- **Patterns to follow:** Die bestehenden Gruppen-Fan-in-Tests in `tests/unit/test_workflow_advancer.py`.
- **Test scenarios:**
  - `group(a, chain(b, c))`: Endliste hat zwei Einträge, der zweite ist das Ergebnis von `c`.
  - `chord(group(a, chain(b, c)), body)`: `body` wird mit einer zweielementigen Liste eingereiht.
- **Verification:** Beide Tests scheitern am unveränderten Stand, und zwar mit einer zu kurzen Liste — nicht mit einem Fehler aus dem Testaufbau.

### U2. Aggregatkennungen zusammengesetzter Glieder im Zustand halten

- **Goal:** Der Abschluss eines zusammengesetzten Gruppenglieds hinterlässt eine dauerhafte, dem Gliedpfad zugeordnete Kennung seines Ergebnisses.
- **Requirements:** R2, R6, R9, R10. Umgesetzt nach KTD1 und KTD2.
- **Dependencies:** U1.
- **Files:** `src/swarmq/workflow_orchestration.py`
- **Approach:**
  1. Das neue Feld in `build_initial_workflow_state` ergänzen, mit einem Kommentar, der es gegen `step_tasks` abgrenzt: Schritte dort, Aggregate hier.
  2. Jeden Leser über einen Vorgabewert gehen lassen, damit ein vor dem Upgrade geschriebener Zustand ohne das Feld nicht scheitert (R9).
  3. Eine eigene, benannte Schreibmethode ergänzen — bewusst nicht `mark_enqueued`, dessen Beleg-Zwang die Veröffentlichungs-Reihenfolge sichert und hier nichts zu sichern hätte. Im Docstring festhalten, warum es eine zweite Tür gibt.
  4. Im `Group`-Zweig von `_on_container_complete` die von `_materialise` gelieferte Kennung unter dem Gliedpfad ablegen und den Zustand schreiben, **bevor** `_on_group_member` gerufen wird.
  5. Prüfen, dass die Zustandsschreibung auch auf dem Pfad stattfindet, auf dem die Gruppe danach *nicht* feuert — genau dort geht der Merker sonst verloren.
- **Patterns to follow:** Die Feld-Beschreibung im Docstring von `build_initial_workflow_state`; die bestehende `_persist`-Aufrufform in den Nachbarzweigen.
- **Test scenarios:**
  - Nach dem Abschluss eines zusammengesetzten Glieds trägt der persistierte Zustand dessen Kennung unter dem Gliedpfad.
  - Der Merker steht auch dann im persistierten Zustand, wenn die Gruppe danach noch nicht vollzählig ist.
  - Ein erneut zugestellter Gliedabschluss schreibt denselben Wert, nicht einen zweiten.
  - Ein Zustandsdokument ohne das neue Feld wird ohne Schlüsselfehler gelesen (AE10).
- **Verification:** Der persistierte Zustand trägt den Merker nachweislich vor dem Gruppenabschluss, und ein aus dem Backend neu geladener Zustand genügt, um die Liste vollständig zu bauen.

### U3. Zusammenstellung beide Kennungsräume lesen lassen

- **Goal:** `_ordered_member_ids` findet für jeden Gliedindex eine Kennung, gleich ob das Glied einfach oder zusammengesetzt ist — und damit auch der Chord-Body seine vollständige Eingabe.
- **Requirements:** R1, R2, R3, R4, R7. Belegt AE1–AE5 und AE8.
- **Dependencies:** U2.
- **Files:** `src/swarmq/workflow_orchestration.py`
- **Approach:**
  1. Je Index erst `step_tasks`, dann das neue Feld nachschlagen; die Definitionsreihenfolge bleibt über den Index bestimmt (KTD4).
  2. Nichts an `_gather_results` ändern — der bestehende TTL-Pfad trägt Aggregatkennungen genauso wie Schrittkennungen und muss R7 damit ohne Zutun erfüllen. Wenn sich beim Testen das Gegenteil zeigt, ist das ein Befund, kein stiller Umbau.
  3. Prüfen, dass die Chord-Header-Verwendung (`gp_last == "h"`) automatisch mitgeht — sie ruft dieselbe Methode.
- **Patterns to follow:** Die bestehende Schleifenform in `_ordered_member_ids`.
- **Test scenarios:**
  - `group(a, chain(b, c))` liefert `[erg(a), erg(c)]` (AE1).
  - `group(a, group(b, c))` liefert `[erg(a), [erg(b), erg(c)]]` (AE2).
  - `group(chain(a, b), chain(c, d))` liefert `[erg(b), erg(d)]` — kein einfaches Glied dabei (AE3).
  - `chord(group(a, chain(b, c)), body)` reiht `body` mit `[erg(a), erg(c)]` ein (AE4).
  - `group(a, chain(b, group(c, d)))` liefert `[erg(a), [erg(c), erg(d)]]` — zwei Ebenen (AE5).
  - Ein abgelaufenes Aggregat-Ergebnis führt zu `WorkflowResultExpired` mit der bestehenden Meldung (AE8).
  - Die Tests aus U1 laufen grün.
- **Verification:** Die U1-Tests kippen von rot auf grün, und die verschachtelten Fälle liefern die vollständige Liste in Definitionsreihenfolge.

### U4. Eine unvollständige Gliederliste laut machen

- **Goal:** Bleibt die Gliederliste kürzer als die Gliederzahl, wird sie als Fehler gemeldet statt ausgeliefert — an beiden Verwendungsstellen.
- **Requirements:** R5. Belegt AE9. Setzt KD1 und KTD3 um.
- **Dependencies:** U3.
- **Files:** `src/swarmq/workflow_orchestration.py`
- **Approach:**
  1. Am Ende von `_ordered_member_ids` gegen `expected_n` prüfen und bei Abweichung `WorkflowValidationError` werfen.
  2. Die Meldung trägt den Gruppenpfad, die erwartete und die tatsächliche Zahl — sie ist der einzige Hinweis, den ein Betreiber im Fehlerfall bekommt.
  3. Im Docstring festhalten, dass das wortlose Überspringen der Ursprung von B10 war; ohne diesen Satz sieht der Guard nach überflüssiger Vorsicht aus und wird beim nächsten Umbau wegvereinfacht.
- **Patterns to follow:** Die bestehende `WorkflowValidationError`-Meldungsform im selben Modul (`:468`).
- **Test scenarios:**
  - Ein Zustand, dem ein Gliedmerker fehlt, erzeugt einen Fehler mit Gruppenpfad und beiden Zahlen — kein gekürztes Ergebnis.
  - Derselbe Zustand als Chord-Header erzeugt denselben Fehler, bevor der Body eingereiht wird.
  - Der vollständige Fall wirft nicht.
- **Verification:** Kein Pfad im Modul liefert nach der Umsetzung noch eine Liste, die kürzer ist als die Gliederzahl.

### U5. Die fehlende Abdeckung schließen

- **Goal:** Die Topologie-Familie „Gruppe mit zusammengesetztem Glied" ist über die Teststufen hinweg abgedeckt, nicht nur an der Nahtstelle.
- **Requirements:** R1–R7. Belegt AE6 und AE7.
- **Dependencies:** U3, U4.
- **Files:** `tests/integration/test_workflow_orchestration.py`, `tests/e2e/test_workflows.py`, `tests/property/test_workflow_advancer_properties.py`
- **Approach:**
  1. Integration: einen verschachtelten Fall gegen das echte Backend fahren, damit der Merker nachweislich den Weg durch Serialisierung und Speicher nimmt.
  2. Ende-zu-Ende: einen verschachtelten Fall über den echten Broker, mit dem Ergebnis über `handle.get()` — die Sicht, die ein Nutzer hat.
  3. Prozesswechsel (AE6): das zusammengesetzte Glied auf einer Advancer-Instanz abschließen, den Zustand neu laden, den letzten Gliedabschluss auf einer zweiten verarbeiten. Das ist der Fall, den KTD2 trägt, und er scheitert ohne die Persistenz.
  4. Erneute Zustellung (AE7): den Gliedabschluss eines zusammengesetzten Glieds zweimal verarbeiten und prüfen, dass die Gruppe einmal feuert.
  5. Property-Stufe: prüfen, ob die vorhandene Generierung um zusammengesetzte Glieder erweiterbar ist. Wenn nicht ohne Umbau, das begründet festhalten statt erzwingen — der Umbau der Generierung wäre ein eigener Gegenstand.
  6. `handle.children()` in einem der Fälle mitprüfen: Der Vertrag aus R8 muss nachweislich unverändert gelten.
- **Patterns to follow:** Die bestehenden Workflow-Fälle der jeweiligen Stufe; die isolierte Docker-Umgebung mit abweichenden Ports aus dem 0.3.0-Lauf.
- **Test scenarios:**
  - Integration: `group(a, chain(b, c))` über das echte Backend liefert beide Ergebnisse.
  - Ende-zu-Ende: ein verschachtelter Workflow liefert über `handle.get()` die vollständige Liste.
  - Prozesswechsel: zwei Advancer-Instanzen, vollständige Liste (AE6).
  - Erneute Zustellung: die Gruppe feuert genau einmal (AE7).
  - `children()` enthält die eingereihten Schritte und keine Aggregat-Einträge (R8).
- **Verification:** Alle drei Stufen laufen grün, und der Prozesswechsel-Test scheitert nachweislich, wenn man die Persistenz aus U2 wieder herausnimmt.

### U6. Vertrag und Abgleich nachziehen

- **Goal:** Der Klassenvertrag stimmt wieder mit dem Verhalten überein, und der Backlog-Abgleich weist B10 als behoben aus.
- **Requirements:** R8.
- **Dependencies:** U5.
- **Files:** `website/reference/changelog.md`, `docs/plans/2026-08-30-001-fix-inventar-debug-backlog.md`, ggf. `src/swarmq/workflow.py`
- **Approach:**
  1. Changelog-Eintrag: Was war kaputt, welche Topologien betraf es, ab wann. Ein Nutzer, der verschachtelte Gruppen einsetzt, muss daraus erkennen, dass frühere Ergebnisse falsch gewesen sein können — das ist die eigentliche Nachricht.
  2. Prüfen, ob der `Group`-Docstring die Zusage präzise genug formuliert; falls er die Verschachtelung offenlässt, sie benennen.
  3. Im Fix-Inventar B10 aus dem Abschnitt „Neu gemeldet" nach „Behoben" verschieben, mit War/Jetzt in der dort etablierten Form.
- **Patterns to follow:** Die Changelog-Einträge der 0.3.0-Fixes; die War/Jetzt-Form im Fix-Inventar.
- **Test scenarios:** Keine — Dokumentationsarbeit.
- **Verification:** Changelog und Fix-Inventar nennen den Defekt, seinen Umfang und seine Behebung.

---

## Verification Contract

| Gate | Befehl | Gilt für |
|---|---|---|
| Lint | `uv run --extra test --extra reload ruff check src/ tests/` | alle Units |
| Typen | `uv run --extra test --extra reload mypy src/swarmq tests/support tests/contracts` | alle Units |
| Schneller Lauf | `uv run --extra test --extra reload pytest tests/unit -q --reruns 1 --reruns-delay 1` | U1, U2, U3, U4 |
| Property | `uv run --extra test --extra reload pytest tests/property -q` | U5 |
| Integration | `uv run --extra test --extra reload pytest tests/integration -q` | U2, U5 |
| Ende-zu-Ende | `uv run --extra test --extra reload pytest tests/e2e -q` | U5 |

Integration und Ende-zu-Ende brauchen RabbitMQ und Valkey. Auf Georgs Rechner belegt die Loadsuite-Umgebung die Standardports — die Läufe gehen über eigene Container auf abweichenden Ports (`SWARMQ_TEST_RABBITMQ_PORT`, `SWARMQ_TEST_VALKEY_PORT`), wie beim 0.3.0-Lauf. Alle Gates sind blockierend.

Der Chaos-Lauf ist hier nicht nötig: Der Gegenstand ist Ergebnis-Zusammenstellung, keine Störungsresistenz. Das CI-Gate wird ihn trotzdem verlangen, weil der Änderungspfad `src/swarmq/` berührt — mit `workflow_orchestration.py` allerdings außerhalb der vier gelisteten Pfade, sodass der Job als überspringbarer manueller Schritt läuft.

---

## Definition of Done

**Global**

- Alle zehn Anforderungen sind durch mindestens ein Arbeitspaket abgedeckt und durch mindestens ein Testszenario belegt.
- Die blockierenden Gates des Verification Contract laufen grün.
- Die U1-Tests sind nachweislich einmal gegen den ungefixten und einmal gegen den gefixten Stand gelaufen und zeigen den Unterschied.
- Kein Pfad im Modul liefert noch eine Gliederliste, die kürzer ist als die Gliederzahl.
- Verworfene Ansätze sind aus dem Stand entfernt: keine auskommentierten Zwischenstände, keine ungenutzten Hilfsfunktionen.

**Je Arbeitspaket**

- U1: Zwei Tests reproduzieren den Defekt am ungefixten Stand.
- U2: Der Merker eines zusammengesetzten Glieds steht im persistierten Zustand, bevor die Gruppe vollzählig ist.
- U3: Verschachtelte Gruppen und Chord-Header liefern die vollständige Liste in Definitionsreihenfolge, über beliebige Tiefe.
- U4: Eine unvollständige Liste erzeugt einen Fehler mit Gruppenpfad und beiden Zahlen.
- U5: Integration, Ende-zu-Ende und der Prozesswechsel-Fall sind abgedeckt; `children()` ist unverändert.
- U6: Changelog und Fix-Inventar weisen den Defekt und seine Behebung aus.

---

## Risks & Dependencies

- **Bereits abgeschlossene Workflows bleiben falsch.** Der Fix wirkt auf Workflows, die nach dem Upgrade laufen. Ergebnisse, die eine verschachtelte Gruppe unter `v0.2.0` oder `v0.3.0` geliefert hat, waren gekürzt und bleiben es — sie liegen als gespeicherte Werte im Backend und in allem, was daraus gerechnet wurde. Wer verschachtelte Gruppen im Einsatz hat, muss die betroffenen Läufe selbst prüfen; der Changelog-Eintrag aus U6 ist die einzige Warnung, die swarmq dafür geben kann.
- **Ein zum Upgrade-Zeitpunkt laufender Workflow bleibt gekürzt.** Sein Zustand trägt das neue Feld nicht und kann es rückwirkend nicht bekommen — die Gliedabschlüsse sind bereits verarbeitet. R9 sichert zu, dass er nicht *schlechter* wird, nicht dass er richtig wird. Der Vollständigkeitsguard aus U4 fängt ihn dann allerdings ab: Er scheitert laut, statt still zu kürzen. Das ist die richtige Richtung, aber es heißt, dass ein Upgrade mitten in einem verschachtelten Workflow diesen Workflow zum Fehler führt statt zu einem falschen Ergebnis. Wer das nicht will, fährt die Workflows vor dem Upgrade leer.
- **Die zusätzliche Zustandsschreibung je zusammengesetztem Glied kostet.** Bei einer Gruppe mit vielen zusammengesetzten Gliedern ist das eine Schreibung mehr pro Glied. Gemessen an einem Workflow-Schritt, der ohnehin Broker und Backend berührt, ist das klein — es ist trotzdem der Preis von KTD2 und sollte im Lasttest sichtbar bleiben.
- **Die Property-Stufe deckt die Familie möglicherweise weiterhin nicht ab.** Wenn die vorhandene Topologie-Generierung keine zusammengesetzten Glieder erzeugt, bleibt die Lücke, die den Defekt zwei Releases lang getragen hat, teilweise offen. U5 verlangt in dem Fall eine Begründung statt eines erzwungenen Umbaus — der wäre ein eigener Gegenstand, aber er sollte dann auch als solcher entstehen und nicht vergessen werden.
