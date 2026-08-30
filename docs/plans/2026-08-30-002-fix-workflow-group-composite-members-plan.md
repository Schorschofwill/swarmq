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
- **Der Guard wirkt breiter als der Fehler.** Er sitzt an der gemeinsamen Stelle **jedes** Gruppen-Fan-ins und **jedes** Chord-Headers, auch flacher. Das ist gewollt (KD1), verändert aber das Fehlerprofil auch für Nutzer, die nie verschachteln — und gehört deshalb in die Nutzerkommunikation, nicht nur in den Code.
- **Kein Eingriff ins Zustandsformat.** Die gewählte Lösung leitet die Gliedkennung aus der ohnehin gespeicherten Topologie ab, statt einen neuen Zustandseintrag zu führen. Das ist keine Sparsamkeit, sondern die Bedingung dafür, dass die Korrektur unter Nebenläufigkeit trägt — siehe KTD1.
- **Nicht aktiver Scope:** Die übrigen offenen Backlog-Einträge (B1, B2, B3-zweite-Hälfte, B4, B5, B7, B9) sind Kontext, nicht Anforderung. B9 berührt denselben Advancer, ist aber ein eigener Gegenstand (Middleware und Signal statt Ergebnis-Zusammenstellung).
- **Execution profile:** Python ≥ 3.14, uv-Toolchain. Eine Quelldatei trägt die Ursache (`src/swarmq/workflow_orchestration.py`); dazu Testabdeckung, die es für diesen Fall bisher überhaupt nicht gibt, und die Veröffentlichungsarbeit aus U5 (Changelog in beiden Sprachfassungen, Fix-Inventar, ggf. `Group`-Docstring). Keine neuen Abhängigkeiten.
- **Execution note:** Aus dem Fehlschlag heraus arbeiten. Für zusammengesetzte Gruppenglieder existiert im gesamten Repo kein einziger Test — der erste Schritt ist ein Test, der den Defekt am unveränderten Stand reproduziert, nicht die Korrektur.
- **Stop conditions:** Stoppen und rückfragen, wenn (a) der Vollständigkeitsguard einen bestehenden Test bricht, der eine heute gültige Topologie absichert, (b) sich zeigt, dass die Ableitung für eine Knotenart nicht deterministisch ist, (c) die Nachbesserung am verlorenen Zustands-Update (KTD5) nötig wird, um U3 überhaupt grün zu bekommen — das wäre ein eigener Gegenstand.
- **Tail ownership:** Der Release-Schnitt gehört Georg, ist aber **benannte Nachfolgearbeit**, keine Nichtzuständigkeit: Vor der Veröffentlichung ist für keinen PyPI-Nutzer etwas behoben, und die Konformitätsstufe der Lasttest-Suite behält ihre Ausnahmen. Zur Versionsklasse siehe KD5.
- **Open blockers:** Keine. KTD1 ist entschieden; die zwei verworfenen Alternativen sind dort mit dem tragenden Grund benannt.

---

## Product Contract

### Summary

Die Kennung des Ergebnisses eines zusammengesetzten Gruppenglieds wird beim Fan-in aus der Topologie abgeleitet, statt gesucht zu werden, wo sie nie stand. Dazu ein Guard, der eine unvollständige Gliederliste als Fehler meldet statt als Ergebnis, die Testabdeckung, die für diese Topologie-Familie bisher fehlt, und eine Offenlegung, aus der ein Betreiber bestimmen kann, ob er betroffen war.

### Problem Frame

`Group` sagt zu, ihr Ergebnis sei „die geordnete Liste der Gliedergebnisse in Definitionsreihenfolge". Für ein Glied, das selbst zusammengesetzt ist, hält swarmq diese Zusage seit `v0.2.0` nicht ein.

Der Weg dorthin, am Code bestätigt:

1. `_frontier` (`src/swarmq/workflow_orchestration.py:282`, Group-Zweig ab `:287`) legt für ein zusammengesetztes Glied **keinen** Schlüssel `<gruppe>/g<i>` an. Es rekursiert direkt auf dessen Blätter. Aus `group(sig(a), chain(sig(b), sig(c)))` werden die Schritt-Schlüssel `/g0` und `/g1/0` — ein `/g1` entsteht nie.
2. Läuft das Glied fertig, erzeugt `_materialise` (`:642`) zwar eine Kennung für sein Ergebnis, schreibt sie aber **nie** nach `step_tasks`. Sie existiert nur als Rückgabewert auf dem Weg in die Fan-in-Zählung.
3. `_ordered_member_ids` (`:598`) sucht für jedes Glied `i` genau `<gruppe>/g<i>` und **überspringt den fehlenden Schlüssel wortlos**. Die Liste wird kürzer, ohne dass etwas geschieht.

Die Einreihung selbst ist korrekt: Die Kennung landet im Chord-Set, die Gruppe feuert also zur richtigen Zeit und bei der richtigen Zahl. Kaputt ist ausschließlich die Ergebnis-Zusammenstellung. Genau deshalb meldet `handle.children()` gleichzeitig, dass alle Schritte abgeschlossen sind — die Beobachtung aus dem Backlog-Eintrag.

Zwei Punkte, die der Backlog-Eintrag nicht nennt und die den Fehler schwerer machen als beschrieben:

- **Der Chord-Header ist genauso betroffen — und dort ist es schlimmer.** `_ordered_member_ids` baut auch die Liste, die als `inject` in den Chord-Body wandert (`:461`, `:475`). Ein `chord`, dessen Header eine `group` mit zusammengesetztem Glied ist, füttert den Body mit einer gekürzten Eingabe. Der Fehler steckt dann nicht im Rückgabewert, den man prüfen könnte, sondern in der nachgelagerten Rechnung selbst.
- **Es gibt für diese Topologie-Familie keinerlei Testabdeckung.** Weder `tests/unit/test_workflow_advancer.py` noch `tests/integration/test_workflow_orchestration.py`, `tests/e2e/test_workflows.py` oder `tests/property/test_workflow_advancer_properties.py` konstruieren eine Gruppe mit zusammengesetztem Glied; die Property-Generierung erzeugt ausschließlich flache Topologien. Deshalb ist der Defekt durch alle bisherigen Härtungsrunden gekommen, und deshalb ist die fehlende Abdeckung selbst Teil des Gegenstands.

Für die Lasttest-Suite heißt das: Die Konformitätsstufe führt den Befund als Ausnahme zu `workflow:chain`, `workflow:group` und `workflow:chord`; 19 von 46 konstruierbaren Mustern sind betroffen. Verschachtelte Gruppen sind bis zur Korrektur nicht benutzbar.

### Key Decisions

- **KD1 — Der Guard gehört dazu, nicht nur der Fix.** Der Backlog nennt das laute Scheitern als Ersatzwunsch („ersatzweise"). Hier ist es beides: die Korrektur **und** der Guard. Begründung: Die Ursache war eine wortlos übersprungene Nachschlage-Lücke. Ein Guard an derselben Stelle verwandelt jede verbleibende oder künftige Instanz dieser Klasse von einem stillen Rechenfehler in einen sofortigen, zuordenbaren Fehler. Die Korrektur allein schließt den heute bekannten Weg; der Guard schließt die Klasse. **Der Preis ist ausdrücklich mitentschieden:** Er wirkt für jede Gruppe und jeden Chord-Header, auch flache, und macht damit auch bisher unbekannte Ursachen einer kurzen Liste laut — etwa einen im publish→mark-Fenster verlorenen Schritt-Marker.
- **KD2 — Der Chord-Header ist in Scope.** Er teilt sich Code und Ursache mit der Gruppe und ist der gefährlichere der beiden Fälle. Ihn draußen zu lassen wäre eine willkürliche Grenze.
- **KD3 — Rekursive Verschachtelung ist in Scope, nicht nur eine Ebene.** Die Ursache ist tiefenunabhängig, also muss es die Korrektur auch sein. Die Zusage endet an der bestehenden Obergrenze `DEFAULT_MAX_WORKFLOW_DEPTH` (heute 10, `src/swarmq/workflow.py:24`), die `_validate` schon bei der Konstruktion durchsetzt — „beliebig tief" gibt es in swarmq nicht und soll es nicht geben.
- **KD4 — Für einen zum Upgrade laufenden verschachtelten Workflow ist lautes Scheitern der gewollte Ausgang.** Sein Ergebnis wäre ohnehin falsch; ein sichtbarer Fehler ist billiger als eine falsche Zahl, die weiterwandert. Die Entscheidung erzeugt eine Betriebsanweisung (verschachtelte Workflows vor dem Upgrade leerfahren), die in die Veröffentlichung gehört, nicht nur in diesen Plan.
- **KD5 — Der Verhaltenswechsel ist ein Bruch und wird als solcher ausgewiesen.** Das Projekt folgt erklärtem SemVer, und der vergleichbare Fall aus 0.3.0 (die neue Startverweigerung des Workers) stand unter „Changed (breaking)". Ein laufender Workflow, der nach dem Upgrade scheitert statt ein Ergebnis zu liefern, gehört in dieselbe Rubrik — nicht unter „Fixed". Ob daraus 0.3.1 oder 0.4.0 wird, entscheidet Georg beim Release-Schnitt; die Rubrik entscheidet dieser Plan.

### Requirements

- **R1 —** Eine `group`, deren Glied eine `chain`, `group` oder ein `chord` ist, liefert eine Ergebnisliste mit genau `len(group.members)` Einträgen, in Definitionsreihenfolge.
- **R2 —** Der Eintrag eines zusammengesetzten Glieds ist das Ergebnis, das dieses Glied als Ganzes produziert: bei einer `chain` das Ergebnis ihres letzten Schritts, bei einer `group` die Liste ihrer Gliedergebnisse, bei einem `chord` das Ergebnis seines Bodys.
- **R3 —** Dieselbe Zusage gilt für einen Chord-Header: Der Chord-Body erhält die vollständige, geordnete Header-Liste als Eingabe.
- **R4 —** Die Zusage gilt rekursiv bis zur geltenden `DEFAULT_MAX_WORKFLOW_DEPTH` (heute 10). Tiefer konstruierbare Topologien lehnt `_validate` schon heute ab; daran ändert dieser Plan nichts.
- **R5 —** Kann die Gliederliste nicht vollständig aufgebaut werden, wird das als Fehler gemeldet, nicht als gekürztes Ergebnis ausgeliefert — weder an den Aufrufer noch als Eingabe in einen Chord-Body. Das gilt für **jede** Gruppe und jeden Chord-Header, nicht nur für verschachtelte.
- **R6 —** Die Zuordnung eines fertigen zusammengesetzten Glieds zu seiner Gruppe hängt nicht davon ab, welcher Prozess welchen Gliedabschluss verarbeitet und in welcher Reihenfolge. Insbesondere überlebt sie **gleichzeitige** Abschlüsse mehrerer Glieder, die von demselben Zustands-Schnappschuss ausgehen.
- **R7 —** Ein abgelaufenes Ergebnis eines zusammengesetzten Glieds verhält sich wie ein abgelaufenes Gliedergebnis heute: `WorkflowResultExpired`, kein stilles Loch und keine gekürzte Liste.
- **R8 —** Der dokumentierte Vertrag von `handle.children()` — „eine Abbildung jedes eingereihten Schritts auf Pfad, Status und Ergebnis" — bleibt unverändert gültig.
- **R9 —** Das persistierte Zustandsformat bleibt unverändert: kein neues Feld, keine Migration, keine neu zu erzeugende Golden-Fixture. Ein zum Upgrade-Zeitpunkt laufender verschachtelter Workflow liefert kein gekürztes Ergebnis mehr, sondern scheitert am Guard aus R5 laut und zuordenbar (KD4).
- **R10 —** Die Fan-in-Zählung bleibt unverändert: Die an `record_member` übergebene Kennung ändert sich nicht, die Zahl unterscheidbarer Glieder kann weder unter noch über `len(group.members)` geraten, und eine erneute Zustellung desselben Gliedabschlusses zählt weiterhin einmal.
- **R11 —** Die Veröffentlichung weist den Defekt als Korrektheitsfehler aus und nennt: die betroffenen Versionen (0.2.0, 0.3.0), die betroffenen Topologien, das **Prüfkriterium**, mit dem ein Betreiber betroffene Läufe findet, die geänderte Fehlerklasse für flache Gruppen (KD1) und die Betriebsanweisung aus KD4.

### Acceptance Examples

- **AE1 —** `group(sig(a), chain(sig(b), sig(c)))` liefert eine zweielementige Liste `[erg(a), erg(c)]`. (Heute: einelementig.)
- **AE2 —** `group(sig(a), group(sig(b), sig(c)))` liefert `[erg(a), [erg(b), erg(c)]]`.
- **AE3 —** `group(chain(sig(a), sig(b)), chain(sig(c), sig(d)))` liefert `[erg(b), erg(d)]` — kein einziges einfaches Glied, die Liste ist trotzdem vollständig und in Definitionsreihenfolge.
- **AE4 —** `chord(group(sig(a), chain(sig(b), sig(c))), sig(body))` ruft `body` mit `[erg(a), erg(c)]` auf. (Heute: mit einer einelementigen Liste — der Fehler landet in der Rechnung des Bodys.)
- **AE5 —** `group(sig(a), chain(sig(b), group(sig(c), sig(d))))` liefert `[erg(a), [erg(c), erg(d)]]` — zwei Ebenen tief.
- **AE6 —** Zwei zusammengesetzte Glieder derselben Gruppe schließen **gleichzeitig** ab: Beide Advancer starten von demselben Zustands-Schnappschuss, beide schreiben zurück. Die Gruppe liefert trotzdem die vollständige Liste. (Das ist der Fall, an dem ein zustandsführender Entwurf zerbricht — siehe KTD1.)
- **AE7 —** Das zusammengesetzte Glied wird auf Worker 1 fertig, das letzte einfache Glied auf Worker 2, mit einem Neuladen des Zustands dazwischen. Worker 2 baut dieselbe vollständige Liste.
- **AE8 —** Der Gliedabschluss eines zusammengesetzten Glieds wird erneut zugestellt. Die Gruppe feuert weiterhin genau einmal und bei genau `len(group.members)` unterscheidbaren Gliedern.
- **AE9 —** Das Ergebnis eines zusammengesetzten Glieds ist vor dem Fan-in abgelaufen. Der Aufrufer sieht `WorkflowResultExpired` mit der bestehenden Meldung, keine kürzere Liste.
- **AE10 —** Fehlt einem Glied die Kennung, meldet der Guard einen Fehler, der Gruppenpfad, erwartete und tatsächliche Gliederzahl nennt — auch bei einer flachen Gruppe.
- **AE11 —** Die Golden-Fixture `tests/integration/backend/golden/values/workflow_state.json` bleibt unverändert grün; das Zustandsformat hat sich nicht bewegt.
- **AE12 —** Ein Betreiber kann allein aus den veröffentlichten Artefakten bestimmen, ob seine Workflows betroffen waren, und was er vor dem Upgrade tun muss.

### Scope Boundaries

**Nicht in Scope:**

- B9 (Workflow-Folgeschritte umgehen Middleware und `TASK_ENQUEUED`). Berührt denselben Advancer, ist aber ein eigener Gegenstand.
- **Das verlorene Zustands-Update** (`store_workflow_state` überschreibt blind, `advance()` liest einmal). Ein vorbestehender Nebenläufigkeitsmangel, den dieser Plan bewusst nicht behebt — siehe KTD5 und Risks. Die gewählte Lösung verkleinert die Angriffsfläche, statt sie zu vergrößern; sie schließt sie nicht.
- Jede Änderung am Zustandsformat, an `WORKFLOW_STATE_VERSION` oder an den Golden-Fixtures. R9 macht daraus eine Erhaltungszusage.
- Änderungen an der Fan-in-Zählung, am Chord-Set oder an `chord_complete.lua`. R10 ist eine Erhaltungszusage, kein Arbeitsauftrag.
- Der Release-Schnitt selbst und die Rücknahme der Ausnahme-Einträge in der Lasttest-Suite (Arbeit im anderen Repo, nach der Veröffentlichung).

### Dependencies / Assumptions

- Der Fehler ist an `v0.3.0` (aktueller `main`, `7786d67`) unverändert vorhanden; er wurde dort nicht berührt.
- **Das Zustandsdokument ist ein eingefrorenes Wire-Format.** `tests/integration/backend/golden/values/workflow_state.json` wird gegen `build_initial_workflow_state` verglichen; die Fixture-Dokumentation nennt eine rote Zeile dort ausdrücklich einen benannten Bruch, der einen Migrationshinweis braucht. Dass `WORKFLOW_STATE_VERSION` selbst nirgends gelesen wird, ändert daran nichts. Die gewählte Lösung kommt deshalb ohne Formatänderung aus.
- **Rollierendes Upgrade:** R5, R6 und R9 gelten erst, wenn der gesamte Worker-Fleet auf der neuen Version läuft. Im gemischten Betrieb kürzt ein alter Worker weiterhin still.
- `_materialise` ist in beiden Zweigen deterministisch: Schritt-Kennung über `workflow_step_task_id`, sonst `{wf_id}:agg:{node_path}`. Das trägt R10 und AE8.
- Der Integrations- und Ende-zu-Ende-Lauf braucht RabbitMQ und Valkey. Auf Georgs Rechner belegt die Loadsuite-Umgebung die Standardports; die Läufe gehen über eigene Container auf abweichenden Ports (`SWARMQ_TEST_RABBITMQ_PORT`, `SWARMQ_TEST_VALKEY_PORT`).

### Success Criteria

- Die Tests aus U1 kippen von rot auf grün, und die verschachtelten Fälle aus U4 belegen die vollständige Liste über Unit-, Integrations- und Ende-zu-Ende-Stufe.
- Kein Pfad im Modul liefert noch eine Gliederliste, die kürzer ist als die Gliederzahl.
- Die Golden-Fixture bleibt grün, ohne neu erzeugt zu werden.
- Ein Betreiber kann aus Changelog und Release-Notes bestimmen, ob er betroffen war (AE12).
- **Nachgelagert, außerhalb der Abnahme dieses Plans:** Nach der Veröffentlichung liefern die 19 betroffenen Muster der Konformitätsstufe im Repo `swarmq-loadsuite` vollständige Ergebnislisten, und deren Ausnahme-Einträge werden dort zurückgenommen.

### Outstanding Questions

Keine offenen Produktfragen. Zwei Punkte liegen bewusst bei Georg und blockieren die Umsetzung nicht:

- Versionsklasse des Fix-Releases (0.3.1 oder 0.4.0). Die Changelog-Rubrik ist in KD5 entschieden, die Zahl nicht.
- Ob 0.2.0 und 0.3.0 auf PyPI zurückgezogen (`yank`) werden sollen. Dieser Plan schlägt es nicht vor — die Offenlegung aus R11 ist die hier vorgesehene Antwort.

### How This Work Fits Together

Der Debug-Feature-Backlog der Lasttest-Suite hat zehn Einträge. Drei waren Fehler und sind mit `0.3.0` behoben (B8, B6 und die erste Hälfte von B3); der Abgleich steht in `docs/plans/2026-08-30-001-fix-inventar-debug-backlog.md`. B10 ist der vierte Fehler und der erste, der aus dem Workflow-Pfad kommt — er fiel auf, weil die Konformitätsstufe zum ersten Mal Workflows durch den Stack fährt. B9 stammt aus demselben Bau und demselben Modulbereich, ist aber ein anderer Gegenstand und bleibt hier draußen.

### Sources / Research

- `docs/debug-feature-backlog.md` (Repo `swarmq-loadsuite`), Eintrag B10, Commit `576279f` vom 2026-08-29.
- Verifikation am Code: `src/swarmq/workflow_orchestration.py` — `_frontier:282`, `advance:381`, `_on_group_member:443`, `_on_container_complete:485`, `_ordered_member_ids:598`, `_materialise:642`, `WorkflowHandle.children:122`; `src/swarmq/backend/valkey/results.py:340` (`store_workflow_state` überschreibt blind); `src/swarmq/workflow.py:24` (Tiefengrenze); `tests/integration/backend/_golden.py:92` (eingefrorenes Zustandsformat).
- Abwesenheitsbefund: keine Konstruktion einer Gruppe mit zusammengesetztem Glied in `tests/`.
- Dokument-Review vom 2026-08-30 (Kohärenz, Feasibility, Adversarial, Produkt, Scope). Er hat den ursprünglichen Entwurf dieses Plans — ein neues Zustandsfeld je zusammengesetztem Glied — an zwei unabhängigen Stellen widerlegt; KTD1 trägt das Ergebnis.

---

## Planning Contract

### Key Technical Decisions

- **KTD1 — Die Gliedkennung wird aus der Topologie abgeleitet, nicht im Zustand geführt.**
  `_ordered_member_ids` bestimmt die Kennung je Gliedindex rekursiv aus der Knotenart:

  | Knoten am Gliedpfad | Kennung |
  |---|---|
  | `Signature` | `step_tasks[pfad]` (wie heute) |
  | `Chain` | rekursiv der letzte Schritt, `{pfad}/{len(steps)-1}` |
  | `Chord` | rekursiv der Body, `{pfad}/b` |
  | `Group` | `{wf_id}:agg:{pfad}` |

  Das bildet exakt die beiden Zweige ab, die `_materialise` heute schon erzeugt — es wird nichts Neues geschrieben, nur das Vorhandene gefunden.

  **Verworfene Alternative A: ein eigenes Zustandsfeld je zusammengesetztem Glied, beim Gliedabschluss geschrieben und persistiert.** Das war der ursprüngliche Entwurf dieses Plans; der Review hat ihn an zwei unabhängigen Stellen widerlegt. Erstens **trägt er unter Nebenläufigkeit nicht:** `advance()` lädt den Zustand einmal (`:381`) und `store_workflow_state` ist ein blindes `SET` des ganzen Dokuments (`backend/valkey/results.py:340`, Docstring: „Overwrites on each call"). Schließen zwei zusammengesetzte Glieder gleichzeitig ab — AE3, der Normalfall bei `concurrency: 10` —, überschreibt der zweite Schreiber den Merker des ersten; mit dem Guard wird daraus ein hart gescheiterter Workflow statt der heutigen gekürzten Liste. Zweitens **bricht er ein blockierendes Gate:** Das Zustandsformat ist über die Golden-Fixture eingefroren.

  **Verworfene Alternative B: `_materialise` für jedes zusammengesetzte Glied immer synthetisch schreiben lassen, damit die Kennung allein aus dem Pfad folgt.** Der tragende Grund ist nicht der Speicherplatz für die zweite Ergebniskopie, sondern R10: B ändert die an `record_member` übergebene Kennung für Ketten- und Chord-Glieder. `chord_complete.lua` feuert nur bei `SCARD == N` — ein über das Upgrade hinweg erneut zugestellter Gliedabschluss legt dann zwei Kennungen desselben Glieds ins Set, und die Gruppe feuert **nie**. Ein Hänger ist teurer als der Fehler, den wir beheben.

- **KTD2 — Der Fan-in-Pfad lädt den Zustand frisch, bevor er die Liste baut.**
  Nach dem feuernden `record_member` steht fest, dass alle Glieder fertig sind — also auch, dass alle ihre `step_tasks`-Schreibungen versucht wurden. Ein frischer Lesevorgang unmittelbar vor dem Bau der Liste holt den zuletzt persistierten Stand statt des Schnappschusses vom Eintritt in `advance()`. Das schließt die Lücke aus KTD5 **nicht**, es verkleinert sie: der „veraltete Schnappschuss"-Anteil verschwindet, der „verlorene Schreibvorgang"-Anteil bleibt. Kosten: ein Backend-Lesevorgang je Gruppenabschluss.

- **KTD3 — Der Vollständigkeitsguard sitzt in `_ordered_member_ids` und wirft `WorkflowValidationError`.**
  Eine Stelle deckt beide Verwendungen ab (Gruppenergebnis und Chord-Body-Eingabe), und es ist dieselbe Stelle, an der der Defekt entstand. Fehlertyp: der vorhandene `WorkflowValidationError`, der dort schon für verletzte Topologie-Invarianten geworfen wird (`:470`). Ein neuer Fehlertyp wurde verworfen: Der Fehlerkatalog-Guard verlangt für jede `SwarmQError`-Unterklasse eine Einordnung, und der Erkenntnisgewinn wäre klein. Ein aus `advance()` geworfener Fehler erreicht den Aufrufer — `worker/results.py` markiert den Workflow über `advancer.fail(...)` als gescheitert, `handle.get()` wirft also, statt zu hängen.

- **KTD4 — Die Definitionsreihenfolge bleibt über den Gliedindex `i` bestimmt, nicht über die Abschlussreihenfolge.** Unverändert gegenüber heute; die Korrektur ersetzt nur, *wie* die Kennung zu einem Index gefunden wird.

- **KTD5 — Das verlorene Zustands-Update bleibt offen und wird benannt.**
  `advance()` ist ein Read-Modify-Write ohne CAS auf ein Dokument, das blind überschrieben wird. Zwei nebenläufige Advances können einander die `step_tasks`-Einträge löschen — das gilt **heute schon** und unabhängig von diesem Plan; heute äußert es sich höchstens in einem fehlenden `children()`-Eintrag. Mit dem Guard aus KTD3 wird daraus ein sichtbarer Fehler. Das ist die gewollte Richtung (KD1), aber es macht einen vorbestehenden Mangel erstmals betriebsrelevant. Ihn zu beheben — CAS, Lua-Merge oder ein Lock auf dem Zustandsdokument — ist ein eigener Gegenstand mit eigenem Plan. Dieser Plan trägt ihn als Risiko, nicht als Arbeitspaket.

### High-Level Technical Design

Die Ursache ist eine Lücke zwischen zwei Kennungsräumen. Einfache Glieder tragen eine **Schritt**-Kennung unter `<gruppe>/g<i>`; zusammengesetzte Glieder tragen eine **Aggregat**-Kennung, die im Zustand nirgends steht. Die Zusammenstellung kennt nur den ersten Raum.

Die naheliegende Reaktion — den zweiten Raum persistent machen — ist die falsche (KTD1, Alternative A). Der richtige Weg ist, dass die Zusammenstellung ihn **ausrechnet**, denn er ist vollständig determiniert: durch die Workflow-Kennung, den Gliedpfad und die Knotenart, und die Topologie liegt ohnehin im Zustand.

Konkret:

1. Eine rekursive Hilfsfunktion bestimmt zu einem Topologie-Knoten und seinem Pfad die Kennung seines Abschlussergebnisses, nach der Tabelle aus KTD1. Sie schreibt nichts und liest nur `step_tasks`.
2. `_ordered_member_ids` ruft sie je Gliedindex auf, statt blind `<gruppe>/g<i>` nachzuschlagen. Es braucht dafür den Gruppenknoten und die Workflow-Kennung — beide liegen an der Aufrufstelle bereits vor.
3. Vor dem Bau der Liste wird der Zustand frisch geladen (KTD2).
4. Findet die Zusammenstellung nicht `expected_n` Kennungen, wirft sie — statt eine kurze Liste zu liefern (KTD3).

Die Rekursion (R4) fällt heraus, ohne eigenes Zutun: Die Hilfsfunktion ruft sich je Ebene selbst auf, und die Tiefengrenze von `_validate` deckelt sie.

`_materialise`, `_on_group_member`, `_on_container_complete` und die Fan-in-Zählung bleiben **unverändert**. Das ist die eigentliche Qualität dieses Zuschnitts: Der Eingriff liegt vollständig auf der Leseseite.

### Assumptions

- Für jeden Gliedpfad liefert die Hilfsfunktion dieselbe Kennung, die `_materialise` beim Abschluss dieses Glieds zurückgegeben hat. Das ist die tragende Annahme des ganzen Entwurfs und gehört in U2 explizit getestet, nicht nur angenommen.
- Der Backend-Zugriff auf ein Aggregat-Ergebnis unterliegt derselben TTL-Behandlung wie jedes andere Gliedergebnis; R7 verlangt keine neue Mechanik, nur dass der bestehende Pfad erreicht wird.

### Sequencing

U1 zuerst — der reproduzierende Test steht vor der Korrektur. U2 trägt die Korrektur, U3 den Guard, U4 die verbleibende Abdeckung, U5 die Veröffentlichung. U3 kann erst nach U2 grün laufen, weil er sonst genau den Defekt meldet, den U2 behebt.

---

## Implementation Units

### U1. Den Defekt als Fehlschlag festhalten

- **Goal:** Tests reproduzieren am unveränderten Stand, dass eine Gruppe mit zusammengesetztem Glied eine gekürzte Liste liefert und ein Chord-Body eine gekürzte Eingabe erhält.
- **Requirements:** R1, R3. Belegt AE1 und AE4 zunächst als Fehlschlag.
- **Dependencies:** keine.
- **Files:** `tests/unit/test_workflow_advancer.py`
- **Execution note:** Vom Fehlschlag ausgehen. Diese Tests müssen vor der Korrektur rot sein und den Defekt zeigen, nicht einen Konstruktionsfehler des Tests.
- **Approach:**
  1. Den vorhandenen `Driver`-Aufbau des Moduls übernehmen — nicht neu erfinden; die bestehenden Gruppen- und Chord-Tests zeigen, wie Zustand, Chord-Speicher und Backend dort gestellt werden.
  2. Einen Fall `group(sig, chain(sig, sig))` konstruieren, alle Schritte der Reihe nach abschließen lassen und die Länge der Endliste prüfen.
  3. Einen Fall `chord(group(sig, chain(sig, sig)), sig)` konstruieren und die Eingabe prüfen, mit der der Body eingereiht wird.
  4. Beide einmal gegen den ungefixten Stand laufen lassen und den beobachteten Ist-Zustand im Testnamen oder einem knappen Kommentar festhalten, damit später erkennbar ist, wogegen sie schützen.
- **Patterns to follow:** Die bestehenden Gruppen-Fan-in-Tests in `tests/unit/test_workflow_advancer.py`.
- **Test scenarios:**
  - `group(a, chain(b, c))`: Endliste hat zwei Einträge, der zweite ist das Ergebnis von `c`.
  - `chord(group(a, chain(b, c)), body)`: `body` wird mit einer zweielementigen Liste eingereiht.
- **Verification:** Beide Tests scheitern am unveränderten Stand, und zwar mit einer zu kurzen Liste — nicht mit einem Fehler aus dem Testaufbau.

### U2. Die Gliedkennung ableiten statt nachschlagen

- **Goal:** `_ordered_member_ids` findet für jeden Gliedindex eine Kennung, gleich ob das Glied einfach oder zusammengesetzt ist — ohne neuen Zustandseintrag.
- **Requirements:** R1, R2, R3, R4, R6, R7, R9, R10. Umgesetzt nach KTD1, KTD2, KTD4. Belegt AE1–AE9 und AE11.
- **Dependencies:** U1.
- **Files:** `src/swarmq/workflow_orchestration.py`, `tests/unit/test_workflow_advancer.py`
- **Approach:**
  1. Eine rekursive Hilfsfunktion ergänzen, die zu Knoten und Pfad die Kennung des Abschlussergebnisses liefert, nach der Tabelle aus KTD1. Sie ist rein lesend. Im Docstring festhalten, dass sie die beiden `_materialise`-Zweige spiegelt — driften die beiden auseinander, bricht die Korrektur still, und dieser Satz ist die einzige Warnung davor.
  2. `_ordered_member_ids` auf die Hilfsfunktion umstellen; Signatur um Gruppenknoten und Workflow-Kennung erweitern. Beide liegen an der Aufrufstelle (`_on_group_member`) bereits vor.
  3. Den Zustand auf dem Fan-in-Pfad frisch laden, bevor die Liste gebaut wird (KTD2).
  4. Nichts an `_materialise`, `_on_group_member`, `_on_container_complete` oder der Fan-in-Zählung ändern. Zeigt sich beim Testen, dass R7 den bestehenden TTL-Pfad doch nicht trifft, ist das ein Befund — kein stiller Umbau.
  5. Prüfen, dass die Chord-Header-Verwendung (`gp_last == "h"`) automatisch mitgeht; sie ruft dieselbe Methode.
  6. Prüfen, dass `build_initial_workflow_state` unangetastet bleibt und die Golden-Fixture ohne Neuerzeugung grün ist (AE11).
- **Patterns to follow:** Die bestehende Rekursionsform von `_frontier` (`:282`) für den Knotenart-Abgleich; die bestehende Schleifenform in `_ordered_member_ids`.
- **Test scenarios:**
  - `group(a, chain(b, c))` liefert `[erg(a), erg(c)]` (AE1).
  - `group(a, group(b, c))` liefert `[erg(a), [erg(b), erg(c)]]` (AE2).
  - `group(chain(a, b), chain(c, d))` liefert `[erg(b), erg(d)]` — kein einfaches Glied dabei (AE3).
  - `chord(group(a, chain(b, c)), body)` reiht `body` mit `[erg(a), erg(c)]` ein (AE4).
  - `group(a, chain(b, group(c, d)))` liefert `[erg(a), [erg(c), erg(d)]]` — zwei Ebenen (AE5).
  - **Deckungstest der tragenden Annahme:** Für jede Knotenart liefert die Hilfsfunktion dieselbe Kennung, die `_materialise` beim Abschluss desselben Glieds zurückgibt.
  - Ein erneut zugestellter Gliedabschluss ändert nichts an der übergebenen Kennung; die Gruppe feuert einmal (AE8, R10).
  - Ein abgelaufenes Aggregat-Ergebnis führt zu `WorkflowResultExpired` mit der bestehenden Meldung (AE9).
  - Die Tests aus U1 laufen grün.
- **Verification:** Die U1-Tests kippen von rot auf grün, die verschachtelten Fälle liefern die vollständige Liste in Definitionsreihenfolge, und `build_initial_workflow_state` ist nachweislich unverändert.

### U3. Eine unvollständige Gliederliste laut machen

- **Goal:** Bleibt die Gliederliste kürzer als die Gliederzahl, wird sie als Fehler gemeldet statt ausgeliefert — an beiden Verwendungsstellen und für jede Gruppe, nicht nur für verschachtelte.
- **Requirements:** R5. Belegt AE10. Setzt KD1 und KTD3 um.
- **Dependencies:** U2.
- **Files:** `src/swarmq/workflow_orchestration.py`, `tests/unit/test_workflow_advancer.py`
- **Approach:**
  1. Am Ende von `_ordered_member_ids` gegen `expected_n` prüfen und bei Abweichung `WorkflowValidationError` werfen.
  2. Die Meldung trägt den Gruppenpfad, die erwartete und die tatsächliche Zahl — sie ist der einzige Hinweis, den ein Betreiber im Fehlerfall bekommt. Sie sollte den wahrscheinlichsten Auslöser andeuten (fehlender Schritt-Marker), damit sie nicht wie ein reiner Selbstwiderspruch von swarmq liest.
  3. Im Docstring festhalten, dass das wortlose Überspringen der Ursprung von B10 war; ohne diesen Satz sieht der Guard nach überflüssiger Vorsicht aus und wird beim nächsten Umbau wegvereinfacht.
  4. Den bestehenden Testbestand einmal vollständig fahren: Jede Gruppe in `tests/` hat heute ausschließlich einfache Glieder, deren Schlüssel schon zur `apply()`-Zeit gesetzt sind — der Guard darf dort nichts brechen. Tut er es doch, greift Stop-Condition (a).
- **Patterns to follow:** Die bestehende `WorkflowValidationError`-Meldungsform im selben Modul (`:470`).
- **Test scenarios:**
  - Einem Zustand fehlt ein Gliedmerker: Fehler mit Gruppenpfad und beiden Zahlen, kein gekürztes Ergebnis.
  - Derselbe Zustand als Chord-Header: derselbe Fehler, bevor der Body eingereiht wird.
  - **Flache Gruppe** mit fehlendem Schritt-Marker: derselbe Fehler (AE10, KD1).
  - Der vollständige Fall wirft nicht.
- **Verification:** Kein Pfad im Modul liefert nach der Umsetzung noch eine Liste, die kürzer ist als die Gliederzahl, und der bestehende Testbestand bleibt grün.

### U4. Die fehlende Abdeckung schließen

- **Goal:** Die Topologie-Familie „Gruppe mit zusammengesetztem Glied" ist über die Teststufen hinweg abgedeckt, nicht nur an der Nahtstelle.
- **Requirements:** R1–R8, R10. Belegt AE6 und AE7.
- **Dependencies:** U2, U3.
- **Files:** `tests/integration/test_workflow_backend.py`, `tests/e2e/test_workflows.py`, `tests/property/test_workflow_advancer_properties.py`
- **Approach:**
  1. **Integration — Persistenznachweis, nicht Ablauf.** `tests/integration/test_workflow_orchestration.py` startet keinen Worker und keinen Consumer; sein eigener Docstring verweist die vollständige Fortschaltung an die e2e-Stufe. Der Integrationsteil prüft deshalb, dass der Zustand mit verschachtelter Topologie unverändert durch Serialisierung und Speicher geht — in der Form von `tests/integration/test_workflow_backend.py`.
  2. **Ende-zu-Ende:** einen verschachtelten Fall über den echten Broker, mit dem Ergebnis über `handle.get()` — die Sicht, die ein Nutzer hat. Hier gehört auch das Szenario „liefert beide Ergebnisse" hin.
  3. **Nebenläufigkeit (AE6):** zwei zusammengesetzte Glieder derselben Gruppe von **demselben** Zustands-Schnappschuss aus abschließen lassen, beide zurückschreiben, dann den Fan-in fahren. Das ist der Fall, an dem der verworfene Entwurf zerbricht; er gehört als Regressionsschutz in den Bestand, damit niemand versehentlich zu einem zustandsführenden Ansatz zurückkehrt. Der sequenzielle `Driver` der Unit-Stufe kann das nicht — der Test braucht zwei Advancer-Instanzen auf gemeinsamem Backend.
  4. **Prozesswechsel (AE7):** das zusammengesetzte Glied auf einer Advancer-Instanz abschließen, den Zustand neu laden, den letzten Gliedabschluss auf einer zweiten verarbeiten.
  5. **Property-Stufe:** prüfen, ob die vorhandene Generierung um zusammengesetzte Glieder erweiterbar ist — sie erzeugt heute ausschließlich flache Topologien. Wenn nicht ohne Umbau, das begründet festhalten statt erzwingen; der Umbau der Generierung wäre ein eigener Gegenstand, der dann aber auch als solcher entstehen muss und nicht vergessen werden darf.
  6. `handle.children()` in einem der Fälle mitprüfen: R8 muss nachweislich unverändert gelten.
- **Patterns to follow:** Die bestehenden Workflow-Fälle der jeweiligen Stufe; die isolierte Docker-Umgebung mit abweichenden Ports aus dem 0.3.0-Lauf.
- **Test scenarios:**
  - Integration: ein Zustand mit verschachtelter Topologie überlebt Serialisierung und Speicher unverändert.
  - Ende-zu-Ende: ein verschachtelter Workflow liefert über `handle.get()` die vollständige Liste.
  - Nebenläufigkeit: zwei zusammengesetzte Glieder vom selben Schnappschuss aus, vollständige Liste (AE6).
  - Prozesswechsel: zwei Advancer-Instanzen, vollständige Liste (AE7).
  - `children()` enthält die eingereihten Schritte und keine Aggregat-Einträge (R8).
- **Verification:** Alle Stufen laufen grün, und der Nebenläufigkeitstest scheitert nachweislich gegen eine zustandsführende Variante der Korrektur.

### U5. Offenlegen und abgleichen

- **Goal:** Ein Betreiber kann aus den veröffentlichten Artefakten bestimmen, ob er betroffen war und was er tun muss; der Backlog-Abgleich weist B10 als behoben aus.
- **Requirements:** R8, R11. Belegt AE12. Setzt KD4 und KD5 um.
- **Dependencies:** U4.
- **Files:** `website/reference/changelog.md`, `website/reference/changelog.de.md`, `docs/plans/2026-08-30-001-fix-inventar-debug-backlog.md`, ggf. `src/swarmq/workflow.py`
- **Approach:**
  1. Changelog-Eintrag in **beiden** Sprachfassungen, unter der Rubrik „Changed (breaking)" statt „Fixed" (KD5). Inhalt: betroffene Versionen (0.2.0, 0.3.0); betroffene Topologien (Gruppe oder Chord-Header mit einem Glied, das selbst `chain`/`group`/`chord` ist); **das Prüfkriterium** — betroffen ist jeder Lauf, dessen *Definition* ein solches Glied enthält, denn gespeicherte Ergebnisse erlauben die Unterscheidung prinzipiell nicht; die neue Fehlerklasse für flache Gruppen (KD1); die Betriebsanweisung, verschachtelte Workflows vor dem Upgrade leerzufahren (KD4); und die Bedingung, dass der gesamte Fleet wechseln muss.
  2. Prüfen, ob der `Group`-Docstring die Zusage präzise genug formuliert; falls er die Verschachtelung offenlässt, sie benennen.
  3. Im Fix-Inventar B10 **an beiden Stellen** nachziehen: den Abschnitt „Neu gemeldet — nicht von 0.3.0 berührt" in einen Behoben-Abschnitt unter **neuer Versionsüberschrift** überführen (nicht unter „Behoben in 0.3.0" — dieser Fix erscheint dort nicht), und die B10-Zeile aus der Tabelle „Nicht behoben — weiterhin offen" entfernen.
  4. KTD5 als offenen Gegenstand festhalten, damit das verlorene Zustands-Update nicht mit diesem Plan verschwindet.
- **Patterns to follow:** Die Changelog-Einträge der 0.3.0-Fixes, insbesondere die Rubrizierung der Startverweigerung; die War/Jetzt-Form im Fix-Inventar.
- **Test scenarios:** Keine — Dokumentationsarbeit. AE12 wird durch Lesen abgenommen: Kann jemand, der den Eintrag liest und den Code nicht kennt, entscheiden, ob er betroffen ist?
- **Verification:** Beide Changelog-Fassungen nennen Defekt, Prüfkriterium, Umfang und Betriebsanweisung; das Fix-Inventar führt B10 an keiner Stelle mehr als offen.

---

## Verification Contract

| Gate | Befehl | Gilt für |
|---|---|---|
| Lint | `uv run --extra test --extra reload ruff check src/ tests/` | alle Units |
| Typen | `uv run --extra test --extra reload mypy src/swarmq tests/support tests/contracts` | alle Units |
| Schneller Lauf | `uv run --extra test --extra reload pytest tests/unit -q --reruns 1 --reruns-delay 1` | U2, U3 |
| Property | `uv run --extra test --extra reload pytest tests/property -m property -q` | U4 |
| Integration | `uv run --extra test --extra reload pytest tests/integration -q` | U2, U4 |
| Ende-zu-Ende | `uv run --extra test --extra reload pytest tests/e2e -q` | U4 |

**Ausnahme U1:** Die dort neu angelegten Unit-Tests sind bis zum Abschluss von U2 planmäßig rot. Das Unit-Gate ist ab U2 blockierend, nicht schon nach U1 — wer es dort anlegt, entschärft die Tests, bis sie grün sind, und zerstört genau den Reproduktionswert, den U1 herstellen soll.

Das Property-Gate braucht `-m property`: `addopts` in `pyproject.toml` schließt den Marker per Default aus, und `tests/property/conftest.py` hängt ihn an jede Datei dort — ohne den Schalter sammelt der Lauf null Tests und endet mit Exit 5. Der Befehl oben ist derselbe wie im `unit-tests`-Job der CI.

Integration und Ende-zu-Ende brauchen RabbitMQ und Valkey. Auf Georgs Rechner belegt die Loadsuite-Umgebung die Standardports — die Läufe gehen über eigene Container auf abweichenden Ports (`SWARMQ_TEST_RABBITMQ_PORT`, `SWARMQ_TEST_VALKEY_PORT`).

Alle übrigen Gates sind blockierend.

Der Chaos-Lauf ist hier nicht nötig: Der Gegenstand ist Ergebnis-Zusammenstellung, keine Störungsresistenz. Die CI-Regel des `chaos-tests`-Jobs listet fünf Pfade (`src/swarmq/worker.py`, `worker/**`, `broker/**`, `backend/**`, `contracts/**`); `workflow_orchestration.py` steht in keinem davon, der Job läuft also als überspringbarer manueller Schritt.

---

## Definition of Done

**Global**

- Alle elf Anforderungen sind durch mindestens ein Arbeitspaket abgedeckt und durch mindestens ein Testszenario belegt — außer R11, das durch Lesen abgenommen wird (AE12).
- Die blockierenden Gates des Verification Contract laufen grün.
- Die U1-Tests sind nachweislich einmal gegen den ungefixten und einmal gegen den gefixten Stand gelaufen und zeigen den Unterschied.
- Kein Pfad im Modul liefert noch eine Gliederliste, die kürzer ist als die Gliederzahl.
- `build_initial_workflow_state` ist unverändert, und die Golden-Fixture ist grün, ohne neu erzeugt worden zu sein.
- Verworfene Ansätze sind aus dem Stand entfernt: keine auskommentierten Zwischenstände, keine ungenutzten Hilfsfunktionen.

**Je Arbeitspaket**

- U1: Zwei Tests reproduzieren den Defekt am ungefixten Stand.
- U2: Verschachtelte Gruppen und Chord-Header liefern die vollständige Liste in Definitionsreihenfolge, bis zur geltenden Tiefengrenze, ohne neuen Zustandseintrag.
- U3: Eine unvollständige Liste erzeugt einen Fehler mit Gruppenpfad und beiden Zahlen, auch bei flachen Gruppen; der bestehende Testbestand bleibt grün.
- U4: Integration, Ende-zu-Ende, Nebenläufigkeit und Prozesswechsel sind abgedeckt; `children()` ist unverändert; die Property-Lücke ist geschlossen oder begründet festgehalten.
- U5: Beide Changelog-Fassungen tragen Defekt, Prüfkriterium, Umfang und Betriebsanweisung; das Fix-Inventar führt B10 nirgends mehr als offen; KTD5 ist als eigener Gegenstand notiert.

---

## Risks & Dependencies

- **Bereits abgeschlossene Workflows bleiben falsch.** Der Fix wirkt auf Workflows, die nach dem Upgrade laufen. Ergebnisse, die eine verschachtelte Gruppe unter `v0.2.0` oder `v0.3.0` geliefert hat, waren gekürzt und bleiben es — sie liegen als gespeicherte Werte im Backend und in allem, was daraus gerechnet wurde. **Das Tückische ist die Nichterkennbarkeit:** Eine gekürzte Liste ist im Nachhinein von einer vollständigen nicht zu unterscheiden, das ist ja die Fehlerklasse. Die einzige beantwortbare Frage ist die nach der Workflow-*Definition*. Genau deshalb ist das Prüfkriterium in R11 Pflichtbestandteil der Veröffentlichung und nicht nur ein Hinweis.
- **Der Guard wirkt breiter als der Fehler.** `_ordered_member_ids` läuft bei jedem Gruppen-Fan-in und jedem Chord-Header. Jede bisher unbekannte Ursache einer kurzen Liste — auch bei flachen Gruppen, etwa ein im publish→mark-Fenster verlorener Schritt-Marker — scheitert nach der Umsetzung laut, wo sie heute ein Ergebnis liefert. Gewollt (KD1), aber ein Betreiber, der nicht verschachtelt, darf den Plan nicht als „betrifft mich nicht" lesen.
- **Das verlorene Zustands-Update (KTD5) wird durch den Guard erstmals betriebsrelevant.** Zwei nebenläufige Advances können einander `step_tasks`-Einträge löschen; das gilt heute schon, äußert sich aber nur in `children()`. Künftig kann daraus ein gescheiterter Workflow werden. KTD2 verkleinert das Fenster, schließt es nicht. Wenn sich das im Lasttest zeigt, braucht es den eigenen Plan aus KTD5 — nicht eine Aufweichung des Guards.
- **Ein zum Upgrade-Zeitpunkt laufender verschachtelter Workflow scheitert.** Das ist die Entscheidung aus KD4 und die richtige Richtung, heißt aber: Ein Upgrade mitten im Lauf führt diesen Workflow zum Fehler statt zu einem falschen Ergebnis. Wer das nicht will, fährt die Workflows vorher leer.
- **Rollierendes Upgrade mit gemischten Worker-Versionen.** Verarbeitet ein alter Worker den letzten Gliedabschluss, kürzt er still weiter; ein neuer Worker auf einem von alten Workern geschriebenen Zustand kann hart scheitern. R5, R6 und R9 gelten erst, wenn der gesamte Fleet gewechselt hat.
- **Die Property-Stufe deckt die Familie möglicherweise weiterhin nicht ab.** Die vorhandene Generierung erzeugt nur flache Topologien. Bleibt es dabei, bleibt die Lücke, die den Defekt zwei Releases lang getragen hat, teilweise offen — U4 verlangt in dem Fall eine Begründung statt eines erzwungenen Umbaus, aber der Umbau sollte dann als eigener Gegenstand entstehen und nicht vergessen werden.
- **Ohne Veröffentlichung ändert sich für keinen Nutzer etwas.** Der Release-Schnitt ist benannte Nachfolgearbeit bei Georg. Bis dahin bleibt der Fehler in 0.2.0 und 0.3.0 ausgeliefert, und die Konformitätsstufe der Lasttest-Suite behält ihre Ausnahmen.
