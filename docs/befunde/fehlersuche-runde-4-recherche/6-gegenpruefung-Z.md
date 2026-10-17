# Gegenprüfung Spur Z (Zustandsmodelle), U17

Plan: `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md` (U17, KTD3, KTD9, R4). Ablauf: `docs/befunde/README.md`. Geprüft: R4Z-01 bis R4Z-05 aus `1-zustandsmodelle.md`, dazu die Invarianten der Lock- und Leader-Maschine (U6, U7). Stand: Branch `test/fehlersuche-runde-4` (= v0.12.3 unter `src/`). Der Prüfer hat keinen der Verdachte gefunden.

## Ergebnis

| Verdacht | Einstufung | Prio | Kern der Begründung |
|---|---|---|---|
| R4Z-01 | ENTSCHEIDUNG | (wäre P3) | Plan 2026-08-30-002 KD1, Commit 02a2da4 und Changelog 0.4.0 nehmen das laute Scheitern bei fehlendem Schritt-Marker im publish→mark-Fenster ausdrücklich hin. „Entscheidung überdenken?“ empfohlen, mit neuem Argument |
| R4Z-02 | FEHLER | P3 | verletzt die Hauptzusage von E-040 („nie still in den `get()`-Timeout“); die Bewusste Folge von E-040 verbietet nur das Fail-Mark, nicht das Weiterschalten, und ihr Zweck (BZ-13: „weiterlaufenden Workflow nicht kippen“) trifft hier nicht zu |
| R4Z-03 | NICHT HALTBAR | – | Zwei gleichzeitige Zustellungen derselben Nachricht verhindert der Running-Marker des Ausführungsschutzes (`execution_guard.py:152`); die Maschine bildet nur Done-Marker nach. Was übrig bleibt (Crash-Fenster, `SWARMQ_EXECUTION_GUARD=false`, Marker nach blockiertem Loop abgelaufen), ist als „seltenes idempotentes Doppel-Enqueue“ entschieden |
| R4Z-04 | FEHLER | P3 | verletzt E-009, E-043, `delivery-semantics.md` („the redelivered original is then skipped“) und die Konflikt-Zeile in KTD5 von Plan 2026-10-08-1541. Das Gate davor setzt sich durch, und ein Erfolgsergebnis wird mit einem Fehlerergebnis überschrieben |
| R4Z-05 | ENTSCHEIDUNG | – | folgt direkt aus E-051 (Retry-Zähler streng) und der Superseded-Regel (KTD5, E-009). Erwartet hatte das Gegenteil nur ein Testszenario im Plan, keine Zusage. Ein Nachtrag zu E-051 ist sinnvoll |

Keiner der fünf Verdachte berührt eine der offenen LÜCKEN FS-25, FS-26, FS-29, FS-32, F3-32 bis F3-38 oder F3-41 (R4). Am nächsten liegen R4Z-01 und R4Z-02 an FS-32, weil beide einen Workflow auf `running` stehen lassen. FS-32 betrifft aber nur den Cancel-Zweig; W9 der Maschine nimmt ihn getrennt aus. R4Z-04 berührt F3-27 und F3-34 (Zustelllimit gegen Ausführungsschutz), aber keine der offenen Fragen dort.

Nachgeprüft: Alle sechs Nachweise scheitern mit `--runxfail` an der fachlichen Assertion, gegen die Fakes (`uv run --extra test pytest tests/property/stateful/test_workflow_machine.py tests/property/stateful/test_retry_chain_machine.py -m property --runxfail`: 6 failed, 31 passed) und gegen Valkey (`uv run just integration -k "workflow_machine_valkey or retry_chain_machine_valkey" --runxfail`: 5 failed, 33 passed; R4Z-03 hat dort keinen xfail-Nachweis). Die Meldungen:

- R4Z-01: `status == failed`, `WorkflowValidationError: group at '' resolved 1 of 2 member results …`
- R4Z-02: `dlq von M2(W1/) bestätigt, Status running (W4, E-040)`
- R4Z-03: `Body 2-mal publiziert`
- R4Z-04: `Erfolgsergebnis überschrieben: 'WorkerHungError: delivered 3 times …'` bzw. `überholter Versuch in der DLQ`
- R4Z-05: `UI-Republish der Retry-Kopie still bestätigt: kein Lauf`

Formhinweis: `test_suspect` in `test_retry_chain_machine.py` und in der Valkey-Variante trägt `xfail(strict=True)` ohne `raises=AssertionError`. Ein Absturz mit einer anderen Ausnahme zählte dort ebenfalls als erwartet. Die Workflow-Nachweise tragen `raises=AssertionError`. Beim Umbenennen auf F4-IDs sollte `raises` nachgezogen werden.

---

## R4Z-01: Gruppenglied ohne persistierten Marker kippt den Workflow beim Fan-in auf `failed`

**Einstufung: ENTSCHEIDUNG** („Entscheidung überdenken?“ mit neuem Argument; als Fehler gewertet wäre es P3)

### Entscheidungs-Abgleich

- Register: keine E-ID (`grep` über `applies_when`, `tags`, `title` nach workflow, group, marker, publish-then-mark: nur E-039 und E-040, beide nicht einschlägig).
- **Plan `docs/plans/2026-08-30-002-fix-workflow-group-composite-members-plan.md:57` (KD1):** „**Der Preis ist ausdrücklich mitentschieden:** Er wirkt für jede Gruppe und jeden Chord-Header, auch flache, und macht damit auch bisher unbekannte Ursachen einer kurzen Liste laut — etwa einen im publish→mark-Fenster verlorenen Schritt-Marker.“ Ebenso `:348` (Risiko, „Gewollt (KD1)“).
- **Commit 02a2da4** („unvollstaendige Gliederliste laut machen“): „Ein im publish-then-mark-Fenster verlorener Schritt-Marker scheitert kuenftig laut, wo er heute ein Ergebnis liefert. Gewollt, und im Plan als Risiko und in der Nutzerkommunikation (U5) gefuehrt.“
- **Changelog 0.4.0** (`website/reference/changelog.md:2951-2957`): „a member step marker lost for any other reason (a step lost in the publish-then-mark window, for instance) now fails loudly where it previously produced a result. That is the point: a wrong answer you cannot detect is worse than a visible failure.“
- **Test, der das Verhalten als erwartet prüft:** `tests/unit/test_workflow_advancer.py:536` `test_guard_catches_lost_marker_of_composite_member`, laut Kommentar „der realitaetsnaechste Fall: der Abschlussschritt eines Kettenglieds verliert seinen Marker (publish-then-mark-Fenster)“, erwartet `WorkflowValidationError` mit „1 of 2“. Das ist genau die Topologie des Nachweises (`group(a, chain(b, c))`). Der Test hat keinen `entscheidung`-Marker, prüft das Verhalten aber ausdrücklich.
- Gegenläufige Zusagen: `_enqueue_node`-Docstring („a crash never loses the step (at worst a rare idempotent double-enqueue)“), `_refresh_step_tasks`-Docstring und KTD11 aus Plan 2026-09-08-001 („bleibt als Schutz gegen das Publish-vor-Persistenz-Fenster: ein Schritt … kann schneller enden als die Persistenz“), `website/guides/workflows.md:232` (`failed` nur bei terminalem Scheitern eines Schritts). Sie sind allgemeiner und jünger (KTD11) als KD1, nehmen den Fall aber nicht ausdrücklich zurück. KD1 ist die spezifische, bewusste Festlegung.

### Widerlegungsversuche

1. *Entschieden?* Ja, siehe oben. Damit greift die Widerlegung als Fehlermeldung.
2. *Realismus:* Der Mechanismus ist echt und läuft deterministisch in beiden Stufen. Das Szenario ist nicht konstruiert: Der echte Advancer markiert erst nach dem Publish (`workflow_orchestration.py:498-559`), und `_terminal_result_id` schlägt ein Blatt in `step_tasks` nach (`:623-624`), statt die Kennung abzuleiten. Häufigkeit: Das Fenster zwischen Publisher-Confirm und CAS ist ein Valkey-Roundtrip lang. Ein harter Worker-Tod (SIGKILL, OOM) genau dort ist selten. Trifft er aber, scheitert der Workflow fast sicher: Die heilende Neuzustellung wartet auf den Running-Marker (`execution_guard_ttl_s`, Default 60 s), während der schon publizierte Nachfolger sofort läuft. Ohne Absturz führen ein Valkey-Aussetzer am CAS oder 25 verlorene CAS-Runden in dasselbe Fenster. Die Neuzustellung kommt dann sofort, das Rennen ist offen.
3. *Quelle der Erwartung:* vorhanden (Docstrings, KTD11), aber von KD1 überstimmt.

### Neues Argument für „Entscheidung überdenken?“

KD1 hat nur zwei Möglichkeiten abgewogen: eine still gekürzte Liste oder lautes Scheitern. Für Blätter gibt es eine dritte. Ihre Kennung ist seit 0.1.2 deterministisch (`workflow_step_task_id`, Commit 4f8820d, auch in `Client.apply`). Der Fan-in läuft erst, wenn `chord_complete.lua` N verschiedene abgeschlossene Glieder gezählt hat; jedes Glied hat dann also schon ein Ergebnis. Der Marker sagt dem Fan-in nichts, was die Zählung nicht schon belegt. Bei abgeleiteter Kennung entfiele das Scheitern, und eine gekürzte Liste bliebe trotzdem unmöglich. Fehlt ein Ergebnis wirklich, meldet das `_gather_results` als `WorkflowResultExpired`. Dazu kommt: Mit E-040 und KTD11 (Neuzustellung bei vorübergehendem Fehler, `WorkflowStateConflict`) ist das Fenster seit 0.10 auch ohne Absturz erreichbar, und das Heilen über die Neuzustellung kommt immer zu spät, weil der Workflow schon `failed` ist. Bei KD1 war der Marker „verloren“; hier ist er nur verspätet.

### Vorschlag für die Register-Zeile (Abschnitt ENTSCHEIDUNG)

| Verdacht | Entscheidung (Zitat) | Trotzdem nötige Doku-Korrektur | Neues Argument |
|---|---|---|---|
| R4Z-01: Ein Gruppenglied, dessen Schritt-Marker der publizierende Advance noch nicht persistiert hat (Absturz, Valkey-Aussetzer oder 25 verlorene CAS-Runden zwischen Publish und Persistenz), scheitert beim Fan-in mit `WorkflowValidationError`, obwohl kein Schritt scheiterte. | Plan 2026-08-30-002 KD1: „Der Preis ist ausdrücklich mitentschieden … etwa einen im publish→mark-Fenster verlorenen Schritt-Marker“; Changelog 0.4.0; `test_guard_catches_lost_marker_of_composite_member` | Docstring `_refresh_step_tasks` (und KTD11) versprechen Schutz im Fenster; er greift nur, wenn die Persistenz vor dem Fan-in liegt. `workflows.md` „Failure modes“ nennt den Fall nicht. Fix-Art: Doku (sofort), ein Register-Eintrag (KD1 als E-nnn, bisher nur im Plan) | Für Blätter ist die Kennung deterministisch, und die Fan-in-Zählung belegt die Vollständigkeit. Die Ableitung (`self._make_task_id(wf_id, path)` in `_terminal_result_id`) schließt die gekürzte Liste ebenso aus und vermeidet das Scheitern. Fix-Art bei Umentscheidung: Code |

Folge für die Nachweise: bis zur Umentscheidung Beleg statt xfail (das heutige `failed` festschreiben) oder entfernen. Die Ausklammerung `deliverable` in der Maschine bleibt.

---

## R4Z-02: Dead-Letter nach gespeichertem, aber nicht weitergeschaltetem Erfolg lässt den Workflow auf `running`

**Einstufung: FEHLER, P3**

### Zitierte Zusage

- **E-040, Entscheidung:** „Endet ein Workflow-Schritt endgültig ohne Erfolg (DLQ, …) markiert der Worker den Workflow synchron … Ein Workflow läuft nie still in den `get()`-Timeout.“ Begründung: „Ohne synchrones Fail-Mark wartet ein Aufrufer bis zum Timeout und ein Chord-Body läuft nie.“
- `website/guides/workflows.md:232`: „A step fails terminally (… delivery limit …) | The worker marks the workflow `failed` when it dead-letters the step“.

### Entscheidungs-Abgleich und Widerlegungsversuche

1. **E-040, Bewusste Folgen:** „Ein Schritt, der schon erfolgreich war, markiert seinen Workflow bei einem späteren Dead-Letter nicht als gescheitert.“ Ebenso `workflows.md:232`: „A late duplicate of a step that already succeeded does not mark the workflow failed“. Das ist der stärkste Widerlegungsgrund. **Warum er nicht greift:** Die Folge verbietet das Fail-Mark. Sie erlaubt nicht, dass der Workflow hängen bleibt, und sie hat einen ausdrücklichen Zweck. Herkunft ist Commit a570112 (BZ-13, MR 3); der Docstring von `tests/unit/worker/test_terminal.py:1-6` und der Kommentar in `terminal.py:57-60` nennen ihn: „Spaetes Duplikat eines erledigten Schritts …: **der Workflow ist weitergelaufen** und darf nicht kippen.“ In R4Z-02 ist der Workflow nicht weitergelaufen: Der Advance ist gescheitert, die DLQ-Zustellung ist die letzte, die ihn hätte weiterschalten können. Ein Fix kann die Bewusste Folge wahren, indem er im Schutzfall den Advance mit dem gespeicherten Ergebnis wiederholt, statt den Workflow fehlzumarkieren. Dass die Autoren die Lücke kennen, zeigt `worker/results.py:250-252`: Im `UnroutableMessage`-Zweig umgehen sie den Schutz bewusst, „dessen Schutz ‚Schritt schon erledigt‘ griffe hier immer, denn das Ergebnis dieses Schritts ist bereits gespeichert“. Für die übrigen Advance-Fehler mit Neuzustellung fehlt diese Behandlung.
2. **E-040, Bewusste Folgen, erster Satz** (vorübergehendes Fail-Mark → Neuzustellung) und KTD3 („transient scheiterndes Fail-Mark wird redelivered“): betrifft ein scheiterndes Fail-Mark, nicht einen gespeicherten Erfolg mit gescheitertem Advance. Greift nicht.
3. **E-031** (DLQ hinterlässt immer ein Fehlerergebnis, auch am Delivery-Limit): Der zweite Weg (Zustelllimit nach wiederholt scheiterndem Advance) überschreibt deshalb das Erfolgsergebnis (`retry.py:302`). E-031 entscheidet das Fehlerergebnis eines *terminal gescheiterten* Tasks, nicht das Überschreiben eines Erfolgs. Das Hängen erklärt es nicht.
4. **Test mit `entscheidung`-Marker:** `test_terminal.py:92` `test_step_without_a_success_result_fails_its_workflow` (E-040) widerspricht nicht, denn der Nachweis verlangt kein Fail-Mark bei gespeichertem Erfolg. `test_step_that_already_succeeded_does_not_fail_its_workflow` trägt keinen Marker und bliebe auch bei einem Fix über Neu-Advance grün.
5. **Realismus:** Der echte Pfad ist nachvollzogen. `ResultTail` speichert das Ergebnis vor dem Advance. Ein `BackendConnectionError`, `WorkflowStateConflict` oder `BrokerConnectionError` im Advance führt zu `redeliver_in_place` (`results.py`, AT_LEAST_ONCE-Zweige). Diese Zustellung löscht laut KTD5-Tabelle nur den Running-Marker, setzt keinen Done-Marker; die Neuzustellung führt den Task also erneut aus. Endet der zweite Lauf terminal (`NoRetry`, letzter Versuch des Budgets, Hard-Timeout bei `max_retries=0`), greift `_step_succeeded`. Zwei Bedingungen müssen zusammenkommen: eine vorübergehende Störung genau im Advance dieses Schritts und ein terminales Scheitern des zweiten Laufs, etwa bei einem nicht idempotenten Task („schon verarbeitet“ → `NoRetry`). Der zweite Weg braucht `delivery_limit − 1` gescheiterte Advances in Folge und ist noch seltener. Folge: Stillstand bis zur TTL (Liveness, kein Datenverlust). Deshalb P3, nicht P2.

### Vorschlag für die Register-Zeile (P3)

| Befund | Verletzte Zusage + Quelle | Fix | Verdacht |
|---|---|---|---|
| Ist das Erfolgsergebnis eines Workflow-Schritts gespeichert, aber der Advance vorübergehend gescheitert (Neuzustellung), und endet die Neuzustellung im Dead-Letter (erneuter Lauf scheitert terminal, oder das Zustelllimit greift nach wiederholt scheiterndem Advance), dann markiert `fail_workflow_step` nicht (`_step_succeeded`), und nichts schaltet den Workflow mehr weiter: `running` bis zur TTL. Auf dem Zustelllimit-Weg überschreibt zusätzlich das Fehlerergebnis den Erfolg. | E-040 („Ein Workflow läuft nie still in den `get()`-Timeout“); Zweck der Bewussten Folge laut BZ-13/a570112 nur „weiterlaufender Workflow“; `workflows.md:232` | Code: Im Schutzfall von `fail_workflow_step` den Advance mit dem gespeicherten Ergebnis wiederholen; scheitert er erneut, wie das Fail-Mark neu zustellen. Am Zustelllimit braucht das einen Nachtrag zu E-040: weiterschalten oder fehlmarkieren, wenn der Advance auch dann scheitert. Register: Nachtrag zu E-040, dass die Folge nur für bereits weitergeschaltete Schritte gilt | R4Z-02 |

---

## R4Z-03: Zwei überlappende Advances publizieren den Chord-Body zweimal

**Einstufung: NICHT HALTBAR**

### Widerlegungsversuche

1. **Realismus des Auslösers (greift).** Der Nachweis (`overlapping_advancers_publish_the_body_once`) stellt dieselbe Nachricht `/h/g2` zweimal *gleichzeitig* zu. Im echten Worker läuft der Ausführungsschutz vor der Ausführung, und sein Running-Marker hält von der Ausführung bis zum Ack, also auch über Ergebnis-Tail und Advance (Plan 2026-10-08-1541 KTD5). Eine zweite Zustellung derselben Identität wird zurückgestellt: `worker/gates/execution_guard.py:152-153` (`if claim.status == "running": return await self._defer(...)`), `delivery-semantics.md:391-395` („**Running elsewhere:** the delivery goes back through the delay queue“). Die Workflow-Maschine bildet vom Ausführungsschutz nur den Done-Marker nach (`workflow_machine.py:23-24`, `:750`), nicht den Running-Marker. Das Szenario umgeht damit einen Mechanismus, der im Default aktiv ist.
2. **Die zweite Form** (Neuzustellung eines *früheren* Mitglieds parallel zum letzten): Hat das frühere Mitglied schon bestätigt oder ist sein Ack verloren gegangen, setzt `GuardedBroker.ack` den Done-Marker, und die Kopie wird verworfen (E-043, K10 der Retry-Maschine). Doppelt publiziert wird nur, wenn sein Advance genau im publish→persist-Fenster des letzten Mitglieds erneut läuft, nach Absturz und Ablauf des Running-Markers. Das ist das Crash-Fenster, das bewusst entschieden ist:
   - Commit 4f8820d: „chord_complete.lua no longer holds a one-shot SET NX flag … the body can never be LOST in the completion→publish crash window (**at worst a rare idempotent double-enqueue**)“;
   - `tests/integration/test_chord_lua_scripts.py:1-9` (gleicher Wortlaut);
   - Plan 2026-05-30-001 `:103` und Risiko-Tabelle `:465` („Doppel-Enqueue des Nachfolgers … Von Schritt-Idempotenz (AT_LEAST_ONCE) absorbiert — bewusst die sichere Richtung“);
   - `_enqueue_node`-Docstring.
3. **Restfälle:** Mit `SWARMQ_EXECUTION_GUARD=false` oder nach einem blockierten Loop (Running-Marker abgelaufen, E-046 und Restrisiko in KTD5: „Hängt der Event-Loop, bleibt die Verlängerung aus“) ist auch die Überlappung derselben Nachricht möglich. Dann gilt at-least-once ausdrücklich (E-043, Bewusste Folgen: „Mit `SWARMQ_EXECUTION_GUARD=false` kann die Neuzustellung erneut verarbeitet werden“; `workflows.md:300`: „steps must be idempotent under at-least-once delivery“).
4. **Zusage „exactly once“** (`workflows.md:25`, `:162`): Im Default hält sie für die Ausführung. Die Body-Kopie hat dieselbe deterministische Kennung und wird vom Ausführungsschutz verworfen, so belegt es der Ablauf `two_advancers_on_the_last_member`.
5. **Plan U8** („ein CAS gewinnt, Callback einmal“) ist ein Testszenario, keine Produktzusage.

Ergebnis: Unter dem echten Ausführungsschutz ist das Szenario nicht erreichbar. Was übrig bleibt, ist entschieden. Eine LÜCKE ist es nicht, weil das Doppel-Enqueue bewusst festgelegt ist (Commit 4f8820d, Plan 2026-05-30-001).

### Vorschlag

- Kein Befund. Register: kein Eintrag nötig. Optional eine Regel, die das Doppel-Enqueue aus dem Plan sichtbar macht (Fix-Art Register): „Nachfolger und Chord-Body werden at-least-once publiziert; einmal ausgeführt werden sie über den Ausführungsschutz (E-009). Ohne Ausführungsschutz oder nach abgelaufenem Running-Marker kann ein Body doppelt laufen.“
- Tests: den xfail-Nachweis `test_overlapping_advancers_publish_the_body_once` entfernen; `two_advancers_on_the_last_member` bleibt der Beleg. Für eine spätere Runde: Die Maschine sollte den Running-Marker nachbilden oder `concurrent_advance` für dieselbe Nachricht auf den Fall „Ausführungsschutz aus“ beschränken.

---

## R4Z-04: Am Zustelllimit wird eine erledigte oder überholte Ausführung dead-gelettert statt als Duplikat bestätigt

**Einstufung: FEHLER, P3**

### Zitierte Zusagen

- **E-009:** „Eine normale Neuzustellung derselben Ausführung (gleiche Identität) wird weiter als Duplikat verworfen.“
- **E-043:** „Die Neuzustellung des Originals überspringt der Ausführungsschutz als `done`.“
- `website/explanation/delivery-semantics.md:380-390`: „…sets its marker as after success; the redelivered original is then skipped“ und „**Done, or superseded by a retry with a higher count:** the delivery is acknowledged and skipped (`duplicate_delivery_skipped`).“
- Plan `2026-10-08-1541-fix-einbindungsfehler-plan.md` KTD5, Konflikte: „Done-Marker oder kleinerer Retry-Zähler als der Superseded-Marker → bestätigen und überspringen“.

### Entscheidungs-Abgleich und Widerlegungsversuche

1. **Gate-Reihenfolge entschieden?** E-070 legt nur Ausführungsschutz → Expiry → Rate-Limit → Lock fest. Das Delivery-Count-Gate steht nur in KTD5 („die Erkennung hängender Worker (Delivery-Count-Gate) bleibt davor“). Begründet ist das mit hängenden Workern, nicht mit erledigten Ausführungen. Keine Regel nimmt die Folge am Limit hin. Greift nicht.
2. **`delivery-semantics.md:642-645` (Route 3):** „The message was delivered `limit - 1` times without an ack landing … This is the pathological-loop guard“. Das trifft wörtlich zu, denn die Acks gingen verloren. Die Route beschreibt aber den Schutz vor einem fehlerhaften Worker. Ein Fehlerergebnis über einem gespeicherten Erfolg, eine Unique-Freigabe während die Retry-Kopie läuft, und ein Fail-Fast des Workflows bei weiterlaufender Kette widersprechen den Zusagen oben, und keine Doku nimmt sie hin. Der Widerspruch zweier Zusagen wird hier zu Lasten von Daten (Ergebnis) aufgelöst. Greift nicht.
3. **E-031** (Fehlerergebnis am Delivery-Limit): Es gilt für einen Task, der „terminal über die DLQ endet“. Hier endete der Task erfolgreich (bzw. lebt als Retry-Kopie weiter). Greift nicht.
4. **E-054 / E-046 Nachtrag F3-27:** behandeln Broker-Zähler auf Kopien und die Absturzschleife, nicht diesen Fall. Greift nicht.
5. **Realismus (mindert die Priorität):** Der Mechanismus ist echt (`pipeline.py` `_pre_pipeline_gates` vor der Gate-Schleife; `gates/delivery_count.py` → `RetryHandler.dead_letter` → `store_result`). In beiden Stufen ist er deterministisch nachgestellt, im Valkey-Lauf mit `delivery_limit_reached_dead_letter`, `task_sent_to_dlq`. Für den Auslöser muss dieselbe Nachricht das Limit erreichen, und die Acks auf die Duplikate müssen weiter scheitern. Mit Default 20 sind das rund 19 Rückgaben derselben Nachricht, etwa bei einem längeren Broker-Vorfall mit wiederholtem Kanalverlust oder bei `nack`-Schleifen auf 4.3 (BZ-29, `x-acquired-count`) nach gescheiterten Kopien (E-041). Mit kleinem `SWARMQ_DELIVERY_LIMIT` oder `x-delivery-limit` genügen ein bis drei. Im Normalbetrieb kommt das nicht vor, deshalb P3. Die Wirkung ist aber nicht kosmetisch: Ein Erfolg wird zum Fehler, und ein späteres Replay der DLQ-Kopie (neue Identität, E-009) führt einen erledigten Task erneut aus.

### Vorschlag für die Register-Zeile (P3)

| Befund | Verletzte Zusage + Quelle | Fix | Verdacht |
|---|---|---|---|
| Erreicht die Neuzustellung einer erledigten (Done-Marker) oder überholten (Superseded-Marker) Ausführung das Zustelllimit, schickt das Delivery-Count-Gate sie als `WorkerHungError` in die DLQ, bevor der Ausführungsschutz sie als Duplikat bestätigt. Folgen: Das Fehlerergebnis überschreibt den Erfolg; `TASK_DLQ` und die Unique-Freigabe laufen (bei der überholten Form, während die Retry-Kopie läuft); ein Workflow-Mitglied kippt bei der überholten Form seinen Workflow per Fail-Fast; ein Replay der DLQ-Kopie führt den Task erneut aus. Die Limit-DLQ setzt keinen Done-Marker. | E-009, E-043, `delivery-semantics.md:380-390`, Plan 2026-10-08-1541 KTD5 (Konflikte) | Code: Das Delivery-Count-Gate liest am Limit vor dem Dead-Letter den Zustand des Ausführungsschutzes; `done`/`superseded` → bestätigen (`duplicate_delivery_skipped`), sonst DLQ wie heute. Doku: Route 3 in `delivery-semantics.md` um diesen Vorrang ergänzen | R4Z-04 |

Nach dem Fix: die Ausklammerung in `RetryChainMachine.deliverable` und den K7-Ausschluss entfernen (Folgeaufgabe aus `1-zustandsmodelle.md`).

---

## R4Z-05: Per UI neu publizierte Retry-Kopie mit Text-Zähler wird als überholt bestätigt

**Einstufung: ENTSCHEIDUNG**

### Zitierte Entscheidung

- **E-051:** „Der Retry-Zähler `x-swarmq-retry-count` wird streng gelesen: Nur ein echtes `int` größer 0 zählt; `bool`, Zeichenketten (auch `"3"`) … zählen wie 0.“ Begründung: „Streng gelesen ist der Retry-Zähler gleich wie im Ausführungsschutz (`retry_count_of`)“. Der Ausführungsschutz ist hier also ausdrücklich mitgemeint.
- **KTD5 (Plan 2026-10-08-1541), Konflikte:** „kleinerer Retry-Zähler als der Superseded-Marker → bestätigen und überspringen (`duplicate_delivery_skipped`)“; `delivery-semantics.md:389-390`.
- **E-009:** gleiche Identität (keine Replay-ID) → Duplikat.
- Tests mit `entscheidung("E-051")` in `tests/unit/worker/test_befund_fs19_counter_headers.py`, keiner widerspricht.

### Widerlegungsversuche

1. *Quelle der Erwartung:* Die einzige Quelle für „die Kopie läuft“ ist ein Testszenario in Plan U9 („Kette beginnt neu“). Das ist keine Zusage an Nutzer. Ohne Zusage wäre es höchstens eine LÜCKE. Das Verhalten folgt aber zwingend aus zwei entschiedenen Regeln, also ENTSCHEIDUNG.
2. *„still“?* Nicht ganz: Der Ausführungsschutz loggt `duplicate_delivery_skipped` mit `reason=superseded`.
3. *Realismus:* sehr gering. Ein Operator müsste eine Retry-Kopie, die nicht aus der DLQ kommt, über „Publish message“ der UI mit Text-Headern neu einspielen *und* das Original aus der Delay-Queue entfernt haben. Liegt das Original noch dort, läuft die Kette über das Original weiter, und das Verwerfen der UI-Kopie ist richtig (echtes Duplikat). Ein Replay aus der DLQ hat eine neue Identität und läuft (E-051, Bewusste Folgen; Ablauf `ui_replay_with_string_count_gets_the_full_budget`).

### Vorschlag

- Kein Befund. Register: Nachtrag zu E-051 (Fix-Art Register, optional Doku-Satz in `dead-letter-queues.md`): „Eine per UI mit Text-Zähler neu publizierte Retry-Kopie ohne neue Replay-ID zählt 0 und gilt gegen den Superseded-Marker als überholt: kein Lauf, kein Ergebnis, keine DLQ-Kopie (`duplicate_delivery_skipped`, `reason=superseded`). Wiedereinspielen nur über `swarmq dlq retry`.“ Dazu der Nachweis als festschreibender Test mit `@pytest.mark.entscheidung("E-051")`, ohne xfail, mit der umgekehrten Assertion.
- Neues Argument für ein Überdenken: keins. Der Fall ist bei den Bewussten Folgen von E-051 nur nicht aufgeführt.

---

## Lock- und Leader-Maschine (U6, U7): Invarianten, die entschiedenes Verhalten zu schwach prüfen

- **Lock-Maschine (U6):** Die Ausnahme `excused` für E-022 ist zeitbasiert und endgültig: Ein Handler ohne Takt für ein Fenster gilt fortan als entschuldigt. Der Takt-Pfad verlangt dann nur `excused`, nicht das entschiedene Signal `task_lock_lost` (`lock_machine.py:369-371`, `continue` ohne Log-Prüfung; E-022 schreibt das Event vor). Ein Keeper, der eine Lease still verliert, fiele deshalb nur in den deterministischen Abläufen auf, nicht in der Zufallssuche. Die einmalige Warnung `task_lock_held_past_hard_timeout` (E-022) prüft keine Invariante.
- **Leader-Maschine (U7):** L4 prüft E-014 nur als Erlaubnis (Rückhalt nur ohne lebende Meldung, `leader_machine.py:855-856`), nicht als Pflicht. Ein Leader, der einen verwaisten statischen Cron weiter publiziert statt ihn zurückzuhalten, ginge als at-least-once durch. Gegen Valkey ist der Rückhalt wegen der Serverzeit in `sched_report.lua` gar nicht erreichbar. L5 prüft für E-048 nur die Obergrenze; das genügt, weil E-048 den frühen Rücktritt ausdrücklich hinnimmt.

## Abgleich mit KTD3

Die erlaubten Abweichungen in KTD3 und in den Docstrings der Maschinen decken keinen der Verdachte ab. R4Z-01 stützt sich auf eine Entscheidung, die nur im Plan steht (2026-08-30-002 KD1), nicht im Register und nicht in KTD3. Die Docstring-Ausnahme „Doppel-Publish im Publish-vor-Persistenz-Fenster“ der Workflow-Maschine ist durch Commit 4f8820d und Plan 2026-05-30-001 gedeckt; in der Tabelle in `1-zustandsmodelle.md` steht bei ihr „keine E-ID“, die Quelle lässt sich jetzt nachtragen.
