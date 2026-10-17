---
title: Der Worker bestätigt erst nach dem Speichern des Ergebnisses; scheitert das Speichern, wiederholt er es begrenzt selbst, bevor die Nachricht zurückgeht und der Task erneut läuft
applies_when:
  - Änderungen an ResultTail.finish (worker/results.py), an der Reihenfolge von Speichern, Advance und Ack oder an redeliver_in_place nach einem Speicherfehler
  - eine Fehlersuche meldet, dass ein erfolgreicher Task nach einem Valkey-Ausfall vollständig erneut läuft
  - eine Fehlersuche meldet, dass ein Slot nach einem Speicherfehler einige Sekunden belegt bleibt
  - ein Plan will das Ergebnis vor dem Speichern bestätigen oder eine Kopie mit dem Ergebnis zurückstellen
tags: [worker, ergebnis, ack, at-least-once, valkey, r4i-01b, pf-17]
id: E-072
datum: 2026-10-17
status: gilt
quelle:
  - Georg 2026-10-17 (Fehlersuche Runde 4, PF-17, Option b)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (ENTSCHEIDUNG R4I-01b, LÜCKE PF-17, Vor dem Fix Punkt 14)
  - website/explanation/architecture.md:216 (a failed result write requeues the message, and the task runs again)
  - website/explanation/delivery-semantics.md:67-91 (SwarmQ acks after the task's terminal state is durably stored)
  - src/swarmq/worker/results.py:313-316 (KTD11, Plan 2026-10-06-0831)
tests:
  - tests/unit/worker/test_results.py::test_ack_follows_the_stored_result
---

**Entscheidung.** Ack nach Ergebnis-Speichern: Ein Worker bestätigt eine `AT_LEAST_ONCE`-Nachricht erst, wenn ihr terminales Ergebnis gespeichert ist. Scheitert das Speichern auch nach dem Wiederholfenster des Valkey-Clients (rund 3,1 s, nach dem Fix von F4-14 auch hinter einem Proxy), wiederholt `ResultTail.finish` das Speichern selbst, begrenzt: Backoff wie bei der Zurückstellung (`SWARMQ_BACKEND_DEFER_BASE_DELAY`, 1 s, verdoppelnd), Obergrenze rund 10–15 s (genaue Zahl im Fix-Plan), gedeckelt durch das Drain-Ende (E-018, E-001 Nachtrag) und `SWARMQ_CONSUMER_TIMEOUT_S`. Der Slot bleibt währenddessen belegt. Erst wenn auch diese Wiederholung scheitert, geht die Nachricht per `redeliver_in_place` zurück, und der Task läuft erneut.

**Begründung.** Ohne eigene Wiederholung läuft bei jedem Valkey-Ausfall über ~3 s jeder gerade fertige Task vollständig erneut, samt Nebenwirkung, obwohl nur die Ablage scheiterte (Störungsläufe B26: 5 Duplikate ohne Persistenz; B23: 40 Duplikate bei 30 s). Valkey-Neustarts, Failover und Sidecar-Neustarts bleiben typischerweise unter 15 s. Die Wiederholung ist mit den Pipeline-Invarianten vereinbar (`invarianten-der-worker-pipeline-2026-08.md`: Ack-Ordering, Terminal-Outcome und publish-then-mark unberührt); `SET … PX` ist idempotent, F3-33 wird nicht verschärft. Die Kopie mit dem Ergebnis zurückzustellen (Option c) ist verworfen (eigener Pfad, Größenfrage).

**Bewusste Folgen.** Ein Slot bleibt nach einem Speicherfehler bis zur Obergrenze belegt; der Konsum des Workers sinkt in dieser Zeit. Ein Ausfall über der Obergrenze (oder über dem Drain-Ende) führt weiter zu einer zweiten Ausführung; das ist at-least-once und kein Fehler (`delivery-semantics.md` „It does not promise: That the task runs exactly once“). Aufgabe für den Fix-Plan (D-2): `delivery-semantics.md` „Redelivery scenarios“ um den Fall ergänzen und „Duplicates appear in three situations“ (`:233-234`) um die vierte Situation erweitern. Festgeschrieben ist heute nur die Reihenfolge Speichern vor Ack; der Test der begrenzten Wiederholung folgt mit dem Fix (PF-17, Chaos-Test: Valkey hinter Toxiproxy neu starten, der Task zählt Ausführungen). `tests/unit/worker/test_results.py::test_backend_store_error_is_redelivered` (ohne Marker) muss der Fix auf eine Fake-Uhr umstellen.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
