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
  - Loadsuite-Abnahme 0.13.0rc1, swarmq-loadsuite-stoerungen/docs/testlauf-v0.13.0rc1.md (Backend-Smoke, Kette rund 29–30 s bei abgewiesener Verbindung)
tests:
  - tests/unit/worker/test_results.py::test_ack_follows_the_stored_result
  - tests/unit/worker/test_result_store_retry.py::test_store_succeeds_on_the_third_attempt_and_acks
  - tests/unit/worker/test_result_store_retry.py::test_five_failed_attempts_redeliver_after_the_nominal_15_seconds
  - tests/unit/worker/test_result_store_retry.py::test_wall_clock_stays_bounded_when_every_attempt_runs_into_the_op_timeout
  - tests/unit/worker/test_result_store_retry.py::test_wall_clock_when_valkey_refuses_every_connection_at_once
  - tests/unit/worker/test_result_store_retry.py::test_consumer_timeout_caps_the_retry_before_the_deadline
  - tests/unit/worker/test_result_store_retry.py::test_drain_cancel_while_waiting_redelivers_without_propagating
  - tests/unit/worker/test_result_store_retry.py::test_drain_cancel_during_a_repeated_store_attempt_redelivers_without_propagating
  - tests/unit/worker/test_result_store_retry.py::test_drain_cancel_in_the_worker_leaves_no_completed_marker_and_no_unique_release
  - tests/unit/worker/test_result_store_retry.py::test_error_result_without_dead_letter_is_stored_on_the_second_attempt_and_acked
  - tests/unit/worker/test_result_store_retry.py::test_cancelled_result_is_stored_on_the_second_attempt_and_acked
  - tests/unit/worker/test_result_store_retry.py::test_fire_and_forget_stores_once_without_retry
  - tests/chaos/test_backend_restart.py::TestBackendRestart::test_task_finishing_during_a_five_second_valkey_outage_runs_once
  - tests/e2e/combinations/test_failure_cascades.py::TestBackendDownResultStoreCombo::test_backend_drop_during_result_store_is_bridged_without_a_second_run
  - tests/e2e/combinations/test_failure_cascades.py::TestBackendDownResultStoreCombo::test_backend_drop_longer_than_the_store_retry_redelivers_without_task_failed
---

**Entscheidung.** Ack nach Ergebnis-Speichern: Ein Worker bestätigt eine `AT_LEAST_ONCE`-Nachricht erst, wenn ihr terminales Ergebnis gespeichert ist. Scheitert das Speichern auch nach dem Wiederholfenster des Valkey-Clients (rund 3,1 s, nach dem Fix von F4-14 auch hinter einem Proxy), wiederholt `ResultTail.finish` das Speichern selbst, begrenzt: Backoff wie bei der Zurückstellung (`SWARMQ_BACKEND_DEFER_BASE_DELAY`, 1 s, verdoppelnd), Obergrenze rund 10–15 s (genaue Zahl im Fix-Plan), gedeckelt durch das Drain-Ende (E-018, E-001 Nachtrag) und `SWARMQ_CONSUMER_TIMEOUT_S`. Der Slot bleibt währenddessen belegt. Erst wenn auch diese Wiederholung scheitert, geht die Nachricht per `redeliver_in_place` zurück, und der Task läuft erneut.

**Begründung.** Ohne eigene Wiederholung läuft bei jedem Valkey-Ausfall über ~3 s jeder gerade fertige Task vollständig erneut, samt Nebenwirkung, obwohl nur die Ablage scheiterte (Störungsläufe B26: 5 Duplikate ohne Persistenz; B23: 40 Duplikate bei 30 s). Valkey-Neustarts, Failover und Sidecar-Neustarts bleiben typischerweise unter 15 s. Die Wiederholung ist mit den Pipeline-Invarianten vereinbar (`invarianten-der-worker-pipeline-2026-08.md`: Ack-Ordering, Terminal-Outcome und publish-then-mark unberührt); `SET … PX` ist idempotent, F3-33 wird nicht verschärft. Die Kopie mit dem Ergebnis zurückzustellen (Option c) ist verworfen (eigener Pfad, Größenfrage).

**Bewusste Folgen.** Ein Slot bleibt nach einem Speicherfehler bis zur Obergrenze belegt; der Konsum des Workers sinkt in dieser Zeit. Umgesetzt (0.13.0, Plan 2026-10-17-2103, U13): bis zu fünf Speicherversuche je terminalem Ergebnis (Erfolg, Fehler ohne Dead-Letter, Abbruch), Wartezeiten nach `backend_defer_delay` nominell 1, 2, 4, 8 s (Summe 15 s, Jitter 0,8 bis 1,2). Wanduhr-Obergrenze nach dem ersten Fehlschlag: vier weitere Versuche zu je höchstens `SWARMQ_BACKEND_OP_TIMEOUT` (5 s) plus die Wartezeiten, nominell rund 35 s, mit Jitter höchstens 38 s (wenn jeder Versuch in die Frist läuft; bei sofort abgewiesener Verbindung kürzer, siehe Nachtrag 2026-10-18); dazu kommt wie bisher der erste Versuch und die Pause vor dem `redeliver`. Der nächste Versuch startet nur, wenn er samt `backend_op_timeout` vor `SWARMQ_CONSUMER_TIMEOUT_S` ab dem Zustellzeitpunkt endet. Ein Drain-Abbruch im Warten oder in einem wiederholten Versuch endet die Wiederholung ohne `CancelledError` mit `redeliver_in_place` (E-001); ein Abbruch im ersten Versuch propagiert wie bisher. FIRE_AND_FORGET speichert einmal (KD8). Events `result_store_retrying` (WARNING, je Worker höchstens eine je 10 s, sonst DEBUG) und `result_store_recovered` (INFO). Ein Ausfall über der Obergrenze (oder über dem Drain-Ende) führt weiter zu einer zweiten Ausführung; das ist at-least-once und kein Fehler (`delivery-semantics.md` „It does not promise: That the task runs exactly once“). Die Doku D-2 ist mit U20 (0.13.0) erledigt: `delivery-semantics.md` „Redelivery scenarios“ hat den Abschnitt „4. Result store fails after the task ran“, und „Duplicates appear in four situations“ nennt die vierte Situation (EN und DE). Festgeschrieben sind die Reihenfolge Speichern vor Ack und die begrenzte Wiederholung (Unit-Tests mit Fake-Uhr, Chaos-Test: Valkey hinter Toxiproxy 5 s weg, der Task zählt Ausführungen; Combo 20: kurzer Blackhole ohne zweiten Lauf, Blackhole über alle fünf Versuche mit Neuzustellung); `tests/unit/worker/test_results.py::test_backend_store_error_is_redelivered` (ohne Marker) läuft auf der Fake-Uhr.

**Nachtrag 2026-10-18 (Loadsuite-Abnahme 0.13.0rc1): Dauer der Kette hängt von der Art des Ausfalls ab.** Ein Speicherversuch dauert höchstens `SWARMQ_BACKEND_OP_TIMEOUT` (5 s), aber nur so lange, bis der Valkey-Client aufgibt. Weist Valkey die Verbindung sofort ab („Error 111 connecting“), endet ein Versuch nach dem Wiederholfenster des Clients beim Verbindungsaufbau: fünf TCP-Wiederholungen mit 0,1 + 0,2 + 0,4 + 0,8 + 1,6 s = 3,1 s (`backend/valkey/connection.py`, `_RETRY_BACKOFF_*`, festgeschrieben in `tests/unit/backend/test_valkey_handshake_retry.py::test_refused_tcp_is_retried_five_times_without_outer_retry`). Damit gilt mit den Vorgaben: (a) Verbindung sofort abgewiesen: nach dem ersten Fehlschlag 4 × 3,1 s + 15 s = nominell 27,4 s (mit Jitter 24,4–30,4 s), ab Beginn des ersten Versuchs 30,5 s (27,5–33,5 s). (b) Jeder Versuch läuft in die Frist (Valkey hängt, Blackhole): nach dem ersten Fehlschlag 4 × 5 s + 15 s = nominell 35 s (32–38 s), ab Beginn des ersten Versuchs 40 s (37–43 s). Gemessen im Backend-Smoke der Abnahme (30 s Ausfall, abgewiesene Verbindung): rund 29–30 s ab Ausfallbeginn; 10 von 39 Tasks scheiterten im fünften Versuch 0,1–1,7 s vor Ausfallende und liefen ein zweites Mal. Das entspricht dieser Regel und ist kein Befund. Die Upgrade note 5 im Changelog 0.13.0rc1 und die Doku nennen seitdem beide Fälle. Festgeschrieben in `tests/unit/worker/test_result_store_retry.py::test_wall_clock_when_valkey_refuses_every_connection_at_once` (Charakterisierung).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
