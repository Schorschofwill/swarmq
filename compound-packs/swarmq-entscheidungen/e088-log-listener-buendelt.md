---
title: Der eingebaute Log-Listener sammelt Zeilen unter WARNING bis zu log_batch_delay (Default 50 ms); WARNING, 10 % Füllstand und jedes Leeren beenden die Pause sofort; Datei-Modus und Opt-in-Queue bündeln nicht
applies_when:
  - Änderungen an swarmq.logging_batch, an _SwarmqQueueListener.dequeue, _StructlogQueueHandler.enqueue oder am Leeren der Log-Queues
  - Änderungen an log_batch_delay (--log-batch-delay, SWARMQ_LOG_BATCH_DELAY) oder an dessen Default
  - eine Fehlersuche meldet, dass INFO-Zeilen verzögert erscheinen oder nach einem SIGKILL die letzten INFO-Zeilen fehlen
  - eine Fehlersuche meldet, dass die Opt-in-Queue oder ein dictConfig-Listener nicht bündelt
tags: [logging, performance, listener]
id: E-088
datum: 2026-10-27
status: gilt
quelle:
  - "Georg 2026-10-27: Bündeln statt Abschalten der Erfolgszeile; Plan freigegeben, Messung auf celsius freigegeben"
  - docs/plans/2026-10-27-0800-perf-log-listener-buendeln-plan.md (R1–R9, KTD1–KTD5)
  - docs/befunde/log-listener-buendeln-messung-2026-10.md
tests:
  - tests/unit/test_logging_batch.py::test_info_lines_wait_for_the_pause_without_waking_the_listener
  - tests/unit/test_logging_batch.py::test_a_warning_ends_the_pause_and_the_lines_before_it_follow
  - tests/unit/test_logging_batch.py::test_a_queue_filling_to_ten_percent_ends_the_pause
  - tests/unit/test_logging_batch.py::test_shutdown_ends_the_pause_and_writes_every_line
  - tests/unit/test_logging_batch.py::test_stop_ends_the_pause_and_writes_every_line
  - tests/unit/test_logging_batch.py::test_a_wake_before_the_pause_ends_it_at_once
  - tests/unit/test_logging_configure.py::test_file_mode_listener_does_not_batch
  - tests/unit/test_logging_optin.py::test_the_opt_in_queue_has_no_batch_delay
  - tests/unit/test_logging_app_wins.py::test_the_opt_in_queue_after_the_takeover_has_no_batch_delay
---

**Entscheidung.**
- Der Listener des eingebauten Loggings (Worker, Worker-Unterprozesse, Compute-Kinder) wartet nach leerer Queue bis zu `log_batch_delay` Sekunden und gibt dann alle bis dahin eingereihten Zeilen gesammelt aus. Default 0,05 s, erlaubt 0 bis 1 s (E-063); 0 gibt jede Zeile sofort aus wie bisher.
- Die Pause endet sofort bei einer Zeile ab WARNING, wenn die Queue 10 % ihrer Kapazität erreicht, und bei jedem Leeren (`shutdown_logging`, `stop`, Leeren mit Frist). Die Frist `log_drain_timeout` steht vollständig für das Ausgeben zur Verfügung.
- Der Datei-Modus (`--log-config`) und die Opt-in-Queue am Logger `swarmq` bündeln nicht, auch nicht nach einer Übernahme durch die App.

**Begründung.** Der Listener wachte für jede Zeile auf und nahm der Event-Loop dabei den GIL. Bei kurzen Tasks kostete allein `task_succeeded` rund 13 % Durchsatz. Abschalten kam nicht in Frage, weil die Erfolgszeile verlangt ist (Logging-Plan 2026-10-06-0831, R16). Gebündelt bringt sie auf celsius +10,8 % Durchsatz bei `io_bound` (A/B, 10 Paare, p = 0,0002), ohne dass eine Zeile fehlt. Datei-Modus und Opt-in-Queue schreiben in Handler der App; gebündelt erschienen swarmq-Zeilen dort nach später geloggten App-Zeilen.

**Bewusste Folgen.**
- INFO- und DEBUG-Zeilen erscheinen bis zu `log_batch_delay` später. Der Zeitstempel bleibt der des Logaufrufs.
- Bei hartem Abbruch ohne Leeren (SIGKILL, Segfault, OOM-Kill, `os._exit`) können bis zu einer Pause INFO- und DEBUG-Zeilen verloren gehen. WARNING und höher sind nicht betroffen.
- Zusätzlich verworfen werden kann eine Zeile nur bei einem Schub, der die Queue um mehr als 80 % ihrer Kapazität füllt, bevor der Listener drankommt.

**Überdenken nur mit neuem Argument.** Wer die heutige Verzögerung nicht will, setzt `--log-batch-delay 0`.
