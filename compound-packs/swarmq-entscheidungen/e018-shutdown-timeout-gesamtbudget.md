---
title: SWARMQ_SHUTDOWN_TIMEOUT ist das Gesamtbudget ab SIGTERM; der Drain endet vorher mit Reserve für das Aufräumen
applies_when:
  - ein Worker wird per SIGTERM beendet (Deploy, Skalierung, K8s/Docker-Stop)
  - Änderungen an shutdown_budget.py, worker/shutdown.py, supervisor._shutdown_all_workers oder ProcessManager.shutdown_grace_s
  - eine Fehlersuche meldet, dass ein Task beim Shutdown früher abgebrochen wird als shutdown_timeout
  - eine Fehlersuche meldet, dass ein Recycling-Drain das Shutdown-Budget nicht einhält
tags: [shutdown, drain, deploy, kubernetes, bz-18]
id: E-018
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:134 (Entscheidungen, BZ-18)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:51 (Abschnitt 1, BZ-18 Gesamtbudget ab SIGTERM)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:597 (MR 17, BZ-18)
tests:
  - tests/unit/test_shutdown_budget.py::test_supervised_default_budget
  - tests/unit/process_manager/test_befund_bz18_shutdown_budget.py::test_default_shutdown_fits_into_kubernetes_termination_grace
  - tests/unit/worker/test_shutdown_budget_steps.py::test_supervised_drain_ends_before_the_supervisor_kills
  - tests/unit/worker/test_shutdown_budget_steps.py::test_recycling_drain_is_exempt_from_the_budget
  - tests/unit/worker/test_shutdown_budget_steps.py::test_hanging_broker_close_is_bounded_and_the_backend_still_closes
  - tests/unit/worker/test_befund_bz18_cleanup_after_grace.py::test_locks_and_leader_are_released_within_the_grace_period
  - tests/unit/worker/test_recycling_sigterm.py::test_sigterm_cuts_the_recycling_drain_with_the_budget_from_the_signal
---

**Entscheidung.** `SWARMQ_SHUTDOWN_TIMEOUT` (und `ProcessManager.shutdown_grace_s`) ist das Gesamtbudget ab SIGTERM. Die Eskalation des Supervisors endet bei `total − 2 s` (SIGKILL), der Drain läuft höchstens bis `finish_by − 5 s`, damit Lock-Freigabe, Leader-Übergabe und Guard-Abschluss noch Zeit haben; das gilt auch eingebettet. Jeder Aufräumschritt hat ein eigenes Zeitlimit. Der Recycling-Drain ist ausgenommen. Die Doku nennt `terminationGracePeriodSeconds` bzw. Docker `stop_grace_period` ≥ `shutdown_timeout + 15`.

**Begründung.** Vorher dauerte der Shutdown länger als die 30 s (K8s) bzw. 10 s (Docker); SIGKILL kam vor Lock-Freigabe und Leader-Übergabe (BZ-18).

**Bewusste Folgen.** Ein laufender Task bekommt beim Shutdown weniger als `shutdown_timeout` Drain-Zeit; ein abgebrochener Task folgt dem Fehlerpfad (E-001). Beim Recycling gilt das Budget nicht.

**Nachtrag 2026-10-14 (FS-02b, U3).** Dauert ein Recycling-Drain länger als `drain_until`, wird das Aufräumbudget ab dem tatsächlichen Drain-Ende neu angesetzt: Jeder Aufräumschritt bekommt danach die Reserve `finish_by − drain_until` (unter dem Supervisor mit `finish_by = sigkill_at`), nie weniger als das ursprüngliche Ende. Ohne Recycling bleibt das Ende bei `finish_by`. Bis U9 (FS-30) setzte ein SIGTERM während eines laufenden Recycling-Drains das Budget nicht in Kraft (behoben, Nachtrag FS-30).


**Nachtrag 2026-10-14 (FS-30, U9): SIGTERM im Recycling-Drain.** Das erste SIGTERM während eines Recycling-Drains setzt das Budget ab diesem Signal in Kraft: Drain-Ende und Publish-Endfrist werden auf `min(bisher, SIGTERM + drain_until)` gezogen, nie später; das Aufräumen endet spätestens bei `SIGTERM + finish_by` (unter dem Supervisor `sigkill_at`). Danach gilt für abgebrochene Tasks E-001. Ein zweites SIGTERM (Eskalation des Supervisors) setzt keinen neuen Anker, sonst läge das Drain-Ende hinter dessen SIGKILL. Die Recycling-Anfrage selbst unterbricht den Drain nicht; kommt das SIGTERM schon, während der Worker auf die Bestätigung des Supervisors wartet, entfällt der Recycling-Drain und es gilt das Budget.

**Überdenken nur mit neuem Argument.** Offen (Gegenprüfung 2026-10-13): `process_manager/restart_budget.py:57` und `process_manager/setpoint.py:210` rechnen noch `shutdown_grace_s + SHUTDOWN_TERM_S`, also mit der alten Bedeutung „Drain-Zeit“; das ändert die Eskalation nicht, überschreitet aber das Gesamtbudget bei Heartbeat-Frist und Status-TTL.
