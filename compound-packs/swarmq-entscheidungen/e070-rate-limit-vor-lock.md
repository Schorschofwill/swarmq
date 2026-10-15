---
title: Das Rate-Limit-Gate läuft vor dem Lock-Gate; dass ein Lock-Konflikt dabei einen Rate-Limit-Slot verbraucht, ist Folge und bisher nicht anders entschieden
applies_when:
  - Änderungen an der Gate-Reihenfolge in worker/pipeline.py, an gates/rate_limit.py oder gates/lock.py
  - eine Fehlersuche meldet, dass eine Lock-Requeue einen Rate-Limit-Slot verbraucht
  - ein Plan will Lock vor Rate-Limit prüfen oder Slots zurückgeben
tags: [rate-limit, lock, pipeline, gates, f3-37]
id: E-070
datum: 2026-04-16
status: gilt
quelle:
  - docs/plans/2026-04-16-008-feat-phase3b2-task-locking-plan.md:45 (Brief-Decision-6, Pipeline-Position nach Rate-Limit), :155-164 (Reihenfolge in _handle_message)
  - src/swarmq/worker/pipeline.py:632-643 (Gate-Sequenz in fester Reihenfolge, der Ordnungstest pinnt sie)
  - src/swarmq/lua/sliding_window_counter.lua:55 (Zählung beim Durchlass)
tests: []
ohne_test: Kein Test trägt bisher den Marker. Kandidat ohne Marker ist tests/unit/worker/test_pipeline.py::test_gates_run_in_fixed_order_and_ack_comes_last.
---

**Entscheidung.** Die Gates laufen in fester Reihenfolge: Ausführungsschutz, Expiry, Rate-Limit, Lock, danach der FIRE_AND_FORGET-Ack und die Ausführung. Das Rate-Limit zählt beim Durchlass; ein anschließender Lock-Konflikt stellt die Nachricht zurück, ohne den Slot zurückzugeben.

**Begründung.** Brief-Decision-6 der Locking-Phase: Der Lock sitzt nach dem Rate-Limit und vor der Ausführung, damit ein gedrosselter Task keinen Lock hält.

**Bewusste Folgen.** Jede Lock-Requeue verbraucht einen Rate-Limit-Slot; bei viel Lock-Konkurrenz läuft weniger Arbeit, als das Rate-Limit erlaubt. Bis zur Entscheidung von F3-37 ist das kein Fehler; die Doku (`rate-limiting.md`, Abschnitt „With a lock“) muss es nennen.

**Überdenken nur mit neuem Argument.** Offen (F3-37): Soll ein Lock-Konflikt den Slot zurückgeben (DECR oder Gate-Tausch)? Ein Test entsteht nur bei einer Entscheidung dafür.
