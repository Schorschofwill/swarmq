---
title: Eine Zustellung, die nach Drain-Beginn noch ankommt, gehört zum Drain und endet wie jeder andere Handler am Drain-Ende
applies_when:
  - Änderungen an Worker._drain_in_flight, am Drain-Snapshot oder an cancel_consume im Shutdown
  - eine Fehlersuche meldet, dass ein Handler, der erst während des Drains startet, am Drain-Ende abgebrochen statt per nack zurückgegeben wird
  - ein Plan will späte Zustellungen im Drain sofort zurückgeben oder den Drain auf die beim Beginn laufenden Handler beschränken
tags: [shutdown, drain, consumer, zustellung]
id: E-050
datum: 2026-10-14
status: gilt
quelle:
  - Commit 3348b96 (Review U10, „späte Zustellungen im Drain“)
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:495-506 (U10, Consumer-Wächter FS-09)
  - src/swarmq/worker/shutdown.py:106-123 (_drain_in_flight, Nachsnapshot)
tests:
  - tests/unit/worker/test_shutdown_budget_steps.py::test_a_delivery_after_the_drain_snapshot_is_drained_too
  - tests/unit/worker/test_shutdown_budget_steps.py::test_a_late_delivery_beside_a_long_runner_is_cancelled_at_the_drain_end
---

**Entscheidung.** Startet während des Drains noch ein Handler (Zustellung an einen spät angemeldeten Consumer oder nach einem aufgegebenen `cancel_consume`, E-053), nimmt der Drain ihn mit: Nach jedem Ende des Wartens wird der Bestand der aktiven Tasks neu erfasst und bis zum selben Drain-Ende weiter gewartet. Was am Drain-Ende noch läuft, auch ein später Handler, wird abgebrochen und folgt E-001. Eine späte Zustellung wird nicht per `nack` zurückgegeben.

**Begründung.** Ohne Nachsnapshot liefen späte Handler ungesehen in Sperrenfreigabe und `broker.close()`; ihr Settle scheiterte dort, und die Nachricht käme nach teilweiser Ausführung erneut. Ein sofortiges `nack` bräuchte einen eigenen Pfad im Handler, obwohl der Fehlerpfad nach E-001 schon definiert ist.

**Bewusste Folgen.** Ein Handler, der erst nach Drain-Beginn startet und am Drain-Ende abgebrochen wird, ist kein Fehler; er endet wie jeder abgebrochene Handler (E-001, mit Nachtrag FS-02a). Das Drain-Ende verschiebt sich durch späte Zustellungen nicht. Späte Zustellungen sind selten, weil der Consumer-Wächter nach `cancel_all` keine Neuanmeldung mehr annimmt und eine späte `consume`-Antwort sofort wieder abmeldet (E-053).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
