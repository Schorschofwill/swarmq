---
title: Rückstellungen nutzen 7 Delay-Stufen von 1 s bis 1 h; längere Wartezeiten bleiben in {q}.DQ mit Head-of-Line-Blocking
applies_when:
  - ein Retry, Lock-, Rate-Limit- oder Guard-Defer wird zurückgestellt
  - Änderungen an queue_names.delay_tier_for, declarations.py oder retry_later
  - eine Fehlersuche meldet Head-of-Line-Blocking bei Wartezeiten über 1 h
  - eine Fehlersuche meldet, dass eine Wartezeit innerhalb einer Stufe bis zum Vierfachen dauert
  - eine Fehlersuche meldet, dass eine Stufe fehlt und der Worker trotzdem startet (delay_tier_unavailable)
tags: [broker, delay-queue, retry, bz-01]
id: E-026
datum: 2026-10-09
status: gilt
quelle:
  - 'docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:58 (Abschnitt 1, technisch festgelegt: BZ-01 7 Stufen)'
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:422 (MR 10, BZ-01)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:707 (Abschnitt 7, BZ-01)
tests:
  - tests/unit/test_queue_names.py::test_delay_tier_for_picks_the_smallest_tier_reaching_the_delay
  - tests/unit/test_queue_names.py::test_seven_delay_tiers_from_one_second_to_one_hour_at_most_factor_four_apart
  - tests/unit/broker/test_delay_tiers.py::test_tier_that_cannot_be_declared_falls_back_to_the_delay_queue_with_one_warning
  - tests/unit/broker/test_delay_tiers.py::test_delay_over_one_hour_stays_in_the_delay_queue
  - tests/integration/broker/test_delay_tiers.py::test_refused_tier_falls_back_to_the_delay_queue
  - tests/unit/worker/test_delay_metrics.py::test_unavailable_delay_tiers_gauge_per_consumed_queue
---

**Entscheidung.** Es gibt 7 Delay-Stufen `{q}.DQ.1s` … `.1h` (höchstens Faktor 4 auseinander); gewählt wird die kleinste Stufe ≥ Wartezeit. Über 1 h bleibt die Nachricht in `{q}.DQ`. Ein Fehler beim Deklarieren einer Stufe ist nicht fatal: Rückfall auf `{q}.DQ`, WARNING `delay_tier_unavailable`, Gauge `swarmq_delay_tiers_unavailable` (Ausnahme: strenger at-least-once-Modus, siehe E-017).

**Begründung.** Eine einzige Delay-Queue mit TTL pro Nachricht ließ kurze Rückstellungen hinter einem 1-h-Retry hängen (BZ-01). Stufen begrenzen das ohne Plugin; mehr Stufen kosten Queues (7 je Queue, mit Prioritäten 21).

**Bewusste Folgen.** Über 1 h bleibt Head-of-Line-Blocking. Innerhalb einer Stufe kann eine Wartezeit bis zum Vierfachen dauern. Nur der Worker, der die Familie deklariert hat, nutzt Stufen; andere Prozesse (Client) nutzen `{q}.DQ`.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. (Technisch festgelegt als Empfehlung; Georg hat nicht widersprochen.)
