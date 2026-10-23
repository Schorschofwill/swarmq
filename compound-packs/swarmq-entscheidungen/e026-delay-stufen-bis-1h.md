---
title: Rückstellungen nutzen 7 Delay-Stufen von 1 s bis 1 h; längere Wartezeiten bleiben in {q}.DQ mit Head-of-Line-Blocking
applies_when:
  - ein Retry, Lock-, Rate-Limit- oder Guard-Defer wird zurückgestellt
  - Änderungen an queue_names.delay_tier_for, declarations.py oder retry_later
  - eine Fehlersuche meldet Head-of-Line-Blocking bei Wartezeiten über 1 h
  - eine Fehlersuche meldet, dass eine Wartezeit innerhalb einer Stufe bis zum Vierfachen dauert
  - eine Fehlersuche meldet, dass eine Stufe fehlt und der Worker trotzdem startet (delay_tier_unavailable)
  - eine gelöschte Stufe lässt sich im Betrieb nicht neu anlegen (delay_tier_redeclare_failed, PF-21)
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
  - tests/unit/broker/test_delay_tiers.py::test_declaring_the_family_again_skips_ready_and_refused_tiers
  - tests/unit/broker/test_delay_tiers.py::test_tier_refused_after_an_unroutable_publish_sends_this_and_later_deferrals_to_the_delay_queue
  - tests/unit/broker/test_delay_tiers.py::test_connection_loss_while_redeclaring_a_tier_sends_this_deferral_to_the_delay_queue
  - tests/unit/broker/test_delay_tiers.py::test_during_the_pause_deferrals_go_to_the_delay_queue_without_an_attempt
  - tests/unit/broker/test_delay_tiers.py::test_after_the_pause_the_next_deferral_declares_the_tier_again_and_uses_it
  - tests/unit/broker/test_delay_tiers.py::test_while_an_attempt_runs_a_parallel_deferral_goes_to_the_delay_queue_at_once
  - tests/unit/broker/test_delay_tiers.py::test_strict_mode_mismatch_while_running_refuses_the_tier_instead_of_failing
---

**Entscheidung.** Es gibt 7 Delay-Stufen `{q}.DQ.1s` … `.1h` (höchstens Faktor 4 auseinander); gewählt wird die kleinste Stufe ≥ Wartezeit. Über 1 h bleibt die Nachricht in `{q}.DQ`. Ein Fehler beim Deklarieren einer Stufe ist nicht fatal: Rückfall auf `{q}.DQ`, WARNING `delay_tier_unavailable`, Gauge `swarmq_delay_tiers_unavailable` (Ausnahme: strenger at-least-once-Modus, siehe E-017).

**Begründung.** Eine einzige Delay-Queue mit TTL pro Nachricht ließ kurze Rückstellungen hinter einem 1-h-Retry hängen (BZ-01). Stufen begrenzen das ohne Plugin; mehr Stufen kosten Queues (7 je Queue, mit Prioritäten 21).

**Bewusste Folgen.** Über 1 h bleibt Head-of-Line-Blocking. Innerhalb einer Stufe kann eine Wartezeit bis zum Vierfachen dauern. Nur der Worker, der die Familie deklariert hat, nutzt Stufen; andere Prozesse (Client) nutzen `{q}.DQ`.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. (Technisch festgelegt als Empfehlung; Georg hat nicht widersprochen.)

**Nachtrag 2026-10-22 (PF-21, Georg: Variante D der Entscheidungsvorlage `docs/befunde/entscheidungsvorlage-pf-21-2026-10-21.md`).** Das Neu-Anlegen einer im Betrieb gelöschten Stufe trennt „abgelehnt“ von „vorübergehend gescheitert“:
- *Abgelehnt* (Kanalfehler wie 403/406, erkannter Konflikt): wie oben, Stufe für den Prozess aus, WARNING `delay_tier_unavailable`, Gauge. Im strengen Modus (E-017) gilt eine at-most-once-Stufe, die der Worker **im Betrieb** vorfindet, ebenfalls als Ablehnung, nicht fatal; Exit 78 kommt erst beim nächsten Start (wie E-083 kein Herunterfahren im Betrieb). Beim Start bleibt sie fatal.
- *Vorübergehend gescheitert* (alles andere: Verbindung weg, keine Antwort binnen `declare_probe_timeout_s` = 10 s oder der kürzeren Publish-Frist, beim Herunterfahren der kürzeren Restfrist): Die laufende Rückstellung geht an `{q}.DQ` statt neu zugestellt zu werden (keine Schleife bis `{q}.XQ`), WARNING `delay_tier_redeclare_failed` mit `retry_in_s`, danach 30 s Pause (fest, ohne Konfiguration), in der Rückstellungen der Stufe ohne Versuch an `{q}.DQ` gehen. Die erste Rückstellung nach der Pause deklariert erst neu und publiziert dann; Erfolg loggt INFO `delay_tier_restored`. Höchstens ein Versuch je Stufe gleichzeitig; eine parallele Rückstellung geht sofort an `{q}.DQ`. Auch die Familien-Deklaration heilt eine ausstehende Stufe. Der Gauge zählt weiter nur abgelehnte Stufen.

Bewusste Folge: Während der Pause gilt in `{q}.DQ` Head-of-Line-Blocking, höchstens 30 s am Stück, solange der Fehler nicht dauerhaft ist; ein dauerhafter Fehler heißt `{q}.DQ` auf Dauer, mit einer WARNING je 30 s.

**Nachtrag 2026-10-23 (F4-27, Klarstellung ohne neue Regel).** „Beim Herunterfahren der kürzeren Restfrist“ gilt auch für einen Versuch, der schon läuft, wenn das Herunterfahren beginnt: Die Restfrist zieht ihn nach wie einen laufenden Publish (R12). Bis 0.14.3 lief er bis zu seiner eigenen Frist weiter und wurde am Drain-Ende abgebrochen (Befund F4-27, `docs/befunde/fehlersuche-runde-4-2026-10.md`).
