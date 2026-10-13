---
title: Der Autoscaler zählt nur die Haupt-Queue; queue_depth{kind="pending"} behält Haupt- plus Delay-Queues
applies_when:
  - Änderungen am Sampler des ProcessManagers, an ready_depth/queue_depth oder an den Queue-Tiefe-Metriken
  - eine Fehlersuche meldet, dass der Autoscaler bei vollen Delay-Queues nicht hochskaliert
  - eine Fehlersuche meldet, dass kind="pending" und die Skalierungsgrundlage auseinanderlaufen
tags: [autoscaling, metrik, delay-queue, bz-01, bz-05, bz-06]
id: E-027
datum: 2026-10-09
status: gilt
quelle:
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:65 (Abschnitt 1, Autoscaling Rev. 3 F9)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:422 (MR 10, Tiefe)
tests:
  - tests/unit/process_manager/test_sampler.py::TestReadyDepthForAutoscaling::test_sums_ready_depth_of_the_main_queues
  - tests/unit/process_manager/test_sampler.py::TestReadyDepthForAutoscaling::test_magicmock_broker_falls_back_to_queue_depth
  - tests/unit/process_manager/test_sampler.py::TestReadyDepthForAutoscaling::test_failing_ready_depth_falls_back_to_queue_depth
  - tests/unit/worker/test_delay_metrics.py::test_delayed_depth_is_pending_minus_ready
  - tests/integration/broker/test_delay_tiers.py::test_queue_depth_counts_tiers_and_ready_depth_only_the_main_queue
---

**Entscheidung.** Der Autoscaler summiert `ready_depth(queue)` (nur Haupt-Queue). Fehlt die Methode oder scheitert sie (Fake, Fremd-Broker), fällt er auf `queue_depth` zurück. Die Metrik `queue_depth{kind="pending"}` behält ihre Bedeutung (Haupt-Queue + alle Delay-Queues); neu ist `kind="delayed"` (nur Delay-Queues).

**Begründung.** Lock-, Guard- und Backend-Rückstellungen liegen dauerhaft in den Delay-Stufen; zählten sie mit, skalierte der Autoscaler stundenlang auf Maximum, ohne dass mehr Arbeit da ist. Kurze Retries kehren binnen Sekunden in die Haupt-Queue zurück und werden dort gezählt.

**Bewusste Folgen.** Viele lange Rückstellungen lösen kein Hochskalieren aus. Dashboards auf `kind="pending"` zeigen weiterhin die Gesamtzahl.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
