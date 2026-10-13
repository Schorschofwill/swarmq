---
title: Prefetch gilt pro Consumer, nicht pro Worker – dokumentierte Grenze mit Startwarnung
applies_when:
  - ein Worker konsumiert mehrere Queues oder Prioritätsbänder
  - Änderungen an worker/prefetch.py, QoS oder an der Consumer-Anlage
  - eine Fehlersuche meldet, dass ein Worker mehr Nachrichten hält als concurrency
  - jemand schlägt globales QoS oder einen gemeinsamen Kanal vor
tags: [broker, prefetch, qos, consumer-timeout, bz-20]
id: E-019
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:107 (Nachweis-Tests BZ-20: Grenze, gemeldet)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:53 (Abschnitt 1, BZ-20 dokumentierte Grenze)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:611 (MR 18, BZ-20)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:722 (Abschnitt 7)
tests:
  - tests/unit/worker/test_prefetch_warning.py::test_queues_times_hard_timeout_over_consumer_timeout_warns_once
  - tests/unit/worker/test_prefetch_warning.py::test_no_warning_within_the_timeout_or_with_one_queue
  - tests/unit/worker/test_prefetch_warning.py::test_sampler_reports_the_prefetch_limit
---

**Entscheidung.** Prefetch bleibt pro Consumer. Statt eines Fixes gibt es die Start-WARNING `prefetch_exceeds_consumer_timeout` (einmal je Start, bei mindestens 2 Queues und `hard_timeout × K > consumer_timeout_s`), die Gauge `swarmq_worker_prefetch_limit` und Doku.

**Begründung.** Globales QoS geht bei Quorum-Queues nicht. Die ursprüngliche Register-Entscheidung („`concurrency` als Obergrenze für den ganzen Worker, gemeinsamer Kanal mit QoS“) ist damit durch die Präzisierung vom 09.10. abends ersetzt.

**Bewusste Folgen.** Ein Worker mit K Queues kann bis zu K × concurrency Nachrichten halten; wartende Nachrichten laufen gegen den `consumer_timeout`. Der Nachweis `tests/integration/broker/test_befund_bz20_prefetch_per_consumer.py` bleibt bewusst xfail (Begründung „Grenze“) und ist kein offener Fehler.

**Überdenken nur mit neuem Argument.** Folgearbeit: native Quorum-Prioritäten (Plan Abschnitt 8).
