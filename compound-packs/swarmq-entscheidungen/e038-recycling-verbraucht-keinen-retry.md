---
title: Ein Selbst-Recycling verbraucht keinen Retry eines laufenden Tasks
applies_when:
  - Änderungen am Selbst-Recycling (worker/recycling.py), an der Recycling-Drainfrist oder an der Publish-Endfrist im Recycling
  - eine Fehlersuche meldet, dass ein Recycling laufende Tasks abbricht, Retries verbraucht oder Fehlerergebnisse erzeugt
  - ein Plan ändert die Drainfrist-Berechnung (größter hard_timeout plus Puffer)
tags: [recycling, drain, retry, supervisor, zusage]
id: E-038
datum: 2026-09-03
status: gilt
quelle:
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:115 (R22, „Ein Recycling verbraucht keinen Retry eines laufenden Tasks.“)
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:267 (KTD11, Drain ohne erzwungenen Abbruch, Frist größter hard_timeout plus Puffer)
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:389 (Risikotabelle)
  - website/reference/changelog.md:2293-2296 („drains without forced cancellation“)
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:88 (Key Decision „Recycling-Grenze consumer_timeout_s“, Georg 2026-10-14)
tests:
  - tests/unit/worker/test_befund_fs30_recycling_drain.py::test_recycling_drain_does_not_abort_a_long_runner_without_hard_timeout
  - tests/unit/worker/test_recycling.py::test_mixed_registry_waits_at_least_consumer_timeout
---

**Entscheidung.** Recycelt sich ein Worker selbst (RSS- oder Taskzahl-Schwelle, Opt-in), nimmt er keine neuen Nachrichten an und wartet ohne erzwungenen Abbruch auf das Ende laufender Tasks. Die Drainfrist ist der größte registrierte `hard_timeout` plus Puffer, nicht `shutdown_timeout`. Ein laufender Task verliert durch das Recycling keinen Retry; der erzwungene Abbruch bleibt SIGTERM vorbehalten (dort gilt E-001).

**Nachtrag 2026-10-14 (FS-30, U9): Recycling-Grenze `consumer_timeout_s`.** Gezählt werden nur Tasks auf den Queues, die der Worker konsumiert (`consumed_queues`). Hat einer davon keinen `hard_timeout`, ist die Drainfrist `max(größter hard_timeout + 30 s, consumer_timeout_s)`; haben alle einen, bleibt es bei `größter hard_timeout + 30 s`; ohne konsumierten Task bei `shutdown_timeout`. Danach hat RabbitMQ jede vor dem Recycling zugestellte Nachricht ohnehin zurückgenommen, längeres Warten brächte keinen Ack mehr. Grenze der Zusage: Ein Task, der über `consumer_timeout_s` hinaus läuft, wird am Fristende nach E-001 abgebrochen; stimmt `consumer_timeout_s` nicht mit dem Broker überein (z. B. Broker ohne Consumer-Timeout), trifft das auch einen gesunden Langläufer. Erreicht ein Handler das Consumer-Timeout des Brokers, schließt RabbitMQ den gemeinsamen Consume-Kanal (E-019), und die übrigen Handler verlieren ihre Zustellung. Ein SIGTERM während des Recycling-Drains setzt das Shutdown-Budget ab diesem Signal in Kraft (E-018, Nachtrag FS-30); dort gilt E-001.

**Begründung.** Recycling ist eine Betriebsmaßnahme gegen Speicherlecks. Ein gesunder Langläufer soll dafür nicht bei jedem Recycling einen Retry verlieren oder in der DLQ landen.

**Bewusste Folgen.** Ein Recycling-Drain, der deutlich länger dauert als `shutdown_timeout`, ist kein Fehler. Ein Task, der seinen eigenen `hard_timeout` reißt, folgt auch während des Recyclings E-037.

**Überdenken nur mit neuem Argument.** Befunde der Fehlersuche Runde 2:
- FS-30, behoben (U9, Plan 2026-10-14-0748; früher FS-01 Recycling-Teil, MB-2): `recycling.py` fiel ohne registrierten `hard_timeout` (Default `None`) auf `shutdown_timeout` zurück und brach Langläufer dann nach E-001 als Fehler ab. Der Rückfall war eine Umsetzungslücke gegen R22; `tests/unit/worker/test_recycling.py::test_task_count_threshold_recycles_with_ack` erwartet jetzt `consumer_timeout_s` (Nachtrag oben).
- FS-02 Teil b, behoben (U3, Plan 2026-10-14-0748): Die Publish-Endfrist lag beim Recycling bei `drain_until`, nicht am Drain-Ende; Retry-, DLQ- und Workflow-Publishes nach etwa 23 s liefen in die verstrichene Frist. Seit U3 folgt die Endfrist dem Drain-Ende des Recyclings. Ein erst am Ende der Recycling-Frist abgebrochener Task wird danach neu zugestellt statt per Retry kopiert (E-001, Nachtrag).
