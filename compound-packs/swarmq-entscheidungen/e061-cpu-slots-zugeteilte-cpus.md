---
title: Die Slot-Voreinstellung im CPU-Modus sind die zugeteilten CPUs, nicht 10, auch für eine eingebettete App
applies_when:
  - Änderungen an SwarmQConfig.concurrency, _apply_pool_defaults oder worker/compute/slots.py
  - eine Fehlersuche meldet, dass SwarmQConfig(pool="cpu") ohne concurrency weniger als 10 Slots nimmt
  - ein Plan ändert die Voreinstellung von concurrency
tags: [cpu-modus, konfiguration, slots, speicher, f3-14]
id: E-061
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-14)
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:84 (R3, nicht die Kernzahl des Hosts und nicht 10), :124 (R25, Speicherdimensionierung), :443 (Auflösung nach dem Pool-Modus)
  - website/reference/configuration.md:34 (10 (io) / assigned CPUs (cpu))
tests:
  - tests/unit/test_slots.py::test_effective_concurrency_cpu_uses_assigned_cpus
  - tests/unit/test_slots.py::test_effective_concurrency_explicit_wins_for_every_pool
  - tests/unit/test_befund_f3_14_embedded_cpu_concurrency.py::test_embedded_cpu_pool_defaults_to_assigned_cpus
  - tests/unit/process_manager/test_run.py::TestStartWiringGaps::test_explicit_concurrency_counts_against_the_assigned_cpus
---

**Entscheidung.** Ohne ausdrückliches `concurrency` hat ein Worker im CPU-Modus so viele Slots, wie ihm CPUs zugeteilt sind (Affinität und cgroup-Quote, aufgerundet, bei `--processes N` geteilt). Das gilt für die CLI und für eine eingebettete App (`SwarmQConfig(pool="cpu")`). Ein ausdrückliches `concurrency` gewinnt immer. Im IO-Modus bleibt die Voreinstellung 10.

**Begründung.** Jeder Slot ist ein Rechenkind mit eigenem Speicher; 10 Slots auf einem kleinen Container führen zu OOM (R3, R25). Die Doku nennt die zugeteilten CPUs ohne Einschränkung auf die CLI.

**Bewusste Folgen.** Bestehende eingebettete CPU-Nutzer ohne `concurrency` bekommen mit 0.12.3 eine andere Slot-Zahl; der Changelog weist darauf hin. Das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Dass die eingebettete App bis 0.12.2 10 nahm, war der Befund F3-14 (behoben in 0.12.3).
