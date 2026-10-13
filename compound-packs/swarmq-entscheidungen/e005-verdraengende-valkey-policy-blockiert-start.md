---
title: Eine verdrängende Valkey-maxmemory-policy verhindert den Worker-Start (strict), abschaltbar
applies_when:
  - ein Worker startet gegen Valkey mit allkeys-*/volatile-* Policy
  - Änderungen an worker/eviction_gate.py, valkey_policy.py, backend/valkey/environment.py oder SWARMQ_VALKEY_EVICTION_CHECK
  - eine Fehlersuche meldet, dass der Worker mit Exit 78 nicht startet, obwohl Valkey erreichbar ist
  - eine Fehlersuche meldet, dass eine nicht lesbare Policy (CONFIG GET verboten) den Start nicht blockiert
tags: [valkey, eviction, start, konfiguration, bz-04]
id: E-005
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:125 (Entscheidungen, BZ-04)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:44 (Abschnitt 1, BZ-04 strict)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:536 (MR 14)
tests:
  - tests/unit/worker/test_eviction_gate.py::test_strict_refuses_to_start_under_an_evicting_policy
  - tests/unit/worker/test_eviction_gate.py::test_start_goes_on_unless_strict_meets_a_proven_evicting_policy
  - tests/unit/test_config_valkey_eviction.py::test_default_is_strict
  - tests/unit/scheduler/test_valkey_watch.py::test_recheck_at_most_once_per_hour
---

**Entscheidung.** Default `SWARMQ_VALKEY_EVICTION_CHECK=strict`: Bei einer nachweislich verdrängenden Policy verweigert der Worker (und damit der Leader) den Start mit `ConfigurationError` (Exit 78), geprüft vor `_wire_coordination`. `warn` und `off` schalten ab. Ist die Policy nicht prüfbar, blockiert das nie, es gibt nur die WARNING `valkey_eviction_policy_unknown`. Der Leader prüft stündlich erneut und warnt.

**Begründung.** Verdrängung trifft Locks, Guard-, Unique- und Fired-Marker und führt zu Doppelausführung (BZ-04). Ein lauter Startabbruch ist besser als stille Doppelläufe; der Abschalter erlaubt bewusste Ausnahmen.

**Bewusste Folgen.** Unter K8s CrashLoopBackOff, unter Docker mit `restart:` eine Neustartschleife, bis die Policy korrigiert oder der Check gelockert ist. Eine verdrängende Policy mit unbekanntem `maxmemory` gilt als unsicher und blockiert. Grenzen (Plan Abschnitt 7): Flush bei gleichzeitigem Neustart aller Prozesse, Verlust seit RDB-Snapshot bleiben unerkannt; die Policy wird nur stündlich nachgeprüft.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
