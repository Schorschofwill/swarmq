---
title: Der Kill eines Rechenkinds trifft seine ganze Prozessgruppe; nach Kindtod beendet der Loop-Prozess verbliebene Nachkommen
applies_when:
  - Änderungen an _kill_child, an der Prozessgruppe der Rechenkinder oder an der Behandlung von Kindtod im SlotPool
  - eine Fehlersuche meldet überlebende Enkelprozesse (z. B. ImageMagick) nach hard_timeout, Cancel oder Kindtod
  - eine Fehlersuche meldet, dass Nachkommen nach SIGKILL des Loop-Prozesses weiterleben
tags: [cpu-modus, compute, kill, prozessgruppe, zusage]
id: E-036
datum: 2026-09-03
status: gilt
quelle:
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:92 (R8), :93 (R9), :99 (R27), :184 (AE13), :259 (KTD3)
  - website/guides/cpu-mode.md:19 („kills the child's process group (SIGTERM, 1 s grace, SIGKILL)“)
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md (KTD17, Nachtrag FS-16, 2026-10-14)
tests:
  - tests/unit/worker/compute/test_pool.py::test_kill_slot_is_two_stage_and_waits_for_death
---

**Entscheidung.** Jedes Rechenkind bildet eine eigene Prozessgruppe. Der Kill nach `hard_timeout` oder Cancel ist zweistufig und synchron: SIGTERM an die Gruppe, bis zu 1 s Gnade, dann SIGKILL an die Gruppe, danach wartet der Pool auf den Tod des Kindes. Nach dem Kill lebt kein Nachkomme des Kindes mehr (R8, AE13). Stirbt das Kind ohne Ergebnis (OOM, Segfault), beendet der Loop-Prozess verbliebene Nachkommen des toten Kindes (R9).

**Begründung.** Killbarkeit ist das Versprechen des CPU-Modus. Ein Task, der Hilfsprozesse startet, darf nach Timeout oder Kindtod keine Prozesse zurücklassen, die weiter rechnen und bei jedem Retry neue Enkel hinzufügen.

**Bewusste Folgen.** Stirbt der **Loop-Prozess** selbst per SIGKILL, sterben die Rechenkinder binnen einer Sekunde über Forkserver und `PR_SET_PDEATHSIG`. Ihre Nachkommen verlieren das Signal aber beim `exec` und sterben nur nach bestem Bemühen (KTD3, R27). Ein überlebender Enkel in genau diesem Fall ist kein Fehler.

**Nachtrag 2026-10-14 (FS-16, KTD17).** Nach dem Tod des Kindes geht genau ein SIGKILL an seine Prozessgruppe („Sweep“). Das gilt, wenn das Kind in der SIGTERM-Gnade endet (Fall a), wenn es ohne Ergebnis stirbt (Fall b; Quelle ist R9) und wenn ein warmes DI-Kind zwischen zwei Jobs ohne Ende-Signal stirbt. Voraussetzung ist die bestätigte eigene Gruppe (Ready-Handshake); vor dem Ready und beim Kill nur per PID gibt es kein `killpg`. Hat die Gruppe schon das SIGKILL der zweiten Stufe bekommen, entfällt der Sweep (Merker `group_killed`); der markierte Test bleibt deshalb bei genau `[SIGTERM, SIGKILL]`. Kein Sweep nach regulärem Ende (Ergebnis geliefert, Recycling- oder Close-Ende mit Ende-Signal): Ob dort verbliebene Enkel beendet werden, entscheidet diese Regel nicht. Ein Enkel, der sich per `setsid` aus der Gruppe löst, bleibt unerreichbar. Festgehalten in `tests/unit/worker/compute/test_befund_fs16_kill_group.py` (echte Prozesse) und `tests/unit/worker/compute/test_pool.py` (ohne `entscheidung`-Marker).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt (FS-16 ist mit dem Nachtrag behoben). Der e2e-Test `tests/e2e/test_cpu_mode.py::test_kill_takes_the_grandchild_with_it` prüft AE13 gegen echte Prozesse.
