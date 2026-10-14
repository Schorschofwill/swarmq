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
tests:
  - tests/unit/worker/compute/test_pool.py::test_kill_slot_is_two_stage_and_waits_for_death
---

**Entscheidung.** Jedes Rechenkind bildet eine eigene Prozessgruppe. Der Kill nach `hard_timeout` oder Cancel ist zweistufig und synchron: SIGTERM an die Gruppe, bis zu 1 s Gnade, dann SIGKILL an die Gruppe, danach wartet der Pool auf den Tod des Kindes. Nach dem Kill lebt kein Nachkomme des Kindes mehr (R8, AE13). Stirbt das Kind ohne Ergebnis (OOM, Segfault), beendet der Loop-Prozess verbliebene Nachkommen des toten Kindes (R9).

**Begründung.** Killbarkeit ist das Versprechen des CPU-Modus. Ein Task, der Hilfsprozesse startet, darf nach Timeout oder Kindtod keine Prozesse zurücklassen, die weiter rechnen und bei jedem Retry neue Enkel hinzufügen.

**Bewusste Folgen.** Stirbt der **Loop-Prozess** selbst per SIGKILL, sterben die Rechenkinder binnen einer Sekunde über Forkserver und `PR_SET_PDEATHSIG`. Ihre Nachkommen verlieren das Signal aber beim `exec` und sterben nur nach bestem Bemühen (KTD3, R27). Ein überlebender Enkel in genau diesem Fall ist kein Fehler.

**Überdenken nur mit neuem Argument.** Heute teilweise verletzt: FS-16 (Fehlersuche Runde 2, xfail-Nachweis im Branch `test/fehlersuche-runde-2`). Ist das Kind nach SIGTERM schon tot (Fall a) oder ohne Ergebnis gestorben (Fall b, R9), schickt der Pool kein SIGKILL an die Gruppe; ein Enkel, der SIGTERM ignoriert bzw. nie eines bekam, läuft weiter. Der markierte Test deckt nur den Fall ab, in dem das Kind bis zum SIGKILL lebt. Der e2e-Test `tests/e2e/test_cpu_mode.py::test_kill_takes_the_grandchild_with_it` prüft AE13 gegen echte Prozesse.
