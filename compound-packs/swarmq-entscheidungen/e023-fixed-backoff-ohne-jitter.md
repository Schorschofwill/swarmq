---
title: retry_backoff "fixed" bleibt ohne Jitter; behoben sind nur Deckel-Jitter und Overflow
applies_when:
  - Änderungen an backoff.calculate_backoff
  - eine Fehlersuche meldet, dass fixed-Retries vieler Tasks synchron laufen (Thundering Herd)
  - eine Fehlersuche meldet, dass Retries oberhalb von max_delay nicht exakt max_delay warten
tags: [retry, backoff, jitter, bz-26]
id: E-023
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:140 (Entscheidungen, BZ-26)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:285 (MR 2, BZ-26)
tests:
  - tests/unit/test_backoff.py::test_fixed_strategy_returns_base_delay
  - tests/unit/test_befund_bz26_backoff.py::test_exponential_backoff_keeps_jitter_at_the_cap
  - tests/unit/test_befund_bz26_backoff.py::test_linear_backoff_keeps_jitter_at_the_cap
  - tests/unit/test_befund_bz26_backoff.py::test_exponential_backoff_with_huge_attempt_stays_at_cap
---

**Entscheidung.** `fixed` liefert immer genau `base_delay`, ohne Jitter. Behoben sind nur: Exponent gedeckelt (`min(attempt, 64)`, kein `OverflowError`) und Jitter am Deckel (`max_delay · (0,6 + 0,2u)` aus derselben Ziehung).

**Begründung.** `fixed` ist ausdrücklich eine feste Wartezeit; wer Streuung will, nimmt `exponential` oder `linear`. Der Nachweis für „fixed ohne Jitter“ ist deshalb entfallen.

**Bewusste Folgen.** Viele gleichzeitig scheiternde `fixed`-Tasks wiederholen sich synchron; das ist kein Fehler. Oberhalb von `max_delay` liegt die Wartezeit bei 60–80 % von `max_delay`.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
