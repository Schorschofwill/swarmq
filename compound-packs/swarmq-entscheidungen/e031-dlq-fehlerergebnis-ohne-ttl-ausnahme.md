---
title: Ein vom Worker dead-gelettered Task hinterlässt ein Fehlerergebnis, auch bei FIRE_AND_FORGET oder result_ttl=0
applies_when:
  - Änderungen am Dead-Letter-Pfad (RetryHandler.dead_letter) oder am Expiry-Gate, die das Speichern des Fehlerergebnisses betreffen
  - eine Fehlersuche meldet, dass trotz result_ttl=0 oder FIRE_AND_FORGET ein Fehlerergebnis gespeichert wird
  - ein Plan will die result_ttl-Kaskade für terminale Fehler vereinheitlichen
tags: [dlq, results, result-ttl, expiry, fire-and-forget]
id: E-031
datum: 2026-09-14
status: gilt
quelle:
  - website/guides/dead-letter-queues.md:51 („Dead-lettering by the worker … an error result is stored“, ohne Ausnahme)
  - src/swarmq/worker/retry.py:286-292 (Kommentar „kein FIRE_AND_FORGET-Zweig, und 0 faellt hier auf DEFAULT_RESULT_TTL_S“)
  - Commit ad81f71 (2026-10-11, „retry, execution und executor fallen bei 0 weiter auf 24 h“)
  - docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md (U9, task_info.result_ttl vor Konfiguration)
tests:
  - tests/unit/worker/test_retry.py::test_dead_letter_stores_error_result_with_task_result_ttl
  - tests/unit/worker/gates/test_expiry.py::TestCheckMaxAgeExpired::test_expired_stores_error_result_for_callers
---

**Entscheidung.** Endet ein Task terminal über die DLQ des Workers (Retries erschöpft, `NoRetry`, `max_age`, Lock-Fehler, Delivery-Limit), speichert der Worker ein Fehlerergebnis, damit `get_result()` mit einem Fehler endet statt in den Timeout zu laufen. Anders als auf dem Erfolgspfad gibt es dabei keinen FIRE_AND_FORGET-Zweig. Ein `result_ttl=0` am Task fällt auf die Konfiguration bzw. auf `DEFAULT_RESULT_TTL_S` (24 h) zurück, statt „nicht speichern“ zu bedeuten.

**Begründung.** Ein terminaler Fehler soll für Aufrufer und Diagnose sichtbar sein. Die Nutzer-Doku sagt ohne Ausnahme zu, dass auf diesem Pfad ein Fehlerergebnis entsteht.

**Bewusste Folgen.** Ein Fehlerergebnis trotz `result_ttl=0` oder FIRE_AND_FORGET ist kein Fehler (FS-18 ist als ENTSCHEIDUNG eingestuft). Abgrenzung: Im Expiry-Gate überspringt eine **globale** `result_ttl=0` der Konfiguration das Speichern weiterhin (Härtung R2 P2, vermeidet `SET … PX 0`; festgeschrieben durch `test_expired_skips_result_store_when_ttl_zero`). Der Kommentar in `src/swarmq/worker/gates/expiry.py:81-83` („mirrors the main-path semantics“) beschreibt den Code ungenau und ist zu korrigieren, nicht das Verhalten.

**Überdenken nur mit neuem Argument.** Ein Test, der ausdrücklich FIRE_AND_FORGET bzw. `result_ttl=0` am Task auf dem Dead-Letter-Pfad prüft, fehlt noch. Kleiner Rest: Expiry nutzt `config.result_ttl`, `dead_letter` `DEFAULT_RESULT_TTL_S`; bei Default-Konfiguration ist beides gleich.
