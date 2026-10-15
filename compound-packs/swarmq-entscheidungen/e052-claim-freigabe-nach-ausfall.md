---
title: Nach einem Backend-Ausfall beim Lock-Erwerb oder Unique-Claim gibt der Aufrufer owner-geprüft frei, nach einer Fehlerantwort nicht
applies_when:
  - Änderungen am Lock-Gate (worker/gates/lock.py), an Client.schedule, _schedule_deferred oder schedule_many im Fehlerpfad des Claims
  - eine Fehlersuche meldet, dass nach einem Timeout beim Lock-Erwerb oder Unique-Claim ein Lock bzw. Flag freigegeben wird, obwohl der Aufruf scheiterte
  - eine Fehlersuche meldet, dass nach einer Fehlerantwort (BackendResponseError) nichts freigegeben wird
  - ein Plan will die Freigabe nach einem Ausfall verzögert wiederholen oder ganz entfernen
tags: [lock, unique, client, backend-ausfall, claim]
id: E-052
datum: 2026-10-14
status: gilt
quelle:
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:159-163 (KTD8, Claim-Freigabe nach Timeout owner-geprüft)
  - Commit 2dc73f8 (U12, FS-07) und Review 21a1d46 (Freigabe nur bei Backend-Ausfall, Ein-Zeilen-Regel)
  - docs/befunde/fehlersuche-runde-2-recherche/fix-r-flow.md:136 und :244-246 (F6, Restrennen „Freigabe vor Acquire“, Default nein)
  - src/swarmq/worker/gates/lock.py:263-292 und :550-560 (_release_unconfirmed)
  - src/swarmq/client/schedule.py:113-120 und :375-388 (Rollback in schedule_many)
tests:
  - tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py::test_lock_written_before_acquire_timeout_is_released
  - tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py::test_shutdown_waits_for_the_background_release_before_closing_the_backend
  - tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py::test_release_after_timeout_never_touches_a_foreign_lock
  - tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py::test_release_failing_on_the_outage_adds_no_warning_and_still_defers
  - tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py::test_error_reply_of_the_acquire_releases_nothing
  - tests/unit/client/test_befund_fs07_client_claim_after_timeout.py::test_schedule_releases_a_claim_whose_reply_timed_out
  - tests/unit/client/test_befund_fs07_client_claim_after_timeout.py::test_release_after_timeout_never_touches_a_foreign_flag
  - tests/unit/client/test_befund_fs07_client_claim_after_timeout.py::test_claim_error_reply_releases_nothing
  - tests/unit/client/test_befund_fs07_client_claim_after_timeout.py::test_schedule_many_rollback_skips_an_entry_whose_claim_got_an_error_reply
---

**Entscheidung.** Endet ein Lock-Erwerb im Lock-Gate oder ein Unique-Claim im Client mit einem Backend-Ausfall (Timeout, Verbindung; `is_backend_outage`), kann Valkey den Lock bzw. das Flag trotzdem geschrieben haben. Der Aufrufer gibt dann unter der ID dieses Versuchs (frisches `owner_token` bzw. frisch erzeugte `task_id`) owner-geprüft frei; ein fremder Lock oder ein fremdes Flag bleibt unberührt. Das Lock-Gate gibt im Hintergrund frei (getrackt, der Shutdown wartet vor `backend.close()` darauf) und stellt die Nachricht zurück; scheitert die Freigabe am selben Ausfall, steht das nur auf DEBUG (`lock_release_failed`), die eine WARNING ist `task_deferred_backend_unavailable`. Der Client gibt inline frei, auch für Einträge mit Fehler in `schedule_many`, und wirft danach den ursprünglichen Fehler. Bei einer Fehlerantwort des Backends (`BackendResponseError`, etwa `WRONGTYPE`) wird nie freigegeben: Der Claim lief dann nicht. Für den Leader regelt E-011 (Nachtrag FS-07) dasselbe mit der Einschränkung auf frisch erzeugte IDs.

**Begründung.** Der Timeout kann einen schon geschriebenen Lock oder ein gesetztes Flag verdecken; ohne Freigabe hing es bis `hard_timeout + 30 s` bzw. `unique_for` (FS-07). Weil die ID frisch ist, trifft die owner-geprüfte Freigabe nur den eigenen Versuch. Eine Fehlerantwort heißt dagegen sicher „nicht ausgeführt“, eine Freigabe wäre ein unnötiger Roundtrip gegen ein womöglich gestörtes Backend.

**Bewusste Folgen.** Eine Freigabe nach einem gescheiterten Erwerb ist kein Fehler, ebenso dass nach einer Fehlerantwort nichts freigegeben wird. Restlücke (F6, bewusst nicht geschlossen): Liegt das `EVALSHA lock_acquire` beim Abbruch schon im Socket-Puffer des Servers, kann die Freigabe über eine andere Verbindung vor dem Erwerb ausgeführt werden; dann bleibt ein verwaister Lock bis `hard_timeout + 30 s`. Eine zweite, verzögerte Freigabe gibt es nicht. Wird der Client-Aufruf selbst abgebrochen, läuft das Flag über seine TTL ab.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
