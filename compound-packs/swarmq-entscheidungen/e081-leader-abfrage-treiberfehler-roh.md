---
title: current_owner und get_scheduler_leader reichen Valkey-Treiberfehler roh weiter, damit „kein Leader“ von „Valkey weg“ unterscheidbar bleibt; die CLI fängt sie gezielt
applies_when:
  - Änderungen an ValkeyLockStore.current_owner, ValkeyBackend.get_scheduler_leader oder am LockStore-Vertrag current_owner
  - Änderungen an _startup_error_classes oder _run_command in swarmq.cli oder an swarmq schedule status
  - ein Plan will Treiberfehler in current_owner wie in force_release oder observe in BackendConnectionError übersetzen
  - eine Fehlersuche meldet, dass current_owner anders als die übrigen LockStore-Methoden keinen BackendConnectionError wirft
tags: [backend, valkey, locks, scheduler, cli, fehlerbehandlung, kp-5]
id: E-081
datum: 2026-04-23
status: gilt
quelle:
  - docs/plans/2026-04-23-001-fix-cli-refactor-residuals-plan.md (Unit 4, Entscheidung E4, KP-5)
  - src/swarmq/backend/valkey/locks.py (ValkeyLockStore.current_owner, Docstring „Decision E4“)
  - src/swarmq/backend/valkey/__init__.py (ValkeyBackend.get_scheduler_leader)
  - src/swarmq/cli/__init__.py (_startup_error_classes, _run_command)
  - 'Georg 2026-10-19 (Gegenprüfung 0.13.1, Punkt C: E4 ins Register übernehmen)'
  - docs/befunde/fehlersuche-runde-4-2026-10.md (Deferred und Verdachte, current_owner)
tests:
  - tests/unit/backend/test_valkey.py::TestSchedulerLeader::test_get_scheduler_leader_propagates_connection_error
  - tests/unit/backend/test_valkey.py::TestSchedulerLeader::test_get_scheduler_leader_propagates_timeout_error
  - tests/unit/backend/test_valkey_locks.py::TestValkeyLockStoreTokens::test_current_owner_propagates_raw_driver_errors
  - tests/unit/test_cli_schedule_status.py::test_raw_driver_error_from_the_leader_lookup_ends_with_exit_1
---

**Entscheidung.** `ValkeyLockStore.current_owner` und `ValkeyBackend.get_scheduler_leader` (ein dünner Aufruf von `current_owner` für den Scheduler-Lock) fangen Treiberfehler nicht ab: `valkey.exceptions.ConnectionError` und `TimeoutError` gehen roh an den Aufrufer. `None` heißt ausschließlich „kein Halter“. Die CLI fängt diese beiden Klassen gezielt (`_startup_error_classes` in `swarmq/cli/__init__.py`, aufgelöst erst beim Aufruf, damit `swarmq --help` `valkey.asyncio` nicht lädt); `swarmq schedule status` endet bei einem Ausfall mit einer Zeile auf stderr und Exit 1, ohne Ausgabe auf stdout.

**Begründung.** Ein abgefangener und als `None` gemeldeter Fehler sähe für Betrieb und Skripte aus wie „kein Leader gewählt“; das verdeckt einen Valkey-Ausfall. Die Entscheidung E4 im Plan 2026-04-23-001 (Unit 4, KP-5) behielt dabei das Verhalten des früheren direkten `_client.get(SCHEDULER_LOCK_KEY)` bei, als `get_scheduler_leader` öffentliche API wurde.

**Bewusste Folgen.** `current_owner` verhält sich anders als `force_release` und `observe`, die Treiberfehler in `BackendConnectionError` übersetzen. Ein Aufrufer außerhalb der CLI muss neben `BackendConnectionError` auch die rohen Valkey-Fehler erwarten. Das ist kein Fehler. Eine Vereinheitlichung (etwa `BackendConnectionError` auch hier, die CLI finge dann diese Klasse) wäre eine Vertragsänderung und braucht eine neue Entscheidung.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Eingetragen am 2026-10-19 (0.13.1); entschieden war E4 am 2026-04-23.
