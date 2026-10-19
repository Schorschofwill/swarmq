---
title: ValkeyBackend.connect() und close() sind serialisiert; ein zweites connect() nutzt einen gelungenen laufenden Aufbau, close() wartet ein laufendes connect() ab
applies_when:
  - Änderungen an ValkeyConnection.connect, ValkeyConnection.close oder an der Lebenszyklus-Sperre in backend/valkey/connection.py
  - Änderungen am Vertrag ResultBackend.connect oder ResultBackend.close (contracts/backend.py) oder an einer anderen ResultBackend-Implementierung
  - ein Plan will einen geteilten Aufbau-Task, damit Wartende denselben Fehler bekommen, oder close() ohne Warten auf connect()
  - eine Fehlersuche meldet, dass close() bis zur Gesamtfrist eines laufenden connect() wartet oder dass ein wartendes connect() nach einem Fehlschlag selbst neu verbindet
tags: [backend, valkey, lebenszyklus, nebenlaeufigkeit, connect, close, vertrag]
id: E-082
datum: 2026-10-19
status: gilt
quelle:
  - docs/plans/2026-10-19-1500-feat-swarmq-0-14-plan.md (R2, KTD2, U2; Entscheide zu den offenen Fragen, Frage 1 (a))
  - src/swarmq/backend/valkey/connection.py (ValkeyConnection.connect, ValkeyConnection.close, _lifecycle_lock)
  - src/swarmq/contracts/backend.py (ResultBackend.connect, ResultBackend.close)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (F4-13, Abbruch schließt den eigenen Client)
tests:
  - tests/unit/backend/test_valkey_connect_concurrency.py::test_concurrent_connects_build_a_single_client
  - tests/unit/backend/test_valkey_connect_concurrency.py::test_close_waits_for_a_running_connect_and_closes_its_client
  - tests/unit/backend/test_valkey_connect_concurrency.py::test_a_waiting_connect_tries_itself_after_the_running_one_failed
  - tests/unit/backend/test_valkey_connect_concurrency.py::test_cancelling_the_running_connect_closes_its_client_and_the_waiter_connects
  - tests/contracts/result_backend.py::ResultBackendContract::test_concurrent_connects_leave_one_usable_connection
  - tests/contracts/result_backend.py::ResultBackendContract::test_close_during_connect_ends_closed
---

**Entscheidung.** `ValkeyBackend.connect()` und `close()` laufen unter einer gemeinsamen `asyncio.Lock` (`ValkeyConnection._lifecycle_lock`). Ein zweites `connect()` während eines laufenden Aufbaus wartet; ist der Aufbau gelungen, kehrt es ohne eigenen Client zurück und nutzt den verbundenen. Scheitert der laufende Aufbau oder wird er abgebrochen, versucht der Wartende es danach selbst, wie ein späterer Aufruf nach F4-13. Es gibt keinen geteilten Aufbau-Task: Ein Abbruch wirkt nur im Task des Aufrufers und schließt dessen eigenen Client. `close()` wartet ein laufendes `connect()` ab und schließt danach; es bleibt kein Client ohne `aclose()` zurück. Die Sperre hält nur `ValkeyConnection.close`; das Abwarten der Pub/Sub-Closer (`ValkeyResults.close`) und `_drop_stores` (`ValkeyBackend.close`) liegen außerhalb. Der Vertrag `ResultBackend.connect/close` verlangt dieses Verhalten von jeder Implementierung.

**Begründung.** Ohne Sperre baute ein zweites `connect()` einen zweiten Client und überschrieb den ersten, der ohne `aclose()` offen blieb; ein `close()` während des Aufbaus kehrte sofort zurück, und der danach gesetzte Client blieb offen. Die Variante „Wartende bekommen denselben Fehler“ (Frage 1 (b)) bräuchte einen geteilten Aufbau-Task mit eigener Abbruchregel und verkomplizierte die Abbruchsemantik von F4-13; nebenläufige `connect()` gibt es im Code heute nicht.

**Bewusste Folgen.** `close()` wartet höchstens die Gesamtfrist eines laufenden `connect()` (`SWARMQ_BACKEND_CONNECT_TIMEOUT_S`, E-075), bevor es schließt. Ein wartendes `connect()` kann nach einem gescheiterten Aufbau selbst bis zur Gesamtfrist warten; zwei Wartende versuchen es nacheinander. Ein injizierter Client oder Pool bleibt unverändert. Fremde `ResultBackend`-Implementierungen müssen `connect`/`close` nebenläufig sicher machen; die Protocol-Signatur bleibt gleich. Kein Fehler.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Eingetragen am 2026-10-19 (0.14.0, U2).
