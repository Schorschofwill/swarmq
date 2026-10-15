---
title: get_result wartet 1 s auf Pub/Sub und pollt danach alle 5 ms; der Parameter polling wirkt nicht
applies_when:
  - Änderungen an Client.get_result, get_results oder ValkeyResults.wait_for_result
  - eine Fehlersuche meldet die GET-Last des 5-ms-Pollings oder das ignorierte polling-Argument als Fehler
  - ein Plan will das Warteverhalten auf Ergebnisse umbauen (Intervall, Backoff, Pub/Sub-Dauer)
tags: [client, results, pubsub, polling, valkey]
id: E-030
datum: 2026-04-15
status: gilt
quelle:
  - docs/brainstorms/performance-optimization-requirements.md:25-26 (R3, R4)
  - docs/plans/2026-04-15-005-feat-pubsub-batch-performance-plan.md:27-28, 72, 255-256 (R3, R4)
  - Commit c18a144 (asyncio.sleep(polling) ersetzt durch festes 5-ms-Intervall)
  - website/reference/api.md:227-229 („5ms-interval polling as a fallback“)
  - src/swarmq/backend/valkey/results.py:30 (_FALLBACK_POLL_S)
  - Commit 1b05cd9 (U20, FS-15: subscribe und Teardown an die Frist gebunden) und Review d2aa879 (verworfener PubSub ohne unsubscribe, close() räumt Hintergrund-Closer ab)
  - src/swarmq/backend/valkey/results.py:229-275 (wait_for_result), :490-512 (wait_for_workflow)
tests:
  - tests/unit/backend/test_valkey_results.py::TestWaitForResult::test_pubsub_miss_falls_back_to_polling
  - tests/unit/backend/test_befund_fs15_result_wait_polling.py::test_hanging_subscribe_does_not_outlast_the_timeout
  - tests/unit/backend/test_befund_fs15_result_wait_polling.py::test_hanging_subscribe_still_finds_an_existing_result_within_the_pubsub_window
  - tests/unit/backend/test_befund_fs15_result_wait_polling.py::test_hanging_subscribe_with_timeout_below_the_pubsub_window_ends_at_the_timeout
  - tests/unit/backend/test_befund_fs15_result_wait_polling.py::test_hanging_unsubscribe_in_teardown_does_not_outlast_the_timeout
  - tests/unit/backend/test_befund_fs15_result_wait_polling.py::test_teardown_after_the_timeout_still_gets_its_minimum_grace
  - tests/unit/backend/test_befund_fs15_result_wait_polling.py::test_wait_for_workflow_with_hanging_subscribe_ends_at_the_timeout
---

**Entscheidung.** `get_result()` abonniert den Benachrichtigungskanal, prüft einmal per GET und wartet dann etwa 1 s auf die Pub/Sub-Benachrichtigung. Kommt keine, fällt der Client auf Polling mit festem 5-ms-Intervall zurück, bis `timeout` abläuft. Das Argument `polling` bleibt aus Signatur-Kompatibilität (R4) bestehen, wirkt aber nicht.

**Begründung.** Ziel war geringe Latenz für kurze Tasks (Benchmark P50 12,5 ms) bei unveränderter API. Das Polling gilt als Rückfall, falls eine Benachrichtigung verloren geht.

**Bewusste Folgen.** Ein Aufruf mit `polling=0.2` pollt trotzdem alle 5 ms. Kein Fehler ist auch, dass ein Wartender nach 1 s bis zu etwa 200 GET/s erzeugt.

**Nachtrag 2026-10-14 (FS-15 Teil 3, U20).** Abonnement und Teardown zählen zur Frist. `wait_for_result` begrenzt das `subscribe` auf `min(_PUBSUB_WINDOW_S, Restfrist)`, also höchstens ein Pub/Sub-Fenster (1 s). Scheitert es oder dauert es länger, wird der PubSub verworfen (im Hintergrund geschlossen, ohne `unsubscribe`, weil valkey-py dafür eine getrennte Verbindung neu aufbauen würde) und ohne Pub/Sub-Fenster direkt gepollt. Der Teardown nach dem Warten läuft bis zur Deadline, mindestens aber `_TEARDOWN_GRACE_S` (50 ms); was dann noch läuft, schließt im Hintergrund weiter, und `close()` des Backends wartet darauf höchstens 0,1 s und bricht den Rest ab. `wait_for_workflow` rechnet das Abonnement auf das volle `timeout` an; ohne Abonnement meldet es keine Benachrichtigung (`False`). Bewusste Folge: Ein langsames, aber gesundes Valkey kann ein Ergebnis, das erst nach dem Abonnement-Versuch entsteht, nur noch per 5-ms-Polling finden. Ein Abbruch des Aufrufers während des Abonnements schließt den PubSub trotzdem.

**Überdenken nur mit neuem Argument.** Offen: FS-15 Teil 1 (Fehlersuche Runde 2). Nach 1 s liest der Client Pub/Sub gar nicht mehr, obwohl das Abonnement gesund bleibt. Für jeden Task über 1 s Laufzeit ist das 5-ms-Polling damit der Normalpfad und kein Rückfall. Die Last bei langen Wartezeiten hat die Entscheidung nicht betrachtet (Produktfrage, Plan 2026-10-14-0748 „Offene Produktfragen“). Bis U20 blockierte ein `subscribe()` ohne Frist den Aufrufer über `timeout` hinaus (FS-15 Teil 3); das war ein Fehler gegen den Docstring von `wait_for_result`, nicht von dieser Entscheidung gedeckt, und ist behoben (Nachtrag oben).
