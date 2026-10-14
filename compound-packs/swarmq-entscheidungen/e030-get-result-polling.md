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
tests:
  - tests/unit/backend/test_valkey_results.py::TestWaitForResult::test_pubsub_miss_falls_back_to_polling
---

**Entscheidung.** `get_result()` abonniert den Benachrichtigungskanal, prüft einmal per GET und wartet dann etwa 1 s auf die Pub/Sub-Benachrichtigung. Kommt keine, fällt der Client auf Polling mit festem 5-ms-Intervall zurück, bis `timeout` abläuft. Das Argument `polling` bleibt aus Signatur-Kompatibilität (R4) bestehen, wirkt aber nicht.

**Begründung.** Ziel war geringe Latenz für kurze Tasks (Benchmark P50 12,5 ms) bei unveränderter API. Das Polling gilt als Rückfall, falls eine Benachrichtigung verloren geht.

**Bewusste Folgen.** Ein Aufruf mit `polling=0.2` pollt trotzdem alle 5 ms. Kein Fehler ist auch, dass ein Wartender nach 1 s bis zu etwa 200 GET/s erzeugt.

**Überdenken nur mit neuem Argument.** Offen: FS-15 (Fehlersuche Runde 2). Nach 1 s liest der Client Pub/Sub gar nicht mehr, obwohl das Abonnement gesund bleibt. Für jeden Task über 1 s Laufzeit ist das 5-ms-Polling damit der Normalpfad und kein Rückfall. Die Last bei langen Wartezeiten hat die Entscheidung nicht betrachtet. Nicht von dieser Entscheidung gedeckt ist ein `subscribe()` ohne Frist, das den Aufrufer über `timeout` hinaus blockiert: Das ist ein Fehler (FS-15, Teil 3), denn der Docstring von `wait_for_result` verspricht das Gegenteil.
