# Fehlersuche Runde 4: Sichtung der Überlebenden, Gruppe E (Broker und Client)

Stand: Worktree `swarmq-fehlersuche4`, Branch `test/fehlersuche-runde-4` (Commit 7b14a90). Einheit U5, Gruppe E.

## Kopf

- **Eingabe:** `mut-gruppe-E.txt` aus U3 (Mutation Lauf A, nur `tests/unit`), nach Vorfilter: **369 Mutanten**, davon 328 `survived` und 41 `no tests`. Die 115 aussortierten Mutanten (`mut-gruppe-E-gefiltert.txt`: Strings, Logtexte, Typannotationen, `__repr__`) wurden nicht gesichtet.
- **Module:** `broker/rabbitmq/{admin, arg_conflict, blocked, connection, consumer_watch, declarations, header_guard, redeclare}.py` (ohne `delivery.py`/`publish_phase.py`, R14), `client/workflows.py`, `client/results.py`. `topology.py` hat keine Überlebenden.
- **Ergebnis:** 2 Fehlerverdachte (R4M-E-01, R4M-E-02), beide mit rotem Probe-Test. 260 Mutanten zeigen Testlücken, 106 sind Rauschen (äquivalent oder nur auf Fakes sichtbar), 3 gehören zu R4M-E-01.
- **Probe-Tests:** `/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/608b3617-a1bc-47f1-bd40-e763775cafe6/scratchpad/u5e/test_mut_e_nachweise.py`. Jeder Test prüft das **erwartete** Verhalten, ein roter Test bestätigt den Verdacht. Die Tests brauchen keine Dienste. Lauf aus dem Worktree:

```
PYTHONPATH=. uv run --extra test pytest -p no:randomly --rootdir . -c pyproject.toml \
  <scratchpad>/u5e/test_mut_e_nachweise.py
```

Ergebnis: 3 von 3 rot, jeweils an der fachlichen Assertion (`86400000 == 604800000`, `60000 >= 86400000`, `Expected close to have been awaited`).

**Grenze der Aussage:** Lauf A hat nur `tests/unit` ausgeführt. Ein Großteil der Testlücken in `admin.py`, `connection.py` und `client/workflows.py` ist auf Unit-Ebene offen, wird aber vermutlich von Integrationstests getötet, z. B. `tests/integration/broker/test_rabbitmq_dlq.py` (inspect, retry, purge), `tests/integration/test_workflow_orchestration.py` (apply, Reattach) oder `tests/contracts/publisher_consumer.py` (`ping`). Lauf B (U4) wiederholt nur das Valkey-Backend. Für Gruppe E gibt es deshalb keine Schnittmenge „in A und B überlebt“. Testlücken heißen hier „auf Unit-Ebene nicht geprüft“.

**Abweichungen zwischen Plan und Code:**

1. Plan U5 nennt die IDs `R4M-nn`, der Auftrag `R4M-E-nn`. Verwendet wird `R4M-E-nn`.
2. Der Lauf-A-Bericht (Einstieg Nr. 8) vermutet, dass bei `connection.py:576` der Prefetch nach einer Neuanlage fehlen würde. Das stimmt nicht: Der einzige Aufrufer, `delivery.consume()` (`delivery.py:522-526`), setzt `_consume_prefetch` direkt davor und ruft danach immer selbst `set_qos(prefetch_count=concurrency)`. Die Mutanten sind auf dem einzigen Aufrufpfad äquivalent (Rauschen).
3. Plan U5, Schritt 4 (Abgleich mit den Testlücken aus Runde 2: Drain-Frist 0, `lock_wait_max_s`, Keeper-Timeout, `_check_late_cancel`): Diese Stellen liegen im Worker bzw. Backend, nicht in Gruppe E. Daraus folgt kein Eintrag.

---

## Teil 1: Fehlerverdachte

### R4M-E-01 (P3): Das Cancel-Flag eines Workflows lebt je nach Handle unterschiedlich lange, und keine der beiden Varianten folgt der Regel für Task-Cancels

- **Fundstelle:** `src/swarmq/client/workflows.py:201-206` (`ClientWorkflows.workflow`: `cancel_ttl_ms=_DEFAULT_WORKFLOW_TTL_MS`, fest 24 h) gegen `:156` (`apply`: `cancel_ttl_ms=state_ttl_ms`). `app.apply` übergibt `state_ttl_ms=config.result_ttl * 1000` (`src/swarmq/app.py:238`). Verwendung in `WorkflowHandle.cancel` (`src/swarmq/workflow_orchestration.py:154`).
- **Mutanten:** `client.workflows.xǁClientWorkflowsǁworkflow__mutmut_11`, `__mutmut_15` (`cancel_ttl_ms=None` bzw. entfernt, `no tests`) und `apply__mutmut_87` (`cancel_ttl_ms=None`, überlebt). Kein Test hält die Lebensdauer des Workflow-Cancel-Flags fest. Beim Lesen fiel auf, dass die beiden Wege voneinander abweichen.
- **Szenario:**
  1. **Reattach, `result_ttl` über 24 h.** Laut Doku soll `result_ttl` für lange Workflows erhöht werden (`website/guides/workflows.de.md:171-173`). Mit `result_ttl=7 d` lebt das Flag aus `(await app.apply(wf)).cancel()` 7 Tage. Das Flag aus `(await app.workflow(id)).cancel()` lebt nur 24 h. Liegen Group- oder Chord-Glieder nach dem Cancel länger als 24 h in der Queue (Rückstau, Retry über die Delay-Queue, langer Vorgänger-Schritt), läuft die Prüfung beim Abholen (`worker/gates/cancel.py:90`) ins Leere, und die Schritte laufen doch. Die Doku sagt dagegen: „Schritte, die noch kein Worker abgeholt hat …, werden beim Abholen gestoppt“ (`workflows.de.md:204-212`). Endet ein laufender Schritt erst nach Ablauf des Flags, prüft `advance()` (`workflow_orchestration.py:281`) das Flag nicht mehr und reiht die nächsten Schritte ein.
  2. **`result_ttl` unter 24 h** (Variante, geringere Konfidenz). Mit `result_ttl=60` lebt auch das Flag aus `apply()` nur 60 s. Für Task-Cancels ist genau das entschieden worden: `max(24 h, result_ttl)`, damit „ein kurzes `result_ttl` einen stornierten, länger wartenden Task nicht doch laufen lässt“ (E-020, BZ-23). Für Workflows gilt diese Regel nicht.
- **Beleg:** Zwei Tests sind rot. `test_r4m_e01_reattached_handle_cancel_flag_lives_as_long_as_the_workflow_state` liefert `86400000 == 604800000` (Reattach 24 h statt 7 d). `test_r4m_e01_apply_handle_cancel_flag_not_shorter_than_24h` liefert `60000 >= 86400000`.
- **Quelle der Erwartung:** (a) Die API-Doku sagt, ein Handle „hält nur die `workflow_id`; jeder Aufruf liest den Zustand aus Valkey, ein Handle lässt sich also in jedem Prozess neu bauen“ (`website/reference/api.de.md:785-787`). Ein neu gebautes Handle sollte sich also wie das Original verhalten. (b) Die Cancel-Zusage in `website/guides/workflows.de.md:204-212`. (c) Die gleiche Regel an anderer Stelle: E-020 / `app.cancel` (`app.py:220-232`).
- **Abgleich:** Im Entscheidungsregister gibt es keine Regel zur Lebensdauer des Workflow-Cancel-Flags. Gesucht wurde mit `grep -il cancel` über `compound-packs/swarmq-entscheidungen/`. Treffer waren E-020 (nur Task-Flag, Tests nur für `app.cancel`/`Client.cancel`) und E-040 (Fail-fast, nicht die TTL). Kein Test mit `entscheidung`-Marker hält 24 h für Reattach fest. Laut `git log -S` stammt die feste 24 h aus dem ersten Workflow-Commit f9f82c3 (U4, 2026-05-30), also aus der Zeit vor BZ-23. Bei BZ-23 wurde nur `app.cancel` angepasst. Nicht in TS-01..03.
- **Vorschlag:** Beide Handles bekommen dieselbe Flag-Lebensdauer, `max(24 h, result_ttl)` wie bei E-020. `ClientWorkflows.workflow` braucht dafür die TTL vom Aufrufer, so wie `apply` sie über `app.apply` bekommt. Wenn das nicht gewollt ist, ist es eine Produktfrage für das Register.
- **Konfidenz:** hoch, dass das Verhalten abweicht. Mittel, dass es als FEHLER eingestuft wird (Variante 1). Variante 2 ist eher LÜCKE.

### R4M-E-02 (P3): `purge_dead_letters` schließt den eigenen DLQ-Channel nicht, jeder Aufruf hinterlässt einen offenen Channel

- **Fundstelle:** `src/swarmq/broker/rabbitmq/admin.py:548-559` (`purge_dead_letters`). `_dlq_queue` (`:120-135`) öffnet seit MR 16 je Aufruf einen eigenen Channel: „Der Aufrufer schliesst ihn (`queue.channel`)“. Die Lesepfade tun das im `finally` (`_drain_dlq_with_filter`, `:256-258`), `purge_dead_letters` nicht.
- **Mutanten:** `admin.xǁRabbitMQAdminǁpurge_dead_letters__mutmut_1..15`, alle `no tests`. Kein Unit-Test ruft `purge_dead_letters` gegen die Verbindungsgrenze auf (`test_cli.py` mockt die Methode ganz).
- **Szenario:** `swarmq dlq purge` (`cli/dlq_cmd.py:253`) ist ein kurzlebiger Prozess, dort räumt das Prozessende den Channel ab. Das Dashboard `swarmv` hält dagegen eine dauerhafte Broker-Verbindung und ruft `purge_dead_letters` bei jedem „Leeren“ auf (`swarmv/src/swarmq_dashboard/dlq.py:223`, `:303-304`). Jeder Aufruf lässt einen `RobustChannel` offen, auf der Consume-Verbindung, falls offen, sonst auf der Hauptverbindung. Ein RobustChannel wird nach einem Reconnect sogar wiederhergestellt. Die Channels häufen sich bis zum Verbindungsende an und zählen gegen `channel_max`. Das ist dieselbe Leckklasse wie in Review MR 10, P0-1 (`close_channel_quietly`), hier mit sehr geringer Rate.
- **Beleg:** `test_r4m_e02_purge_closes_its_own_dlq_channel` ist rot: `Expected close to have been awaited`. Fake an der Verbindungsgrenze: `connection.channel()` liefert den Channel, `get_queue("tasks.XQ")` die Queue, die Purge-Zahl 3 kommt korrekt zurück.
- **Quelle der Erwartung:** Docstring von `_dlq_queue` („Der Aufrufer schliesst ihn“), dieselbe Regel in `_drain_dlq_with_filter` und `docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md` (Kanal-Lecks).
- **Abgleich:** Keine Entscheidung zu `purge` im Register (`grep -il purge`: kein Treffer). Nicht in TS-01..03: Das sind aiormq-Verbindungslecks, hier ist es ein Channel, den swarmq selbst öffnet. `git show 47ecffb` (MR 16, BZ-21) zeigt die Ursache: Vorher lief `_dlq_queue` über den Publish-Channel, Schließen war nicht nötig. MR 16 hat einen eigenen Channel je Aufruf eingeführt und nur die Lesepfade angepasst. Es ist also eine Regression seit 0.12.0.
- **Vorschlag:** In `purge_dead_letters` `close_channel_quietly(amqp_queue.channel)` im `finally` aufrufen, wie in `_drain_dlq_with_filter`. Nachweistest wie der Probe-Test, dazu der Fehlerpfad (`purge()` wirft, Channel wird trotzdem geschlossen).
- **Konfidenz:** hoch.

### Geprüft, ohne Fehler (Schwerpunkte aus dem Lauf-A-Bericht)

- **`connection.py:576` `_ensure_consume_channel__mutmut_6/7`:** äquivalent, siehe Abweichung 2. Die Neuanwendung des Prefetch ist auf dem heutigen Aufrufpfad doppelt. Gefährlich würde das erst bei einem zweiten Aufrufer ohne eigenes `set_qos`.
- **`connection.py:585/591` `_ensure_channel_tracked__mutmut_8/17`:** Das Original ist richtig. Kein Unit-Test öffnet einen *geschlossenen* Kanal neu bzw. stellt sicher, dass ein offener nicht ersetzt wird (Kanalleck). Das ist eine Testlücke. `__mutmut_1` (ohne Fast-Path) ist äquivalent, weil die erneute Prüfung unter der Sperre dasselbe liefert.
- **`set_publish_deadline__mutmut_7/9`:** `>=` statt `>` setzt dieselbe Frist erneut und ist damit äquivalent. `break` statt `continue` ist eine Testlücke: Nach einem abgelaufenen Deckel (`reschedule` wirft `RuntimeError` außerhalb des Zustands ENTERED) würden die übrigen nicht mehr nachgezogen.
- **`consumer_watch.py` `cancel_all__mutmut_3`, `tick__mutmut_19`:** Das Original ist richtig. Einträge mit `tag is None` entstehen real: `_reregister` hat erfolgreich abgemeldet, `_consume` ist gescheitert. Mit `break` würden in `cancel_all` alle folgenden Consumer im Drain angemeldet bleiben. Testlücke.
- **`consumer_watch.py` `_consume__mutmut_17/25/26`, `_run__mutmut_2/4`, `start__mutmut_6`:** Testlücken. Auffällig ist, dass der periodische Takt selbst ungeprüft ist. `_run(None)` bzw. `suppress(None)` würde den Wächter ohne Timer laufen lassen bzw. beim ersten Intervall beenden, und kein Unit-Test bemerkt das. Die Unit-Tests rufen `tick()` direkt auf.
- **`redeclare.py:59`:** Testlücke. Eine geschlossene Verbindung würde erst im Deklarieren scheitern, der Aufrufer wickelt das ohnehin in `BrokerConnectionError`.
- **`blocked.py`:** Rauschen. Echte aio-pika-Verbindungen haben `transport` bzw. `transport.connection`, `__mutmut_11` ist äquivalent.
- **`header_guard.py`:** Grenzwerte (`<` gegen `<=` am Limit) sind ungeprüft, das ist eine Testlücke. `frame_max // 2` gegen `/ 2` ist bei geradem `frame_max` äquivalent. `frame_max > 1` ist irrelevant (AMQP-Minimum 4096).
- **`arg_conflict.py`:** Nur Testlücken: Fristen, `suppress(None)` beim Schließen, `durable`, Texte ohne `received`/`current` in `recognized_mismatch`. Dazu Rauschen bei `log` (alle Aufrufer übergeben ein `AdoptionLog`) und `_delivery_limit_hint` (Gleichheit erzeugt keinen 406).
- **`admin.purge_dead_letters`:** R4M-E-02.
- **`connection.ping`:** Kein Unit-Test, das Original ist richtig. `health.py:136` ruft es auf, `tests/contracts/publisher_consumer.py` deckt es ab. Testlücke auf Unit-Ebene.
- **`ClientWorkflows.apply`:** Kein Fehler im Publish-Aufbau gefunden. `HEADER_ENQUEUED_AT` in Sekunden entspricht `client/schedule.py:274`, der verworfene Zustellmodus ist durch E-039 abgedeckt. Die Argumente des Schritt-Publish (`workflow_id`, `task_id`, `queue`, `kwargs`, Header) sind auf Unit-Ebene ungeprüft. Der Docstring von `tests/unit/client/test_workflows.py` verweist dafür auf die Integration. Testlücke. Besonders kritisch wären `apply__mutmut_1/2`: Mit `workflow_id = "None"` teilten sich alle Workflows einen Zustand.
- **`ClientWorkflows.workflow`:** Ohne Unit-Test. Das Original ist richtig bis auf die TTL (R4M-E-01).
- **`_mark_apply_failed__mutmut_23/25/28`:** Rauschen. Die Erstanlage setzt `version: 0` immer, ein frisch gelesener Zustand hat seit KTD11 ebenfalls immer eine Version.

---

## Teil 2: Tabelle je gesichteter Funktion

Einordnung: **F** = Fehlerverdacht, **T** = Testlücke, **R** = Rauschen. Die Zahl in Klammern ist die Anzahl der Mutanten. Mutanten-IDs sind die Suffixe `__mutmut_n`.

| Modul | Funktion | Mutanten | Einordnung | Verdacht-ID |
|---|---|---|---|---|
| `broker/rabbitmq/admin.py` | `route_via_dlx` | 5, 8, 9, 14, 19, 20 | R (6): nur der Verb-Text für Log und Fehler | – |
| | `_dlq_queue` | 2, 9 | T (2): Verbindung None/geschlossen; Channel-Leck, wenn die XQ fehlt | – |
| | `_parse_task_fields` | 10, 23, 29–32, 34–36 | R (2): `cast`-String; T (7): `task_name`, Typprüfung von id/name | – |
| | `_normalise_header_keys` | 4, 6–13 | R (2): 8, 11 (utf-8 ist Standard bzw. Groß-/Kleinschreibung egal); T (7): bytes-Schlüssel nie geprüft | – |
| | `_extract_task_id` | 1, 2 (no tests) | R (2): toter Code, kein Aufrufer | – |
| | `_drain_dlq_with_filter` | 2, 3, 5, 10, 13, 27, 29 | R (2): Defaults 2/3 (Aufrufer übergeben explizit); T (5): u. a. 27 (retry-one verschöbe alle vorher gelesenen in die Haupt-Queue) | – |
| | `_drain_dlq_messages` | 4, 6, 8, 10, 30–33, 36, 37, 39–41, 45, 48, 51, 53, 54 | R (6): 4, 10, 36, 39, 53, 54; T (12): `fail` (Fake ignoriert), Eintragsschlüssel, `is_match`/`is_malformed`, `matched` | – |
| | `_copy_then_ack` | 10, 11, 14, 17, 29, 33, 42 | R (1): 10; T (6): `PERSISTENT` der Kopie nie geprüft, Malformed-Schutz bei retry-all | – |
| | `_forward_headers` | 5, 13, 16 | R (2): 13, 16 (Header immer gestempelt); T (1): Reset `retry-count` | – |
| | `_dead_lettered_at_ms`, `_fresh_generation` | je 3 | T (2): unlesbarer Header | – |
| | `_message_ttl_s` | 8, 9 | T (2): Grenzwert TTL 0/1 ms | – |
| | `_remaining_dead_letter_ttl` | 4 | R (1): Zweig nach `_stamp_or_expire` unerreichbar | – |
| | `_stamp_or_expire` | 18, 19 | T (2): Grenzwert Rest-TTL 0 | – |
| | `list_dead_letters` | 1, 12, 13, 16, 17, 20, 21, 24, 25 | R (1): Default; T (8): Ausgabeschlüssel (CLI, Dashboard) | – |
| | `inspect_dead_letter` | 1, 3, 5, 6, 9–13 | R (1): Default; T (8): Treffer, Filter, Rückgabe | – |
| | `retry_dead_letter` | 1, 7, 12, 14, 15 | R (1); T (4): 15 meldet Erfolg ohne Treffer | – |
| | `retry_all_dead_letters` | 1, 3, 6, 10, 12 | R (1); T (4): Malformed bleibt in der XQ | – |
| | `purge_dead_letters` | 1–15 (no tests) | T (15); F: Channel wird nicht geschlossen | R4M-E-02 |
| | `_passive_message_counts`, `queue_statuses`, `_passive_declarations` | 7, 10, 13; 14, 17, 20, 24, 27, 30; 28, 31, 32 | R (10): Felder sind im echten DeclareOk immer da; T (2): Fehlertext 31/32 | – |
| | `_passive_message_count` | 1 (no tests) | R (1): toter Code | – |
| `broker/rabbitmq/arg_conflict.py` | `recognized_mismatch` | 11, 20 | T (2): Text ohne `received`/`current` | – |
| | `declare_adopting_existing` | 2, 10, 11, 16, 19, 21, 24, 25, 61, 63 | R (3): 2, 24, 25; T (7): Frist, `durable`, `suppress(None)` | – |
| | `declare_chain` | 3, 13–16, 21, 22, 27, 28, 30, 36, 38, 44 | R (4): `log` (22, 27, 38, 44); T (9): Fristen, Schließen | – |
| | `_open_probe_channel` | 1, 5, 8, 9, 11 | R (3): 1, 5, 8; T (2): 9, 11 | – |
| | `AdoptionLog.__init__`, `adopted` | 1; 29, 30 | R (1); T (2): „nur die erste Übernahme als INFO“ (Docstring) | – |
| | `_delivery_limit_hint` | 10 | R (1): Gleichheit unerreichbar | – |
| `broker/rabbitmq/blocked.py` | `unblocked_event` | 6, 11, 16 | R (3) | – |
| `broker/rabbitmq/connection.py` | `_connect_robust_closing_on_cancel` | 2, 3, 9 | T (3): Unit mockt `connect_robust`; Schließen beim Abbruch | – |
| | `__init__` | 4, 5, 7, 8, 10, 11, 25, 33, 35–38, 46 | R (6): 33, 35–38, 46; T (7): Fehlertext Konflikt-Argumente, Default `publish_timeout_s` (25: ohne Deckel) | – |
| | `_connect_owned`, `_connection_for_consume` | 10, 11; 8 | T (3): Fehlertext | – |
| | `_connect_injected` | 2, 23–29, 32, 33 | R (5): 2, 23, 24, 25, 29; T (5): 26–28 (TypeError verschluckt, Pool-Leck), 32, 33 | – |
| | `_acquire_from_pool` | 8, 9, 10 | T (3): `__aexit__` mit zwei Argumenten, Pool-Leck | – |
| | `ping` | 1, 2, 6–9, 11, 12 (no tests) | T (8) | – |
| | `_drain_pending_channel_closes` | 5, 7, 8 | T (3) | – |
| | `close` | 3, 23–25, 27 | T (5): close→connect-Wiederverwendung | – |
| | `set_publish_deadline` | 7, 9 | R (1): 7; T (1): 9 | – |
| | `_discard_publish_channel`, `_close_channel_detached` | 2, 21; 1 | T (3): Fristen, `_pending_channel_closes` wächst | – |
| | `_ensure_consume_channel` | 6, 7 | R (2): `consume()` setzt QoS danach | – |
| | `_ensure_channel_tracked` | 1, 8, 10, 17, 18, 30, 51, 62 | R (3): 1, 18, 62; T (5): Neuanlage eines geschlossenen Kanals, Fristen | – |
| | `_throwaway_channel_message_count` | 2, 7, 20, 27, 30, 33, 37–39 | R (5): 20, 27, 30, 33, 39; T (4): 2, 7, 37, 38 | – |
| `broker/rabbitmq/consumer_watch.py` | `__init__`, `start` | 11; 2, 6 | R (2): `_stopping=None`; T (1): `_run(None)` | – |
| | `stop` | 10, 11, 18 | T (3): Referenz des abgekoppelten Takts | – |
| | `cancel_all` | 3 | T (1) | – |
| | `_run` | 2, 4 | T (2): periodischer Takt ungeprüft | – |
| | `tick`, `_reregister` | 16, 19, 32, 45; 19 | T (5): `seen_on` nach Neuanmeldung, `break` | – |
| | `_consume` | 17, 24, 25, 26 | T (4) | – |
| `broker/rabbitmq/declarations.py` | `_declare_adopting` | 12, 18, 19, 24, 31, 37 | R (5): `log`; T (1): 18 Frist | – |
| | `_declare_delay_tier` | 52, 54 | R (2): Rückgabe unbenutzt | – |
| | `_declare_queue_triplet` | 45 | T (1): Stufen doppelt deklariert | – |
| | `_delay_target` | 3, 11 | R (2): `delay_queue_for` ignoriert `delay_ms` | – |
| | `invalidate_declaration` | 2 | T (1): Stufe bliebe „ready“ und unroutbar | – |
| | `redeclare_delay_tier` | 5, 9, 10, 13 | R (4): Rückfallziel nur im Log | – |
| `broker/rabbitmq/header_guard.py` | `guard_headers`, `negotiated_frame_max` | 3, 6, 13; 6 | R (2): 3, 6 (negotiated); T (2): Grenzwerte | – |
| `broker/rabbitmq/redeclare.py` | `redeclare_on_throwaway_channel` | 2 | T (1) | – |
| `client/results.py` | `get_result`, `get_progress` | 1, 2, 6, 8, 13; 2 | R (2): Defaults; T (4): `task_id` an Backend/Fehler | – |
| | `get_results` | 1, 2 | R (2): Defaults | – |
| | `run` | 1, 2, 4–10, 12, 14, 15 | R (2): Defaults; T (10): Weitergabe von Name, Argumenten, Queue, Timeout | – |
| | `_pending_oneshot_extra_ms` | 11, 12, 19 | R (2): 12, 19 (1 ms); T (1): 11 (`fire_ts_ms` None) | – |
| `client/workflows.py` | `apply` | 1, 2, 7–9, 17, 19, 20, 23–25, 30, 33, 35, 37, 39, 40, 46–48, 58–63, 65, 67, 68, 71, 72, 76, 78, 79, 84–87 | T (37); F (1): 87 | R4M-E-01 |
| | `_mark_apply_failed` | 2–4, 10–13, 23, 25, 28, 40, 42, 44, 47, 49 | R (3): 23, 25, 28; T (12) | – |
| | `workflow` | 1–15 (no tests) | T (13); F (2): 11, 15 | R4M-E-01 |
| | `_unpack` | 4, 6, 13, 15 | R (2): 4, 6; T (2): Cancel-Ergebnis ohne `task` | – |

---

## Teil 3: Verdachtsliste (Pflichtfelder)

| ID | Fundstelle | Mutanten | Szenario (kurz) | Beleg | Quelle der Erwartung | Konfidenz | Abgleich Register / TS |
|---|---|---|---|---|---|---|---|
| R4M-E-01 | `client/workflows.py:156`, `:201-206`; `app.py:238` | `workflow__mutmut_11/15`, `apply__mutmut_87` | Ein Reattach-Handle setzt das Workflow-Cancel-Flag fest auf 24 h, das Apply-Handle auf `result_ttl`. Bei `result_ttl > 24 h` laufen nach 24 h abgeholte Glieder trotz Cancel, und `advance()` reiht weiter ein. Bei `result_ttl < 24 h` gilt die BZ-23-Klasse für Workflows. | 2 rote Probe-Tests | API-Doku „Handle in jedem Prozess neu baubar“, Workflow-Guide „Abbrechen“, E-020 als gleiche Regel an anderer Stelle | hoch (Abweichung), mittel (Einstufung FEHLER) | keine Entscheidung zur Workflow-Flag-TTL; E-020 betrifft nur Task-Flags; nicht TS-01..03 |
| R4M-E-02 | `broker/rabbitmq/admin.py:548-559` | `purge_dead_letters__mutmut_1..15` | Jeder Purge lässt einen eigenen DLQ-Channel offen. Im dauerhaft verbundenen Dashboard (`swarmv`) häufen sich RobustChannels an. Regression seit MR 16 (47ecffb). | 1 roter Probe-Test | Docstring `_dlq_queue` „Der Aufrufer schliesst ihn“; gleiche Regel in `_drain_dlq_with_filter` | hoch | keine Entscheidung zu purge; nicht TS-01..03 (Channel von swarmq, keine aiormq-Verbindung) |

---

## Teil 4: Testlücken für den Fix-Plan

Nach Wirkung geordnet. Jeder Punkt ist ein Unit-Test gegen das Original, der den genannten Mutanten tötet.

1. **DLQ-Lesepfade gegen die Verbindungsgrenze statt gegen ein gemocktes `_dlq_queue`.** Die heutigen Tests in `tests/unit/broker/test_dlq_read.py` ersetzen `_dlq_queue`. Damit sind Channel-Öffnen und -Schließen, Queue-Name und `fail=False` ungeprüft (`_dlq_queue__mutmut_2/9`, `_drain_dlq_messages__mutmut_6/8`, `_drain_dlq_with_filter__mutmut_5`). Dazu kommt der Test zu R4M-E-02.
2. **`inspect_dead_letter`/`retry_dead_letter` mit nicht passender Vorgänger-Nachricht.** `_drain_dlq_messages__mutmut_45/51`, `_drain_dlq_with_filter__mutmut_27`, `inspect_dead_letter__mutmut_3–13`, `retry_dead_letter__mutmut_15`: Ein Mutant würde die erste beliebige Nachricht zurückgeben bzw. in die Haupt-Queue verschieben und für eine fehlende ID Erfolg melden.
3. **`retry_all_dead_letters` lässt nicht lesbare Bodies in der XQ.** Betrifft `_copy_then_ack__mutmut_11`, `_drain_dlq_messages__mutmut_48`, `retry_all__mutmut_6/10/12`, `_drain_dlq_with_filter__mutmut_29`.
4. **Kopien aus der DLQ sind `PERSISTENT`** (`_copy_then_ack__mutmut_14/17/29/33`), und **`dlq retry` setzt `x-swarmq-retry-count` zurück** (`_forward_headers__mutmut_5`).
5. **Ausgabeschlüssel von `list_dead_letters` und Einträge** (`body`, `body_raw`, `task_name`, `queue`, `headers`, `body_size`). CLI und Dashboard lesen sie. Mutanten: `list_dead_letters__mutmut_12–25`, `_drain_dlq_messages__mutmut_30–33`, `_parse_task_fields__mutmut_29–36`.
6. **bytes-Header-Schlüssel und ungültiges UTF-8** (`_normalise_header_keys__mutmut_4/6/7/9/10/12/13`, `body_raw` 37/40/41) sowie unlesbare DLQ-Header (`_dead_lettered_at_ms__mutmut_3`, `_fresh_generation__mutmut_3`, Grenzwerte `_message_ttl_s__mutmut_8/9`, `_stamp_or_expire__mutmut_18/19`).
7. **`ClientWorkflows.apply` prüft den Schritt-Publish:** Queue, Body (`task_id`, `args`, `kwargs`), Header `x-swarmq-workflow-id`/`-step`/`-enqueued-at`, eindeutige `workflow_id` je Aufruf, `step_tasks` im gespeicherten Zustand, `store_workflow_state(ttl_ms=…)`. Betrifft 37 Mutanten, Kernfälle `apply__mutmut_1/2/7/33/35/37/40/58–63/67`.
8. **`ClientWorkflows.workflow`:** `WorkflowNotFound` bei fehlendem Zustand, Handle mit der richtigen ID (`workflow__mutmut_1–10/12–14`). Dazu der Test zu R4M-E-01.
9. **`_mark_apply_failed`:** Fehlerobjekt (`type`, `message`), `get_workflow_state(workflow_id, op_timeout=…)` beim zweiten Versuch, CAS mit `ttl_ms` und `op_timeout` (`__mutmut_2–4/10–13/40–49`).
10. **ConsumerWatch-Takt:** Der Wächter tickt ohne `request_check` von selbst (`_run__mutmut_2/4`, `start__mutmut_6`). `cancel_all` und `tick` laufen nach einem Eintrag ohne Tag weiter (`cancel_all__mutmut_3`, `tick__mutmut_19`). Nach einer Neuanmeldung gilt `seen_on`, ein sofortiger Broker-Cancel im nächsten Takt wird erkannt (`tick__mutmut_16/32/45`, `_reregister__mutmut_19`, `_consume__mutmut_24`). `missing_ticks` startet bei 0 (`_consume__mutmut_25/26`). Eine späte Antwort während `stop` bei noch geführtem Eintrag wird abgemeldet (`_consume__mutmut_17`). Der abgekoppelte Takt bleibt referenziert (`stop__mutmut_10/11`).
11. **Kanal-Neuanlage in `RabbitMQConnection`:** Ein geschlossener Publish- oder Consume-Kanal wird neu geöffnet, ein offener nicht ersetzt (`_ensure_channel_tracked__mutmut_8/10/17`). Fristen bei `ready()`/`channel()` (`__mutmut_30/51`).
12. **Publish-Deadline und Kanalverwurf:** Nachziehen nach einem abgelaufenen Deckel (`set_publish_deadline__mutmut_9`). `_pending_channel_closes` schrumpft nach dem Schließen (`_discard_publish_channel__mutmut_21`). Hängende Close-Tasks werden abgebrochen (`_drain_pending_channel_closes__mutmut_5/7/8`). Fristen (`_discard_publish_channel__mutmut_2`, `_close_channel_detached__mutmut_1`).
13. **Pool-Freigabe mit drei Argumenten:** Ein Fake mit echter `__aexit__`-Signatur (`_acquire_from_pool__mutmut_8–10`, `_connect_injected__mutmut_26–28`, `close__mutmut_23–25`). Heute verschlucken die `except Exception`-Zweige einen `TypeError`.
14. **close→connect auf dem URL-Pfad** (`close__mutmut_3/27`) und Default `publish_timeout_s` (`__init____mutmut_25`).
15. **`ping` auf Unit-Ebene:** nicht verbunden, Timeout, Fehler-Wrapping (`ping__mutmut_1–12`).
16. **`invalidate_declaration` einer Delay-Stufe** nimmt sie aus „ready“ (`__mutmut_2`). Ohne das bliebe eine abgelehnte Stufe Ziel jeder Rückstellung.
17. **`arg_conflict`:** Schließen des Probe-Channels, wenn `close()` wirft (`suppress(None)`: `declare_adopting_existing__mutmut_61`, `declare_chain__mutmut_13/28`, `_open_probe_channel__mutmut_9`). Fristen (`__mutmut_11/63`, `declare_chain__mutmut_14–16/30`). `durable=True` (`__mutmut_16/19/21`). `recognized_mismatch` mit Text ohne `received`/`current` (`__mutmut_11/20`). Log-Ebene der zweiten Übernahme (`adopted__mutmut_29/30`).
18. **`header_guard` an den Grenzwerten** `size == limit` (`guard_headers__mutmut_6/13`). **`redeclare` bei geschlossener Verbindung** (`__mutmut_2`). **`_declare_queue_triplet`** deklariert bereite Stufen nicht erneut (`__mutmut_45`).
19. **`ClientResults.run`/`get_result`/`get_progress` reichen ihre Argumente weiter** (`run__mutmut_4–15`, `get_result__mutmut_6/8/13`, `get_progress__mutmut_2`). **`_pending_oneshot_extra_ms`** ohne `fire_ts_ms` (`__mutmut_11`). **`_unpack`** mit Cancel-Ergebnis ohne `task` (`__mutmut_13/15`).
20. **Fehlertexte von `BrokerConnectionError`** (niedrige Priorität): `_connect_owned__mutmut_10/11`, `_connection_for_consume__mutmut_8`, `_connect_injected__mutmut_32/33`, `_throwaway_channel_message_count__mutmut_37/38`, `_passive_declarations__mutmut_31/32`, `__init____mutmut_4–11`.

Nebenbefund zum Aufräumen (kein Verdacht): `RabbitMQAdmin._extract_task_id` und `_passive_message_count` haben keinen Aufrufer mehr (toter Code, 3 Mutanten `no tests`).

---

## Teil 5: Zusammenfassung „Fehler / Testlücke / Rauschen“ je Modul

| Modul | Mutanten | Fehler (Verdacht, Mutanten) | Testlücke | Rauschen |
|---|---|---|---|---|
| `broker/rabbitmq/admin.py` | 126 | R4M-E-02 (an `purge_dead_letters`, 15 Mutanten dort als Testlücke gezählt) | 87 | 39 |
| `broker/rabbitmq/arg_conflict.py` | 34 | – | 22 | 12 |
| `broker/rabbitmq/blocked.py` | 3 | – | 0 | 3 |
| `broker/rabbitmq/connection.py` | 72 | – | 50 | 22 |
| `broker/rabbitmq/consumer_watch.py` | 18 | – | 16 | 2 |
| `broker/rabbitmq/declarations.py` | 16 | – | 3 | 13 |
| `broker/rabbitmq/header_guard.py` | 4 | – | 2 | 2 |
| `broker/rabbitmq/redeclare.py` | 1 | – | 1 | 0 |
| `client/results.py` | 23 | – | 15 | 8 |
| `client/workflows.py` | 72 | R4M-E-01 (3 Mutanten) | 64 | 5 |
| **Summe** | **369** | **2 Verdachte (3 Mutanten zugeordnet)** | **260** | **106** |

Die Mutationen zeigen überwiegend Testlücken auf Unit-Ebene. Die meisten liegen an der Verbindungsgrenze, die heute gemockt wird: `_dlq_queue`, `connect_robust`, Pool-`__aexit__`. Beide Fehlerverdachte kamen nicht aus der Mutation selbst. Sie fielen beim Lesen der ungeprüften Funktionen auf (`workflow()` ohne Test, `purge_dead_letters` ohne Test).
