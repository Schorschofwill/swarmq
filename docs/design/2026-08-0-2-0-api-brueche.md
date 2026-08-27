# 0.2.0 — Bruch-Inventar und Migrationsentwurf (Plan U14)

Inventar aller API-Brüche des 0.2.0-Releases nach KTD16, entworfen **vor**
der Umsetzung (U15 setzt um, U16 released). Public im Sinne von KTD16 sind:
die eager- und lazy-Oberfläche von `swarmq/__init__.py`, CLI-Kommandos,
-Flags und -Exit-Codes, `SWARMQ_*`-Umgebungsvariablen, die Wire-Formate
(Task-Message, Result-Envelope, `x-swarmq-*`-Header), das Valkey-Key-Layout
und die Agent-Spec-Artefakte. Interne Modulpfade und die
`swarmq.cli`-Façade-Re-Exports sind nicht public; ihre Änderung steht
trotzdem im Changelog, weil die Surface-Whitelist sie heute pinnt.

Jeder Eintrag nennt: alt → neu, Migrationsschritt, und ob sich Verhalten
oder nur ein Name ändert. Die einzige Verhaltensänderung des gesamten
Programms ist der Band-Erhalt beim Retry (KTD2, Abschnitt V1).

---

## A. Public-Brüche (Migrationshinweis erforderlich)

### A1. `ValkeyBackend.create_pubsub()` entfällt

- **Alt:** `pubsub = backend.create_pubsub()` gab das rohe
  `valkey.asyncio.client.PubSub`-Objekt heraus.
- **Neu:** ersatzlos gestrichen. Das Warten auf Ergebnisse/Fortschritt
  läuft über die Vertragsmethoden `backend.wait_for_result(task_id,
  timeout=…)` und `backend.watch_progress(task_id)` (seit U17); Workflows
  über `backend.wait_for_workflow(wf_id, timeout=…)`.
- **Migration:** Aufrufer, die eigenes Pub/Sub auf den
  Benachrichtigungs-Kanälen gebaut haben, stellen auf die genannten
  Vertragsmethoden um. Wer wirklich rohes Pub/Sub braucht, nutzt seinen
  eigenen `valkey`-Client — die Kanalnamen (`sq:notify:…`) sind Teil des
  Key-Layouts und bleiben stabil (Golden-Fixture `channels.txt`).
- **Verhalten oder Name:** Name/Oberfläche; kein Verhaltenswechsel.

### A2. `ValkeyBackend`-Methoden-Aliase entfallen (KTD10)

Alle in U17/U18 angelegten Weiterleitungen der heutigen Methodennamen
werden entfernt; es bleiben die Vertragsnamen bzw. die Sub-Store-Objekte.
Nur relevant für Nutzer, die `ValkeyBackend` direkt ansprechen (die
`SwarmQ`-/`Client`-/`Worker`-Oberfläche ist nicht betroffen).

| Alt (Alias) | Neu |
|---|---|
| `setnx_unique(hash, owner, ttl_ms)` | `claim_unique(hash, owner, ttl_ms)` |
| `get_results_batch(ids)` | `get_results(ids)` (Vertragsname) |
| `publish_notification(task_id)` | `notify_result(task_id)` |
| `publish_workflow_notification(wf_id)` | `notify_workflow(wf_id)` |
| `subscribe_and_wait(channel, timeout)` | `wait_for_result(task_id, timeout=…)` / `wait_for_workflow(wf_id, timeout=…)` |
| `zadd_schedule` / `zrem_schedule` | `backend.schedules.add` / `.remove` (Bytes-Vertrag) |
| `zrangebyscore_schedules(max, limit)` | `backend.schedules.poll_due(now_ms, limit=…)` |
| `hset_schedule_def` / `hsetnx_schedule_def` | `backend.schedules.put` / `.add` |
| `hget_schedule_def` / `hdel_schedule_def` / `hgetall_schedule_defs` | `backend.schedules.get` / `.remove` / `.list_all` |
| `set_schedfire_dedup` / `set_schedfire_inflight` / `clear_schedfire_inflight` / `is_schedfire_completed` | `backend.schedules.mark_fire_completed` / `.claim_fire` / `.release_fire_claim` / `.is_fire_completed` |
| `set_schedfire_handover_marker` / `get_and_del_schedfire_handover_marker` | `backend.schedules.set_handover_marker` / `.take_handover_marker` |

- **Migration:** Namen 1:1 ersetzen (rechte Spalte); Semantik, Key-Layout
  und Lua-Verträge sind identisch (Golden-Fixtures, KTD12).
- **Verhalten oder Name:** nur Namen.
- **Entscheidung zur Marker-Rückgabe:** `take_handover_marker` liefert
  `bool` (wie heute die Fassade); der Vertrag in
  `contracts/coordination.py` ist bereits so geschnitten.

### A3. `RabbitMQBroker`-Admin-Aliase entfallen (KTD10)

| Alt (Alias) | Neu (Vertragsname) |
|---|---|
| `list_dead_letter_queue(queue, limit)` | `list_dead_letters(queue, limit)` |
| `get_queue_depth(queue)` | `queue_depth(queue)` |
| `get_dlq_depth(queue)` | `dead_letter_depth(queue)` |
| `nack(delivery, requeue=True)` | `redeliver(delivery)` |
| `nack(delivery, requeue=False)` | `route_via_dlx(delivery)` |
| `publish_to_delay_queue(…)` | `retry_later(delivery, delay_ms=…, headers=…)` |
| `publish_to_dlq(…)` | `dead_letter(delivery, headers=…)` |

- **Migration:** Namen ersetzen. `retry_later`/`dead_letter` bestätigen
  das Original selbst (kein separates `ack` mehr nötig — wer das alte
  Muster `publish_to_*` + `ack` nutzte, entfernt das `ack`).
- **Verhalten oder Name:** Namen; die Verben kapseln das bisherige
  Publish+Ack-Muster. Zum Zielband der Verben siehe V1.

### A4. Konstruktoren gegen Verträge typisiert

- **Alt:** `Worker(broker: RabbitMQBroker, backend: ValkeyBackend, …)`,
  `Client(broker: RabbitMQBroker, backend: ValkeyBackend, …)`.
- **Neu:** Signaturen nennen die Verträge (`Broker`/`Publisher`,
  `ResultBackend` aus `swarmq.contracts`); `RabbitMQBroker`/
  `ValkeyBackend` implementieren sie und bleiben die ausgelieferten
  Implementierungen.
- **Migration:** keine für normale Nutzer — bestehender Code läuft
  unverändert. Nur eigene Typ-Annotationen, die exakt die alten
  Konstruktor-Typen spiegeln, werden weiter (oder auf die Verträge)
  gestellt.
- **Verhalten oder Name:** nur Typen.

---

## B. Interne Brüche (Changelog-Satz genügt, KTD16)

- **B1. Klassen-Re-Exports der Alias-Module entfallen:**
  `swarmq.locking.LockManager`, `swarmq.limits.RateLimiter`,
  `swarmq.workflow_orchestration.ChordManager`/`chord_set_key` → kanonisch
  `swarmq.backend.valkey.{locks,rate_limit,chords}`. Die PEP-562-
  `__getattr__`-Weiterleitungen und die zwei Guard-Allowlist-Zyklen
  (`locking ↔ backend.valkey.locks`, `limits ↔ backend.valkey.rate_limit`)
  werden gestrichen; `locking`/`limits` sind danach reine Modelle.
- **B2. `swarmq.scheduler.ScheduleStore` bleibt** die definitions-typisierte
  Klasse (Entscheidung aus U18 bestätigt): sie ist die von CLI und Tests
  genutzte API und delegiert an den Bytes-Vertrag `backend.schedules`.
  Die im Plan erwogene Alias-Gleichheit `ScheduleStore is
  ValkeyScheduleStore` wird **nicht** umgesetzt — zwei Schichten, zwei
  Zwecke (Definitions-API vs. Bytes-Vertrag). Kein Bruch; Changelog-Satz
  zur Klarstellung.
- **B3. CLI-Surface-Whitelist:** Die ambienten Re-Exports (`Path`,
  `Sequence`, `Any`, `TYPE_CHECKING`, Exceptions …) der `swarmq.cli`-Façade
  werden in U15 **nicht** getrimmt und `_DLQNotFound` **nicht** umbenannt —
  die Sunset-Entscheidung aus
  `docs/solutions/patterns/sunset-cli-surface-whitelist-kp2-2026-12-31.md`
  (Frist 2026-12-31) bleibt bestehen; 0.2.0 zieht sie nicht vor, weil kein
  Konsumenten-Inventar vorliegt. Die Whitelist selbst bleibt der Pin.
- **B4. Interne Modulpfade:** `swarmq.worker` (Paket statt Modul, seit U6),
  `swarmq.client`, `swarmq.scheduler`, `swarmq.process_manager` sind
  Pakete; wer interne Pfade (`swarmq.worker.Worker` bleibt gültig) oder
  Patch-Ziele nutzt, findet die Logik in den Submodulen
  (`worker/pipeline.py`, `client/schedule.py`, …). Changelog-Satz.
- **B5. Dead-Letter-Routing:** nach KTD16 nicht public; die U8-Umstellung
  der Worker-Pfade auf die Vertragsverben ist ein Changelog-Satz (siehe
  auch V1 für die eine Verhaltenskorrektur).

---

## C. Entscheidung Dead-Letter-Fallbacks und `x-death` (KTD3)

**Entscheidung: keine Vereinheitlichung in 0.2.0; Wiedervorlage als
eigenes Follow-Up nach 0.2.0 (Kandidat 0.3.x), nur mit eigenem
Chaos-Nachweis je Pfad.**

Begründung: Die Fallbacks sind je Aufrufstelle absichtlich verschieden —
Retry-/Expiry-Publish-Fehler → `redeliver` (Nachricht bleibt im Spiel),
Lock-Pfade → `route_via_dlx` (Broker-seitiges Dead-Lettering ohne
Diagnose-Header). Eine Vereinheitlichung ist nur unter Broker-Fehlern
beobachtbar, die e2e-Suite sieht sie nicht (Risiko „stiller
Verhaltenswechsel", Plan-Risikotabelle); die Characterization
`tests/integration/broker/test_rabbitmq_fallbacks.py` pinnt den Status
quo. **`x-death`-Differenz:** über `route_via_dlx` geroutete (nicht
dekodierbare/unbekannte) Nachrichten tragen RabbitMQs `x-death`-Header
statt der `x-swarmq-*`-Diagnose-Header; das bleibt in 0.2.0 so und ist im
Betriebs-Playbook (`swarmq dlq inspect`) als Unterscheidungsmerkmal
dokumentiert — beide Header-Familien bleiben lesbar.

---

## D. Golden-Fixtures / Wire-Kompatibilität (KTD12)

**Keine Abweichungen.** Key-Layout (`keys.json`, `values/`), Kanäle
(`channels.txt`), Queue-Argumente (`queue_args.json`), Header
(`headers.txt`) und Lua-Verträge (`lua_contract.txt`) sind gegenüber
0.1.2 unverändert; die einzige Fixture-Regenerierung (U7, `headers.txt`)
entfernte ein nie emittiertes Kommentar-Literal aus dem Scan — kein
Wire-Bruch. Wire-Formate und `SWARMQ_*`-Umgebungsvariablen: unverändert.

---

## V. Verhaltenskorrekturen

### V1. Band-Erhalt beim Retry (KTD2, umgesetzt in U8)

- **Alt:** Retry-, Rate-Limit- und Lock-Rückstellungen publizierten an die
  im Nachrichten-Body genannte Queue. Eine von einem Fremd-Producer in ein
  Prioritätsband (`tasks.high`) gelegte Nachricht, deren Body die
  Basis-Queue nennt, verlor beim ersten Retry ihr Band.
- **Neu:** `retry_later`/`dead_letter` zielen auf das Verzögerungs- bzw.
  Dead-Letter-Ziel der **Zustellungs-Queue** (`delivery.queue`) — ein Retry
  aus `tasks.high` bleibt in `tasks.high`.
- **Migration:** keine; das ist die dokumentierte Korrektur eines
  Band-Verlusts. Wer sich auf das alte (fehlerhafte) Umrouten verlassen
  hat, publiziert Retries explizit um.
- **Absicherung:** `tests/e2e/test_retry_band.py`.

---

## E. Doku-Änderungen in U15 (Entwurfsstand)

- `website/reference/changelog.md` / `.de.md`: 0.2.0-Abschnitt — Entwurf
  liegt bereits im `[Unreleased]`-Block beider Dateien (dieser MR).
- `website/reference/api.md` / `.de.md`: Abschnitt „`class Worker` /
  `class Client`" erhält die Vertrags-Signaturen (A4); die Blackbox-Absätze
  zu `RabbitMQBroker`/`ValkeyBackend` bleiben, ergänzt um einen Satz zu den
  Verträgen in `swarmq.contracts`. Keine Alias-Methoden dokumentiert —
  kein weiterer Änderungsbedarf.
- `website/reference/configuration*.md`, `cli*.md`: keine inhaltlichen
  Brüche (Flags, Env-Vars, Exit-Codes unverändert) — nur Regeneration in
  U15, falls die Agent-Spec sie erzeugt.
- Agent-Spec-Artefakte: in U15 regenerieren (`agent_spec.py`), danach
  Abgleich gegen dieses Inventar.

## Prüfliste (U14-Verification)

- Jeder in U17/U18 angelegte Fassaden-Alias steht in A2/A3. ✔
- Jeder KTD10-Klassen-Re-Export steht in B1/B2. ✔
- `create_pubsub` hat einen Migrationshinweis (A1). ✔
- Surface-Whitelist/Sunset ist entschieden (B3). ✔
- KTD3-Entscheidung inklusive `x-death` ist getroffen (C). ✔
- Golden-Abweichungen: keine (D). ✔
- Einzige Verhaltensänderung ist die benannte KTD2-Ausnahme (V1). ✔
