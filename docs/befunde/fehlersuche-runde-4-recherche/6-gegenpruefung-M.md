# Gegenprüfung M – Mutation, Gruppen E und F (R4M-E-01/02, R4M-F-01/02/03)

Einheit U17, Spur M. Worktree `swarmq-fehlersuche4`, Branch `test/fehlersuche-runde-4` @ e92699f. Nur gelesen, nichts unter `src/` oder `tests/` geändert. Ablauf nach `docs/befunde/README.md`: Entscheidungs-Abgleich (Register `compound-packs/swarmq-entscheidungen/`, Abschnitte „Entscheidungen“ der Befundregister, `docs/plans/`, `docs/solutions/`, `website/`, Changelog, Code-Kommentare, `git log -S`, Tests mit `entscheidung`-Marker), Quelle der Erwartung, Realismus.

**Probe-Tests selbst ausgeführt** (ohne Dienste, `-p no:randomly -n0`):

```
PYTHONPATH=. uv run --extra test pytest -p no:randomly -n0 --rootdir . -c pyproject.toml \
  <scratchpad>/u5e/test_mut_e_nachweise.py <scratchpad>/u5f/proben/
→ 7 failed, 16 passed in 0.34s
```

`<scratchpad>` = `/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/608b3617-a1bc-47f1-bd40-e763775cafe6/scratchpad`. Rot sind genau die sieben Verdachtsproben (3 × E, 4 × F), jede an der fachlichen Assertion:

| Probe | Assertion | Ergebnis |
|---|---|---|
| `u5e/…::test_r4m_e01_reattached_handle_cancel_flag_lives_as_long_as_the_workflow_state` | `ttl_reattached == ttl_original` | `86400000 == 604800000` |
| `u5e/…::test_r4m_e01_apply_handle_cancel_flag_not_shorter_than_24h` | `ttl_ms >= 86_400_000` | `60000 >= 86400000` |
| `u5e/…::test_r4m_e02_purge_closes_its_own_dlq_channel` | `channel.close.assert_awaited()` (Purge-Zahl 3 und `get_queue("tasks.XQ")` vorher grün) | `Expected close to have been awaited` |
| `u5f/proben/test_probe_dlq_truncated_expired.py::test_list_reports_truncation_when_expired_entries_used_up_the_limit` | `queue.get.await_count == 2` (grün), dann `truncated is True` | `scanned: 1, limit: 2, truncated: False` |
| `u5f/proben/test_probe_schedule_add_orphan_claim.py::test_rejected_add_leaves_no_dedup_claim` | `inserted is False` (grün), dann kein Claim-Owner | Owner `'nightly'` |
| `…::test_following_the_hint_lets_the_add_succeed` | `inserted is True` nach `remove --permanent` | `inserted: False, duplicate_of: 'nightly'` |
| `…::test_dedup_rejection_does_not_claim_the_name_exists` | `duplicate_of == "first"`, `inserted is False` (grün), dann `already_exists is False` | `already_exists: True` |

Die Proben prüfen Verhalten an der öffentlichen bzw. CLI-Grenze (Handle-`cancel()`, `purge_dead_letters`, `_run_dlq_list`, `_run_schedule_add`), mocken nur I/O (Backend-/Broker-Fakes, In-Memory-Backend aus `tests/support/inmemory/`). Der Fake in `test_r4m_e02` bildet die echte aio-pika-API nach (`Queue.channel`, `Channel.get_queue`, `Queue.purge() → DeclareOk.message_count`).

## Ergebnis auf einen Blick

| Verdacht | Einstufung | Prio | Kern der Begründung |
|---|---|---|---|
| R4M-E-01 (Variante 1: Reattach-Handle 24 h) | **FEHLER** | P3 | Plan 2026-05-30 `:104` „Cancel-Flag … TTL = `config.result_ttl`“; Kommentar `client/workflows.py:42-45`; `api.md:761-763` „handle can be rebuilt in any process“ |
| R4M-E-01 (Variante 2: `result_ttl` < 24 h) | abgetrennt als **ENTSCHEIDUNG** („überdenken?“) | – | Derselbe Plan legt TTL = `result_ttl` fest; neues Argument: E-020/BZ-23 |
| R4M-E-02 | **FEHLER** | P3 | Docstring `_dlq_queue` „Der Aufrufer schliesst ihn“; Regression aus 47ecffb (MR 16) |
| R4M-F-01 | **LÜCKE** | P3 | Keine Zusage über den Claim; E-059-Nachtrag regelt den Nachbarpfad (Client) mit „Claim bleibt stehen“; Produktfrage |
| R4M-F-02 | **LÜCKE** | P4 | `already_exists` nirgends dokumentiert, aus 8971cbc ohne Duplikat-Fall |
| R4M-F-03 | **FEHLER** | P3 | `cli.md:430-431` und `dead-letter-queues.md:131-132` „`truncated` when `--limit` is hit“; Docstring `list_dead_letters` „stopped at limit“ |

Bezug zu den zwölf offenen LÜCKEN aus Plan R4 (R4): Nur R4M-E-01 berührt eine, **FS-32** (siehe dort). R4M-F-01 berührt keine der zwölf, aber den E-059-Nachtrag (Review U15). Die übrigen berühren keine.

---

## R4M-E-01 – Workflow-Cancel-Flag: Reattach-Handle fest 24 h, Apply-Handle `result_ttl`

### Einstufung

- **Variante 1 (Reattach über `app.workflow(id)` setzt das Flag fest 24 h, auch bei `result_ttl` > 24 h): FEHLER, P3.** Konfidenz hoch.
- **Variante 2 (Flag lebt nur `result_ttl`, auch wenn das < 24 h ist): ENTSCHEIDUNG** des Plans, mit „Entscheidung überdenken?“ und neuem Argument. Vorschlag: im Register als eigener Punkt R4M-E-01b führen, damit jede Zeile genau eine Einstufung hat.

### Entscheidungs-Abgleich

- **E-020** (`e020-cancel-flag-lebensdauer.md`): `applies_when` nennt nur „cancel() für einen verzögerten oder laufenden Task“, `client/results.cancel`, `clear_cancel_flag`, das Cancel-Gate; Tags `[cancel, ttl, client, bz-23]`; alle Tests betreffen `app.cancel`/`Client.cancel`/`clear_cancel_flag`. Für das Workflow-Flag (`sq:wf:cancel:{id}`, `set_workflow_cancel_flag`) gilt E-020 **nicht**. Es ist die gleiche Regel an anderer Stelle, kein Beschluss für Workflows.
- **E-040** regelt Fail-fast, nicht die TTL. Sein Nachtrag sagt nur „Der Workflow-Cancel-Zweig bleibt unverändert (FS-32)“.
- **Plan** `docs/plans/2026-05-30-001-feat-workflow-primitives-plan.md:104` (Key Technical Decisions): „Workflow-State + Cancel-Flag im `sq:wf:*`-Namespace: … `sq:wf:cancel:{id}` (`SET NX PX`/`EXISTS` wie bestehende Cancel-Flags). **TTL = `config.result_ttl`** (Ceiling-Division)“. Das ist die maßgebliche Festlegung: Das Flag soll `result_ttl` leben, gleich ob das Handle aus `apply()` oder aus dem Reattach kommt.
- **Code-Kommentar** an der Konstante, `src/swarmq/client/workflows.py:42-45`: „24h matches the result-TTL default; app.apply() passes config.result_ttl * 1000 through.“ Die 24 h sind als Ersatzwert für den Default gemeint. Im Reattach-Pfad wurde die TTL nur nicht durchgereicht.
- **Historie:** `git log -S"cancel_ttl_ms=_DEFAULT_WORKFLOW_TTL_MS"` → f9f82c3 (U4, 2026-05-30, „Client.workflow(id) reattaches“) und 778ac40 (U10, Paketumbau, verbatim). Die Commit-Nachricht begründet die 24 h nicht. BZ-23 (E-020) hat später nur `app.cancel` angehoben.
- **Tests:** Kein Test hält die Reattach-TTL fest. `grep cancel_ttl_ms tests` findet nur Handles mit Testwerten (1000, 60 000). Kein `entscheidung`-Marker im Workflow-Handle-Umfeld außer E-030 (`get()`-Polling) und E-039 (FAF-Schritt), beide ohne Bezug.
- **Doku:** `website/guides/workflows.md` und `api.md` nennen die Lebensdauer des Workflow-Flags nicht. `workflows.md:169-171`: „The workflow state lives for `result_ttl` … Set `result_ttl` high enough to cover the whole workflow including retries.“

### Quelle der Erwartung (Variante 1)

1. Plan `:104` „TTL = `config.result_ttl`“ (KTD) und der Kommentar `workflows.py:42-45`.
2. `website/reference/api.md:761-763`: „Holds only the `workflow_id`; every call reads the state from Valkey, so a handle can be rebuilt in any process.“ Ein neu gebautes Handle soll sich wie das Original verhalten.
3. Cancel-Zusage `website/guides/workflows.md:199-208` bzw. `api.md:779-781`: „steps not yet picked up finish as cancelled; a step that is already running completes, but no further steps are enqueued“.

### Realismus und Widerlegungsversuche

- *„Reattach kennt `result_ttl` des anwendenden Prozesses nicht.“* Trägt nicht: `SwarmQ.workflow` hat `self.config.result_ttl`, so wie `SwarmQ.apply`. Notfalls ließe sich die Rest-TTL des Zustands-Keys lesen. Ein Grund für fest 24 h ist nirgends genannt.
- *„Der Zustand läuft ohnehin ab.“* Nein: Bei `result_ttl = 7 d` lebt der Zustand 7 d, das Flag aus dem Reattach-Handle 24 h. Danach sieht `advance()` (`workflow_orchestration.py:281`) kein Flag mehr und reiht weiter ein. Das Cancel-Gate (`worker/gates/cancel.py:90`) lässt noch wartende Group-/Chord-Glieder laufen.
- *Auslöser:* `result_ttl` > 24 h (laut Doku genau der Rat für lange Workflows), Abbruch aus einem anderen Prozess (Reattach ist der dokumentierte Weg: Dashboard, Betreiber-Skript) und ein Schritt, der nach dem Cancel noch länger als 24 h läuft oder wartet (Rückstau, Retry über die Delay-Queue, langer Vorgänger-Schritt). Das ist selten, aber gerade der Anwendungsfall langer Workflows. P3 ist angemessen.
- **Verschärfung durch FS-32 (offene LÜCKE):** Nach `handle.cancel()` bleibt `status()` auf `running`, solange kein Schritt mehr fertig wird. `get()` erkennt den Abbruch nur am Flag (`workflow_orchestration.py:173-180`). Läuft das Flag nach 24 h ab, während der Zustand noch 6 Tage lebt, ist der Abbruch ganz verschwunden: `status()` meldet `running`, `get()` läuft in `ResultNotReadyError` statt `TaskCancelledError`. Ein Fix von FS-32 (Zustand `cancelled` schreiben) würde diesen Teil heilen, den Weiterlauf der Schritte aber nicht.

### Variante 2: ENTSCHEIDUNG, „Entscheidung überdenken?“

Mit `result_ttl = 60` lebt das Flag aus `apply()` 60 s. Das entspricht dem Plan (`:104`, TTL = `result_ttl`). Weil auch der Zustand nach `result_ttl` abläuft, endet die Kette über `workflow_state_missing`, und die Doku verlangt ein ausreichend großes `result_ttl` (`workflows.md:169-171`). **Neues Argument:** Für Task-Flags wurde genau diese Kopplung mit BZ-23 als Fehler erkannt und auf `max(24 h, result_ttl)` angehoben, weil „ein kurzes `result_ttl` einen stornierten, länger wartenden Task nicht doch laufen lassen“ soll (E-020, `app.py:220-232`). Bei Workflows laufen die schon eingereihten Group-/Chord-Glieder nach Ablauf des Flags beim Abholen doch, mit Nebenwirkungen (ihr `advance` endet dann still).

### Vorschlag Register-Zeile

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| F4-nn (R4M-E-01) | P3 | Ein per `app.workflow(id)` neu gebautes `WorkflowHandle` setzt das Workflow-Cancel-Flag fest auf 24 h, das Handle aus `apply()` auf `result_ttl`. Bei `result_ttl` > 24 h laufen nach Ablauf des Flags wartende Group-/Chord-Glieder doch, `advance()` reiht weiter ein, und `get()` meldet den Abbruch nicht mehr (zusammen mit FS-32). Quelle: Plan 2026-05-30 `:104`, `workflows.py:42-45`, `api.md:761-763`. | Code: `SwarmQ.workflow` reicht `config.result_ttl * 1000` an `Client.workflow(…, cancel_ttl_ms=…)` durch (optional, Default wie heute für den reinen Client). Nachweis: Variante-1-Probe als `@pytest.mark.befund` neben `tests/unit/client/test_workflows.py`. |
| (R4M-E-01b) | – | ENTSCHEIDUNG (Plan `:104`): Workflow-Cancel-Flag lebt `result_ttl`, auch < 24 h. Überdenken mit dem Argument aus E-020/BZ-23. | Produktfrage (siehe unten). Kein Nachweis-Test, solange die Regel gilt. |

**Produktfrage zu R4M-E-01b.** Optionen: (a) `max(24 h, result_ttl)` wie E-020 auch für Workflow-Flags (dann fallen beide Varianten zusammen, Fix an einer Stelle); (b) bei `result_ttl` bleiben und die Lebensdauer des Workflow-Flags in `workflows.md` („Cancelling“) dokumentieren. **Empfehlung: (a).** Gleiche Regel für beide Flag-Arten, kein Mehraufwand gegenüber dem Fix von Variante 1, und dieselbe Begründung wie BZ-23. Das Ergebnis gehört als Regel ins Register (Nachtrag zu E-020 oder neue E-nnn).

---

## R4M-E-02 – `purge_dead_letters` schließt seinen DLQ-Channel nicht

### Einstufung: **FEHLER, P3** (Konfidenz hoch für den Mechanismus, Wirkung heute gering)

### Entscheidungs-Abgleich

- Register: `grep -il purge compound-packs/swarmq-entscheidungen/*.md`: kein Treffer. E-028 (DLQ nicht zerstörend) regelt `list`/`inspect`, nicht `purge` und nicht den Channel-Lebenszyklus.
- `docs/solutions/`: Treffer zu `purge` nur in `aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md` (Testszenario-Purge, ohne Bezug).
- Historie: `git show 47ecffb` (MR 16, BZ-21, 2026-10-11). Vorher: `pub = await self._ensure_publish_channel(); return await pub.get_queue(…)`, also der langlebige Publish-Channel, ohne Schließen. Nachher öffnet `_dlq_queue` je Aufruf `connection.channel()`, und der neue Docstring sagt „Der Aufrufer schliesst ihn (`queue.channel`)“. MR 16 hat `_drain_dlq_with_filter` angepasst (`finally: close_channel_quietly(queue.channel)`, `admin.py:256-257`), `purge_dead_letters` (`admin.py:548-559`) nicht. Die Commit-Nachricht spricht nur von „dlq list/inspect/retry“. Das ist eine **Regression seit 0.12.0**, kein Beschluss.
- Tests: `tests/unit/test_cli.py` mockt `purge_dead_letters` ganz. `tests/integration/broker/test_rabbitmq_dlq.py:399-414` prüft nur die Zahl. Kein `entscheidung`-Marker.

### Quelle der Erwartung

Docstring `_dlq_queue` (`admin.py:121-126`): „Der Aufrufer schliesst ihn (`queue.channel`)“; dieselbe Regel in `_drain_dlq_with_filter` (`admin.py:254-257`, „Was noch unbestaetigt ist, gibt der Broker zurueck“). `purge_dead_letters` ist öffentlicher Vertrag (`contracts/broker.py:138`, `BrokerAdmin`).

### Realismus und Widerlegungsversuche

- *„Ein RobustChannel wird beim Reconnect wiederhergestellt“* (Sichtung): nur solange eine Python-Referenz besteht. `aio_pika/robust_connection.py:67` führt Channels in einem `WeakSet`. Auf AMQP-Ebene bleibt der Channel aber offen: aiormq hält ihn stark (`aiormq/connection.py:958`, `self.channels[channel_number] = channel`), bis die Verbindung endet. Jeder Purge belegt also eine Channel-Nummer gegen `channel_max`. Die Kernaussage hält, das Detail zur Wiederherstellung ist abzuschwächen.
- *CLI:* `swarmq dlq purge` ist kurzlebig und ruft `broker.close()` (`dlq_cmd.py:250-268`). Dort entsteht kein Leck.
- *Dashboard swarmv:* Der Aufrufpfad existiert (`swarmv/src/swarmq_dashboard/web/routes/dlq.py:308` → `dlq.py:223` → `dlq.py:303-304`, dauerhafte Verbindung über `_verbunden()`). **Aber swarmv pinnt `swarmq>=0.7,<0.8`** (`swarmv/pyproject.toml:17`, `uv.lock`: 0.7.0). Dort läuft noch der alte Pfad über den Publish-Channel. Heute ist kein bekannter Aufrufer betroffen. Betroffen wird swarmv mit dem offenen Umstieg auf 0.12 (Notiz „offen rc2, Staging, swarmv“), dazu jede eingebettete Nutzung von `BrokerAdmin.purge_dead_letters` in einem langlebigen Prozess.
- *Rate:* ein Channel je manuellem „Leeren“. Bis zum RabbitMQ-Standard `channel_max` 2047 braucht es sehr viele Purges ohne Reconnect. Darum P3, nicht höher. Die Fehlerklasse ist trotzdem eindeutig: Ein öffentlicher Admin-Pfad verletzt den eigenen Vertrag, und das seit einer bestimmten Änderung.
- Zweiter Fall im selben Fix: Wirft `amqp_queue.purge()`, bleibt der Channel ebenfalls offen. Der Fix im `finally` deckt beides.

### Vorschlag Register-Zeile

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| F4-nn (R4M-E-02) | P3 | `purge_dead_letters` schließt den eigenen DLQ-Channel aus `_dlq_queue` nicht (auch nicht, wenn `purge()` wirft). In langlebigen Prozessen (Dashboard ab swarmq 0.12, eingebettete Nutzung) bleibt je Purge ein AMQP-Channel bis zum Verbindungsende offen. Regression aus 47ecffb (MR 16, 0.12.0). Quelle: Docstring `_dlq_queue`. | Code: `try/finally` mit `close_channel_quietly(amqp_queue.channel)` wie in `_drain_dlq_with_filter`. Nachweis: Probe `test_r4m_e02_purge_closes_its_own_dlq_channel` plus Fehlerpfad (`purge()` wirft, Channel trotzdem geschlossen). |

---

## R4M-F-01 – `schedule add --unique-for` auf belegten Namen hinterlässt den Dedup-Claim

### Einstufung: **LÜCKE, P3** (Produktfrage). Kein FEHLER, weil keine Zusage über den Claim verletzt ist und die gleiche Regel an anderer Stelle in die Gegenrichtung zeigt.

### Entscheidungs-Abgleich

- **E-058** (`applies_when`: „Änderungen an ScheduleStore.add, Client._schedule_deferred oder swarmq schedule add“; „…dass swarmq schedule add nach verlorener Antwort ‚already exists‘ meldet“). Festgelegt ist: Bei `schedule add` meldet ein bestehender Name „already exists“, auch bei gleichem Inhalt. **Bewusste Folge:** „Geht bei `swarmq schedule add` die Antwort verloren, meldet die CLI ‚already exists‘, obwohl der eigene Insert durchging.“ Nachtrag: „Die CLI meldet ‚already exists‘, und der Eintrag feuert trotzdem.“ Ein `False` von `add_schedule` kann bei der CLI also der **eigene** gespeicherte Schedule sein. Pauschales Freigeben nähme ihm dann die Einmaligkeit.
- **E-059, Nachtrag „Claim nach einem Fehler beim Speichern (Review U15)“** regelt den Nachbarpfad (`Client._schedule_deferred`): „*Konflikt* (`False`): … der Claim bleibt stehen, damit ein Wiederholversuch des Aufrufers nicht doppelt ausführt“; „Blockieren ist für ein Einmaligkeits-Feature die richtige Richtung.“ Festgeschrieben durch `test_store_conflict_raises_and_keeps_the_own_claim` (Marker über E-059). `applies_when` von E-059 nennt die CLI nicht. Für `schedule add` ist der Fall also **nicht entschieden**. Die nächste Regel zeigt aber gegen die Erwartung der Sichtung.
- **E-011, Nachtrag FS-06b** und `tests/integration/test_cli_schedule.py:77-94`: Der Owner ist der Schedule-Name; ein Selbsttreffer bei `schedule add` bleibt Duplikat. **E-066**: Der Claim lebt ab dem Speichern bis Fire plus `unique_for`; ein gleicher Aufruf in dieser Zeit ist ein Duplikat („kein Fehler“). Beide erklären Schritt 3 und 4 des Szenarios, regeln aber nicht den Claim eines abgewiesenen `add`.
- Code: `schedule_cmd.py:248-256` gibt nur bei einer Ausnahme frei („Rollback the dedup claim on store failure (P1 #1)“). Der Kommentar `:205-208` sagt nur „On duplicate, the insert is skipped“.

### Quelle der Erwartung (geprüft, trägt nicht als Zusage)

- `website/reference/cli.md:320-324`: „claims a dedup flag … before inserting the schedule; … A second `schedule add` … **while the flag is held** is rejected“. Das beschreibt den Claim, sagt aber nichts darüber, dass er bei einem abgewiesenen Insert freigegeben wird.
- Hinweistext `schedule_cmd.py:196`: „use 'schedule remove --permanent' first to replace it“. Das scheitert bei einem eindeutigen Schedule **auch ohne** den verwaisten Claim: Ersetzt man einen gespeicherten `--unique-for`-Schedule durch `remove --permanent` und `add` mit denselben Argumenten, lehnt dessen eigener, noch lebender Claim ab (`remove` gibt keine Claims frei, `_run_schedule_remove` ruft kein `release_unique`; Selbsttreffer = Duplikat nach FS-06b). Der Hinweis ist also schon heute keine Zusage, die über Claims hinweg gilt.
- Das Rollback-Prinzip (P1 #1) gilt für Ausnahmen, nicht für `False`.

### Realismus

Mechanismus und Probe stimmen (rote Probe gegen den echten `_run_schedule_add` mit In-Memory-Backend; dessen `claim_unique`/`add_schedule` bilden SET NX bzw. HSETNX nach). Auslöser: Betreiber legt mit `--unique-for` einen Namen an, der schon mit anderem Inhalt existiert. Das ist selten. Folge: Sperre für `report(args)` bis `unique_for` plus Zeit bis zum ersten Fire, und ein Producer-Aufruf mit gleichen Argumenten bekommt „nightly“ als `task_id` zurück (E-066-Pfad), ohne dass es den Task gibt. Lästig, aber selbstheilend über die TTL.

### Nebenbefund beim Abgleich (für Spur M zur Aufnahme vorgeschlagen, gleiche Produktfrage)

Die CLI gibt den Claim bei **jeder** Ausnahme frei (`except BaseException`, `schedule_cmd.py:251-255`), also auch bei einem Backend-Ausfall (Timeout, Verbindung). `sched_add.lua` speichert Definition und Fire-Index atomar (E-058-Nachtrag). Nach einem Ausfall kann der Schedule also gespeichert sein und feuern, während sein Claim schon freigegeben ist: Ein zweiter gleicher `add` oder ein Producer-Aufruf gilt nicht mehr als Duplikat. Für den Client hat E-059 genau diesen Fall umgekehrt entschieden („*Backend-Ausfall* … Der Claim bleibt stehen wie … nach `PublishIndeterminate` (E-041)“). Für die CLI fehlt die Regel. Das ist dieselbe Lücke in Gegenrichtung (zu früh frei statt zu lang gehalten).

### Produktfrage

**Wie geht `swarmq schedule add --unique-for` mit dem eigenen Claim um, wenn das Speichern nicht eindeutig gelingt?**

| Option | Inhalt | Folge |
|---|---|---|
| (a) | Bei `False` den gespeicherten Eintrag unter dem Namen lesen: trägt er **nicht** den eigenen `dedup_hash` (anderer Inhalt), den Claim owner-geprüft freigeben; trägt er ihn (möglicher eigener Insert nach verlorener Antwort, E-058), stehen lassen. Bei Backend-Ausfall stehen lassen (wie E-059), bei Fehlerantwort/sonstigem Fehler freigeben. | Kein verwaister Claim bei echtem Namenskonflikt; E-058-Fall bleibt geschützt; CLI und Client folgen derselben Ausfallregel |
| (b) | Heutiges Verhalten bei `False` beibehalten (blockieren, wie E-059 für den Client) und in `cli.md` dokumentieren: „Ein abgewiesener `add` hält den Claim bis `unique_for` + Zeit bis zum Fire“; Hinweistext um „and wait for the dedup flag to expire“ ergänzen. Ausfallregel trotzdem an E-059 angleichen. | Keine Logik am Konfliktpfad, aber eine sichtbare Sperre nach einem Tippfehler im Namen |
| (c) | Claim erst **nach** erfolgreichem Speichern setzen. | Verworfen: E-066/Unit 7 haben „Claim vor dem Insert“ bewusst gewählt (Doppel-Speicher-Race) |

**Empfehlung: (a).** Der eigene Insert ist bei der CLI, anders als beim Client-Oneshot mit verschwundenem Eintrag, über den gespeicherten `dedup_hash` erkennbar. Damit trägt das Argument aus E-059 („kann nur der eigene sein“) hier nicht, und der Claim lässt sich gezielt freigeben. Antwort als Nachtrag zu E-059 (oder neue E-nnn mit `applies_when` „swarmq schedule add“) ins Register, festschreibende Tests mit Marker.

### Vorschlag Register-Zeile

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| F4-nn (R4M-F-01) | P3 | LÜCKE: `swarmq schedule add --unique-for` auf einen schon belegten Namen hält den Dedup-Claim bis `unique_for` + Zeit bis zum Fire, ohne dass ein Schedule ihn trägt. Umgekehrt gibt die CLI den Claim nach einem Backend-Ausfall frei, obwohl `sched_add.lua` gespeichert haben kann. Der Client-Pfad ist in E-059 (Nachtrag Review U15) geregelt, die CLI nicht. | Produktfrage (Empfehlung a); danach Code in `_run_schedule_add` plus Regel und Test mit Marker. Kein Nachweis-Test vor der Entscheidung. |

---

## R4M-F-02 – JSON `already_exists: true` bei einem Dedup-Duplikat

### Einstufung: **LÜCKE, P4**

### Entscheidungs-Abgleich und Quelle

- Register: keine Regel zu den JSON-Feldern von `schedule add`. E-058 betrifft nur den Namenskonflikt.
- Herkunft: Plan `docs/plans/2026-04-17-003-fix-phase3c-review-followup-plan.md:194-197` (Unit 4, M-2): „`{"inserted": bool, "name": str}`. Wenn `already_exists`: `inserted=false`“. Commit 8971cbc (2026-04-17) führt `already_exists: bool` als `not inserted` ein, also zu einer Zeit, in der `False` nur „Name existiert“ hieß. `duplicate_of` kam am selben Tag mit 0a2a8b6 (R3.5 CLI), ohne `already_exists` anzupassen. Das ist eine Nebenwirkung, keine Entscheidung.
- Doku: `already_exists` steht nirgends unter `website/` (grep ohne Treffer). `cli.md:322-323` nennt für das Duplikat nur `inserted: false` und `duplicate_of`. Die Integrationstests `test_cli_schedule.py:249, 263` legen `already_exists` nur für Erfolg bzw. Namenskonflikt fest.
- Damit fehlt eine Zusage. Der Feldname widerspricht dem Duplikatfall, und der Tabellenzweig derselben Funktion (`schedule_cmd.py:186-197`) unterscheidet beide Fälle. Zusammen ist das eine Produktfrage, kein FEHLER.

### Realismus

Das Verhalten ist sicher (rote Probe). Die Wirkung ist gering: Ein Agent, der `already_exists` statt `duplicate_of` auswertet, hält einen Duplikat-Abweis für einen Namenskonflikt. Kein Datenverlust.

### Produktfrage

Optionen: (a) `already_exists` heißt „unter dem Namen existiert ein Eintrag“ und ist im Duplikatfall `false`, und das Feld wird in `cli.md` dokumentiert; (b) Verhalten lassen und dokumentieren: „`already_exists` ist `not inserted`; den Grund nennt `duplicate_of`“. **Empfehlung: (a)**, eine Zeile Code, und der Feldname sagt dann, was er meint. Ausgabevertrag für Agenten (R3.10 P3 #18). Änderungshinweis im Changelog.

### Vorschlag Register-Zeile

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| F4-nn (R4M-F-02) | P4 | LÜCKE: `schedule add --format json` meldet bei einem Dedup-Duplikat `already_exists: true`, obwohl unter dem Namen nichts gespeichert ist; das Feld ist nicht dokumentiert. | Produktfrage (Empfehlung a); dann Code in `_render_add_result`, Doku `cli.md`, Test. |

---

## R4M-F-03 – `dlq list/inspect/retry`: „nicht gekürzt“ bzw. „nicht gefunden“, obwohl das Lesen am Limit stoppte

### Einstufung: **FEHLER, P3** (Konfidenz hoch für den Mechanismus, mittel bis niedrig für die Häufigkeit)

### Entscheidungs-Abgleich

- **E-028** (`applies_when` u. a. „… oder abgelaufene Einträge beim Listen verschwinden“): Entschieden ist, dass abgelaufene Einträge beim Listen **ohne Kopie bestätigt** werden, außerdem Teilmengen bei parallelen Listings und die geänderte Reihenfolge. Über Zählung und Kürzungsanzeige sagt E-028 nichts. Kein Widerspruch: Der Fix ändert nicht, *dass* Abgelaufene verworfen werden.
- **Dokumentierte Folge, die beim Fix zu beachten ist:** `website/guides/dead-letter-queues.md:271-274`: „Expired messages are dropped on read. … No log event reports it, and the `scanned` count of `--format json` does not include it.“ (eingeführt in a56c75e, U11). Dass `scanned` die Abgelaufenen nicht zählt, ist also zugesagt. Der Fix darf `scanned` nicht umdeuten, er muss die Kürzung getrennt ermitteln.
- E-031 (DLQ-Fehlerergebnis) ohne Bezug. Kein Test mit `entscheidung`-Marker prüft `truncated`/`scan_exhausted` an der Grenze.
- Die Fundstelle (`admin.py` `_drain_dlq_messages`, `scanned = len(entries)`) liegt in Gruppe E. `3c-sichtung-E.md` führt den Fall nicht. Im Register nur einmal führen (Fundstelle `admin.py`, Anzeige `dlq_cmd.py`).

### Quelle der Erwartung

- `website/reference/cli.md:430-431`: „The table output shows `(truncated)` when `--limit` is hit; `--format json` includes a `truncated: bool` envelope.“
- `website/guides/dead-letter-queues.md:131-132`: „`--limit N` … The output shows `(truncated)` or JSON `truncated: true` when hit.“
- Docstrings `admin.py:468-470` (`list_dead_letters`): „`scanned_count` — the number of messages **actually read** … so callers can tell ‚DLQ had fewer than limit‘ from ‚stopped at limit‘“, und `:494-496` (`inspect_dead_letter`): „distinguish ‚genuinely not in the DLQ‘ from ‚scan stopped at limit before the match‘“.
- Das Limit wurde erreicht (Probe: `queue.get.await_count == 2` bei `--limit 2`), die Anzeige sagt das Gegenteil. Damit ist die Zusage in `cli.md` verletzt.

### Realismus und Widerlegungsversuche

- *„RabbitMQ verwirft abgelaufene Nachrichten selbst.“* Nur am Kopf der Queue und nur bei gesetzter `expiration` (`dead-letter-queues.md:284-286`). Vom DLX geroutete Nachrichten (Delivery-Limit, Rückfall bei gescheiterter Worker-Kopie) und Altbestand tragen keine `expiration` und halten Abgelaufene dahinter fest. Außerdem kann ein Eintrag während einer langen Lesung ablaufen. Der Fall ist also möglich, mit dem Default `SWARMQ_DEAD_LETTER_TTL_S=604800` ohne eigene Konfiguration.
- *„Der Fall verschwindet nach dem ersten Listing.“* Stimmt, die Abgelaufenen sind danach bestätigt. Schwer wiegt der eine falsche Lauf bei `dlq inspect`/`dlq retry <id>`: „not found … (scanned N messages)“ ohne den Hinweis „scan hit --scan-limit“ und JSON `scan_exhausted: false`, obwohl der Task hinter dem Limit liegt. Ein Betreiber oder Agent schließt daraus, der Task sei nicht in der DLQ. Exit-Code 3 (`_DLQNotFound`) verstärkt das für Skripte.
- *Mock-Realismus:* Die Probe nutzt die Fakes aus `tests/unit/broker/test_dlq_read.py` (`_reader`, `_message`) und den echten `_run_dlq_list` → `list_dead_letters` → `_drain_dlq_messages`. Der Ablaufpfad (`_stamp_or_expire` → Ack, `continue`) ist echter Code.
- Häufigkeit: braucht abgelaufene Einträge hinter einem Eintrag ohne `expiration` **und** eine DLQ, die mindestens `--limit`/`--scan-limit` (Default 100/1000) Nachrichten hält. Das ist selten, aber im Störfall (viele Dead-Letter) wahrscheinlicher. P3.

### Vorschlag Register-Zeile

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| F4-nn (R4M-F-03) | P3 | `dlq list` meldet `truncated: false` bzw. keine `(truncated)`-Zeile, `dlq inspect/retry <id>` melden „not found“ ohne Scan-Limit-Hinweis (`scan_exhausted: false`), obwohl das Lesen am Limit stoppte. Ursache: abgelaufene Einträge verbrauchen Lesungen, `scanned = len(entries)` zählt sie nicht. Quelle: `cli.md:430-431`, `dead-letter-queues.md:131-132`, Docstrings `list_dead_letters`/`inspect_dead_letter`. | Code: Kürzung aus der Zahl der **Lesungen** ableiten, nicht aus `scanned` (z. B. `_drain_dlq_messages` liefert `reads`/`hit_limit`; Vertrag `BrokerAdmin` erweitern oder `scanned` dokumentiert lassen und ein Flag ergänzen). `scanned` bleibt ohne Abgelaufene (zugesagt). Nachweis: Probe `test_list_reports_truncation_when_expired_entries_used_up_the_limit` plus Gegenstück für `inspect` (`scan_exhausted`). |

---

## Stichprobe der Testlücken (je 5 mit höchster Wirkung)

Frage: Stimmt die Einordnung „Testlücke“ (Original richtig), oder ist das Original falsch? Geprüft durch Lesen des Originals im Kontext. Für F sind zwei Mutanten zusätzlich nachgebildet (`mut_env`), gegen die Probe und gegen `tests/unit/process_manager`.

### Gruppe E

| Mutant | Änderung | Original geprüft | Ergebnis |
|---|---|---|---|
| `admin._drain_dlq_with_filter__mutmut_27` | `_copy_then_ack(…, match_task_id → None, …)` | `admin.py:254`: Der Filter steuert in `_copy_then_ack`, welche Kopie in die Haupt-Queue geht (`to_main = move_to_main and (match_task_id is None or is_match)`). Das Original reicht ihn richtig durch. Mit dem Mutanten verschöbe `dlq retry <id>` alle vorher gelesenen Einträge | Testlücke bestätigt (hohe Wirkung) |
| `admin.retry_dead_letter__mutmut_15` | `len(matched) > 0` → `>= 0` | `admin.py:526`: Das Original meldet nur bei Treffer `found`. Richtig | Testlücke bestätigt. Nebenbei: `scanned` hier hat dieselbe Abgelaufenen-Unterzählung wie R4M-F-03 (dort mitgeführt) |
| `connection._acquire_from_pool__mutmut_8` | `__aexit__(None, None, None)` → `(None, None)` | `connection.py:376`: `aio_pika/pool.py:158-163` verlangt drei Argumente. Original richtig; der `except Exception` verschluckt beim Mutanten den `TypeError` | Testlücke bestätigt (Fake mit echter Signatur nötig) |
| `consumer_watch.cancel_all__mutmut_3` | `continue` → `break` bei `entry.tag is None` | `consumer_watch.py:166-168`: Einträge ohne Tag entstehen real (`_reregister` abgemeldet, `_consume` gescheitert). Original richtig | Testlücke bestätigt |
| `declarations.invalidate_declaration__mutmut_2` | `discard(name)` → `discard(None)` | `declarations.py:248-250`: Original verwirft die Stufe korrekt (Review MR 10, P2-3) | Testlücke bestätigt |
| zusätzlich `workflows.apply__mutmut_1` | `workflow_id = None` | `workflows.py:85` `uuid.uuid7()`. Richtig | Testlücke bestätigt (alle Workflows teilten sich mit dem Mutanten einen Zustand) |

### Gruppe F

| Lücke / Mutant | Änderung | Original geprüft | Ergebnis |
|---|---|---|---|
| T-F-01 `restart_budget._check_heartbeats__mutmut_12/_30` | `continue` → `break` | `restart_budget.py:74-89`: Tote, kanallose, terminierende und recycelnde Worker werden übersprungen, die Schleife läuft weiter. Richtig | Testlücke bestätigt (Probe grün gegen Original) |
| T-F-02 `restart_budget._escalate_terminating__mutmut_4/_14` | `continue` → `break` | `restart_budget.py:144-155`: `remaining` überschreibt `_terminating`. Mit `break` bekäme jeder spätere Eintrag nie SIGKILL. Original richtig. **Nachgebildet:** Probe rot unter beiden Vorkommen (1 failed), `tests/unit/process_manager` grün unter beiden (322 passed) | Testlücke bestätigt |
| T-F-08 `supervisor._monitor_loop__mutmut_68` | `autoscale is not None` → `is None` | `supervisor.py:531-532`: `_autoscale_tick()` nur mit Autoskalierer. Richtig | Testlücke bestätigt (Probe grün) |
| T-F-13 `dlq_cmd._run_dlq_purge__mutmut_3` | Queue-Argument vertauscht | `dlq_cmd.py:253`: `purge_dead_letters(args.queue)`. Richtig. **Nachgebildet** (`args.queue` → `"default"`): Probe rot | Testlücke bestätigt (Löschen einer fremden DLQ wäre unentdeckt) |
| T-F-15 `schedule_cmd._resolve_fire_time__mutmut_10/_23` | Basis `time.time() / 1000`; Zeitzone von `--at` überschrieben | `schedule_cmd.py:151` `int(time.time() * 1000)`, `:160-161` UTC nur ohne `tzinfo` (wie `cli.md:312`). Richtig | Testlücke bestätigt (Probe grün) |

**Ergebnis der Stichprobe:** Alle 11 (bzw. 12) Einordnungen als Testlücke stimmen; in keinem Fall ist das Original falsch. Einzige Querverbindung: `retry_dead_letter`/`inspect_dead_letter` teilen die Zählung aus R4M-F-03 (kein neuer Verdacht, dort abgedeckt). Die Messbefunde der F-Sichtung (SIGTERM → Exit 0 im mutmut-Enkel) bleiben unberührt. Die beiden Nachbildungen bestätigen, dass die Proben unter dem Mutanten rot und die Bestandssuite grün ist.

## Bezug zu den offenen LÜCKEN aus Plan R4

| Offene LÜCKE | Berührt durch | Wirkung |
|---|---|---|
| FS-32 (`status()` bleibt nach `handle.cancel()` `running`) | R4M-E-01 | Verschärft: Läuft das Workflow-Flag vor dem Zustand ab (Reattach-Handle 24 h bei `result_ttl` > 24 h), meldet auch `get()` den Abbruch nicht mehr. Fix von R4M-E-01 und FS-32 zusammen planen |
| FS-25, FS-26, FS-29, F3-32 bis F3-38, F3-41 | – | nicht berührt. R4M-F-01 liegt neben E-059 (Nachtrag Review U15), nicht neben F3-36 (Cron-Fires und Middleware) |

---

## Gruppe D: Valkey-Backend (R4M-D-01/02/03)

Gegenprüfung zu `3c-sichtung-D.md`. Nur gelesen; nichts unter `src/` oder `tests/` geändert, keine Dienste gestartet. Eigene Messskripte unter `<scratchpad>/u17d/` (`fenster.py`, `d02_refused.py`).

**Probe-Tests selbst ausgeführt** (ohne Dienste, geschlossener bzw. stummer lokaler Port):

```
PYTHONPATH=. uv run --extra test pytest -p no:randomly -n0 --rootdir . -c pyproject.toml \
  <scratchpad>/u5d/test_mut_d_nachweise.py
→ 5 failed in 4.00s
```

| Probe | Assertion | Ergebnis |
|---|---|---|
| `test_r4m_d01_chord_store_wraps_a_transport_error` | Gegenprobe `LockManager.acquire` → `BackendConnectionError` (grün), dann `ChordManager.record_member` → `BackendConnectionError` | roh `valkey.exceptions.ConnectionError: Error 111 connecting to 127.0.0.1:…` |
| `test_r4m_d01_chord_store_reports_a_self_healing_reply_as_outage` | `OutOfMemoryError` aus dem Skript → `BackendConnectionError`, nicht `BackendResponseError` | roh `valkey.exceptions.OutOfMemoryError` |
| `test_r4m_d01_worker_redelivers_when_the_chord_store_is_down` | `advance` wirft den rohen `ConnectionError` → `fail` nicht aufgerufen, Ergebnis `Redelivered` | `Expected fail to not have been awaited. Awaited 1 times.`, Log `workflow_advance_failed` |
| `test_r4m_d02_connect_wraps_a_timeout` | `connect()` gegen stummen Port → `BackendConnectionError` | roh `valkey.exceptions.TimeoutError: Timeout connecting to server` |
| `test_r4m_d03_second_connect_after_a_failed_one_still_reports_the_outage` | erster `connect()` wirft (grün), zweiter muss ebenfalls werfen | `DID NOT RAISE BackendConnectionError` |

Die Proben mocken nur die Grenze: echter valkey-py-Client (6.1.1) gegen einen lokalen Port, ein `register_script`-Mock für die OOM-Antwort, die Worker-Fixtures aus `tests/unit/worker/conftest.py`. Die dritte D-01-Probe setzt den Advance auf genau die Ausnahme, die die erste Probe aus dem echten `ChordManager` holt; `WorkflowAdvancer.advance` → `_on_leaf_complete` → `_on_group_member` (`workflow_orchestration.py:260-290`, `:395`) fängt dazwischen nur `_StaleWorkflowState` und `_WorkflowAlreadyTerminal`. Der Kurzschluss ist also realistisch.

### Ergebnis auf einen Blick (Gruppe D)

| Verdacht | Einstufung | Prio | Kern der Begründung |
|---|---|---|---|
| R4M-D-01 | **FEHLER** | P2 | Plan 2026-05-30 `:89` nennt genau diesen Fall („sonst entkommt der Counter-Fehler dem Worker-Handler“), `:451`; `logging.md:667` (Backend-Fehler im Advance → Redelivery); E-040 (Fail-Mark nur bei endgültig erfolglosem Schritt) |
| R4M-D-02 | **FEHLER** | P3 | Vertrag `contracts/backend.py:17` „Treiberfehler → `BackendConnectionError`“; Code-Kommentar `connection.py:81-83` („timed-out op … wrapped to BackendConnectionError“); `errors.md:192-193` |
| R4M-D-02b (Dauer bis zur Ausnahme, rund 3 min) | **LÜCKE** | P3 | Retry-Politik aus dem Spike (5 Wiederholungen, Deckel 2 s) ist für Befehle entschieden, die Verschachtelung beim Verbindungsaufbau nie |
| R4M-D-03 | **FEHLER** | P3 | Vertrag „Idempotent; Treiberfehler → `BackendConnectionError`“; Fehlerkatalog „Back off, retry“; E-005 („es gibt nur die WARNING“) wird still umgangen |

---

### R4M-D-01 – `ChordManager` wandelt Treiberfehler nicht; ein erfolgreicher Workflow kann `failed` werden

#### Einstufung: **FEHLER, P2** (Konfidenz hoch für den Mechanismus, mittel für die Häufigkeit)

#### Entscheidungs-Abgleich

- **Register:** `grep -il chord` trifft E-039 (FAF-Schritt) und E-040 (Fail-fast), `grep -il "BackendConnectionError\|connect()\|reconnect"` zusätzlich E-004, E-005, E-006, E-008, E-013, E-023, E-030, E-034, E-037, E-041, E-060, E-062. Keine Regel legt fest, dass ein Treiberfehler im Chord-Store den Workflow fail-markt. E-040 regelt das Gegenteil: „Endet ein Workflow-Schritt **endgültig ohne Erfolg** … markiert der Worker den Workflow synchron als `failed`“; unter „Bewusste Folgen“: „Ein Schritt, der schon erfolgreich war, markiert seinen Workflow bei einem späteren Dead-Letter nicht als gescheitert.“ Hier ist der Schritt erfolgreich (Ergebnis gespeichert).
- **Plan** `docs/plans/2026-05-30-001-feat-workflow-primitives-plan.md:89` (Institutional Learnings): „**BackendConnectionError-Boundary** … jede neue Backend-Methode wrappt `(ValkeyError, OSError)` → `BackendConnectionError`, **sonst entkommt der Counter-Fehler dem Worker-Handler**.“ Der „Counter“ ist der Chord-Zähler aus U3. Dazu `:451` (System-Wide Impact): „Advancement-Fehler folgen der Ack-Ordering-Regel — transient → `nack(requeue=True)` … neue Backend-Methoden wrappen in `BackendConnectionError`.“ Der Plan hat das Verhalten also ausdrücklich gefordert; U3 (`:262-288`) nennt nur Lua-Testfälle, keinen Fehlerpfad, und so blieb es ungeprüft.
- **docs/solutions** `logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md:171`: „Library-Exceptions an der Boundary wrappen — I/O Methoden dürfen nie library-spezifische Exceptions nach außen lassen“; `:156`: „Ohne Wrapping wurde der strukturierte Error-Pfad des Workers … komplett umgangen.“
- **Gegenbeispiel bewusst roher Fehler:** `ValkeyLockStore.current_owner` lässt Treiberfehler absichtlich durch (Docstring „Decision E4: ‚kein Leader‘ vs. ‚Backend unerreichbar‘ bleibt unterscheidbar“, `locks.py`), und die CLI fängt dafür die rohen Klassen (`cli/__init__.py:403-424`). Das gilt nur für die Leader-Abfrage; für den Chord-Store gibt es keine solche Begründung, und sein einziger Aufrufer (Worker) kennt die rohen Klassen nicht.
- **Konsistenz der Stores:** `LockManager.acquire/release/renew/extend` (`locks.py`, je `except BackendConnectionError: raise` / `except Exception: raise as_backend_error(…)`), `ValkeyExecutionGuardStore._run`, `ValkeyRateLimitStore.check`, `ValkeySetpointStore._guarded`, `results.py` und `schedules.py` über `_run` bzw. `as_backend_error` wandeln alle. `chords.py:61-64` ist die einzige Skript-Grenze ohne `try`.
- **Historie:** `git log -- src/swarmq/backend/valkey/chords.py`: ff443e4 (U18, verschoben), 3360838 (Keyspace), ed7e649 (Namensraum); keine Änderung am Fehlerpfad. Das Wrapping in `locks.py` kam mit 7366621/1c5682b/ff8c098; `chords.py` blieb dabei außen vor.
- **Tests:** Kein Test mit `entscheidung`-Marker berührt Advance oder Chord-Store. `test_workflow_advance_unexpected_error_fail_marks_and_acks` (`tests/unit/worker/test_results.py:185`) prüft Fail-Mark für `RuntimeError("advance bug")`, also einen Programmfehler, nicht einen Treiberfehler. Der Fix ändert dieses Verhalten nicht.
- **Chaos:** `tests/chaos/test_workflow_chord_idempotency.py` prüft nur Redelivery und Latenz (`add_latency`), keinen Ausfall während des Chord-Aufrufs.

#### Quelle der Erwartung

1. Plan 2026-05-30 `:89` und `:451` (s. o.), wörtlich für den Chord-Zähler.
2. `website/reference/logging.md:667`: `workflow_advance_backend_error` „Backend/broker error … while advancing. WARNING for `AT_LEAST_ONCE`: the step is redelivered and the idempotent advance runs again.“ Dagegen `:666` `workflow_advance_failed`: „**Unexpected** error … the workflow is marked failed.“ Ein Verbindungsabbruch zu Valkey ist kein unerwarteter Fehler.
3. Code an der Fundstelle: `worker/results.py:233-242` („Transient — redelivery retries the idempotent advance“).
4. E-040 (s. o.) und `website/guides/workflows.md:227-243` (Fehlerfälle): kein Valkey-Ausfall als Grund für `failed`.
5. Gleiche Regel an anderer Stelle: `RateLimitStore.check` „must raise `BackendConnectionError` when the backend is unreachable (connection error, timeout)“ (`api.md:906-909`, `contracts/coordination.py:76-81`).

#### Realismus und Widerlegungsversuche

- *„Ein Ausfall trifft zuerst das Zustandslesen und wird dort gewandelt.“* Richtig für Ausfälle, die vor dem Advance beginnen: `get_workflow_state` und `is_workflow_cancelled` laufen über `_run` und führen zur Redelivery. Für den Transportfehler bleibt deshalb nur das Fenster zwischen diesen beiden Lesungen und `EVALSHA chord_complete` (ein bis zwei Round-Trips) oder ein Verbindungsabbruch während des Skripts. Das schwächt die Häufigkeit für Neustarts ab.
- *Gemessenes Fenster:* Mit der Retry-Politik aus `connection.py:247` wirft `record_member` gegen einen abweisenden Port nach **3,1 s** (`<scratchpad>/u17d/fenster.py`, echter Backoff). Ein Neustart, der im genannten Fenster beginnt, länger als 3,1 s dauert und vor Ende des Retry-Fensters des Fail-Marks (weitere rund 3,1 s, gewandelt) zurück ist, endet mit `failed`. Dauert er länger, scheitert das Fail-Mark gewandelt und die Nachricht wird neu zugestellt (`results.py:187-192`); dann bleibt nur die falsche ERROR-Zeile. Mit `LOADING` gilt dasselbe: `BusyLoadingError` ist in valkey-py ein `ConnectionError`.
- *Kein Zeitfenster nötig: OOM unter `noeviction`.* E-005 verlangt `maxmemory-policy noeviction`. Erreicht Valkey `maxmemory`, lehnt es Schreibbefehle mit `OOM` ab, Lesebefehle nicht. Das Zustandslesen gelingt, `chord_complete.lua` (`SADD`) scheitert sofort und roh (`OutOfMemoryError` wird nicht wiederholt, kein Transportfehler). Das Fail-Mark ist ebenfalls ein Schreibzugriff; scheitert es mit `OOM`, wandelt `as_backend_error` das zu `BackendConnectionError` und der Schritt wird neu zugestellt. Läuft der Speicher aber knapp an der Grenze und geben ablaufende Ergebnis-Keys zwischen beiden Aufrufen Platz frei (unter Last laufend der Fall), gelingt das Fail-Mark, und der Workflow ist `failed`. Das trifft **jedes** Gruppenglied, nicht nur das letzte: `_on_group_member` ruft `record_member` für jedes Glied (`workflow_orchestration.py:395`). Die Sichtung beschreibt das Szenario zu eng („beim letzten Gruppenglied“).
- *Hängender Server* (`CLIENT PAUSE` beim Failover, Fork-Stillstand): `socket_timeout` 10 s → roher `TimeoutError` nach mehreren Wiederholungen; gleiche Folge.
- *Loadsuite:* `valkey_restart` ist als Störung geplant (Plan R4 KTD5, `2b-erwartungen.md:13`, Erwartung „kein Verlust“). Ob die Störläufe Workflows enthalten, ist nicht geprüft (kein Kontext Lastserver). Ein falsch `failed` gemeldeter Workflow wäre dort ein Verlust im Sinne der Erwartung.
- *Schaden:* Der Workflow ist terminal `failed`, `get()` wirft `TaskError` mit der Treibermeldung, der Chord-Body läuft nie, obwohl jeder Schritt gelungen ist. Ein Fail-Mark wird nicht zurückgenommen. Das rechtfertigt P2 trotz mittlerer Häufigkeit.

#### Vorschlag Register-Zeile

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| F4-nn (R4M-D-01) | P2 | `ChordManager.record_and_check_complete`/`record_member` geben Treiberfehler roh weiter (Transport, Zeitablauf, `OOM`/`READONLY`). Im Advance landen sie im `except Exception` des Workers: `workflow_advance_failed`, Fail-Mark, Ack. Unter einem Valkey-Neustart im Advance-Fenster oder unter `OOM` an der `maxmemory`-Grenze (`noeviction`, E-005) wird ein Workflow, dessen Schritte alle gelungen sind, `failed`; betroffen ist jedes Gruppen- und Chord-Kopfglied. Quelle: Plan 2026-05-30 `:89`, `:451`; `logging.md:666-667`; E-040; `results.py:233-242`. | Code: in `chords.py` wie `LockManager.acquire` wandeln (`except BackendConnectionError: raise` / `except Exception as exc: raise as_backend_error(…, exc) from exc`). Nachweis: Proben (a) und (b) als `@pytest.mark.befund` in `tests/unit/backend/` (neue Datei für `chords.py`), dazu ein Worker-Test mit echtem `ChordManager` über einen Fake-Client bis `Redelivered`. Testlücke Nr. 1 der Sichtung (Fehlerpfade aller Stores einheitlich) im selben Zug. |

---

### R4M-D-02 – `connect()` wandelt einen Zeitablauf nicht in `BackendConnectionError`

#### Einstufung: **FEHLER, P3** (Konfidenz hoch; Wirkung gering, weil CLI und Supervisor beide Klassen gleich behandeln)

#### Entscheidungs-Abgleich

- **Register:** kein Treffer für Verbindungsaufbau beim Start. E-006 (Exit-Codes `swarmq check`) und E-062 (`worker check`) betreffen den Prüfbefehl, nicht `connect()`.
- **Decision E4** (Leader-Abfrage roh, `cli/__init__.py:403-407`): gilt ausdrücklich nur für `get_scheduler_leader`. Dass die CLI die rohen Klassen fängt, ist ein Nebeneffekt dieser Entscheidung, keine Entscheidung über `connect()`.
- **Historie:** `git log -S"except (ValkeyConnectionError, OSError)"`: bf626f5 (erste Fassung), 8101c88 (U17, verbatim). `socket_connect_timeout` kam später mit 111983a („bound every hang path“, Härtung R2 P1-2). Der `except` wurde dabei nicht erweitert: Seit 111983a gibt es den Zeitablauf beim Verbinden, vorher entstand er praktisch nicht.
- **Tests:** `test_connect_wraps_connection_error` (`tests/unit/backend/test_valkey.py:318`) prüft nur `ConnectionError`; kein `entscheidung`-Marker an `connect()`.

#### Quelle der Erwartung

1. Vertrag `ResultBackend.connect`: „Idempotent; Treiberfehler → `BackendConnectionError`“ (`src/swarmq/contracts/backend.py:17`).
2. Code-Kommentar an der Fundstelle (`connection.py:81-83`): „a timed-out op surfaces as ValkeyTimeoutError → retried (transient) → wrapped to BackendConnectionError“. `ValkeyTimeoutError` steht in `_TRANSIENT_ERRORS` und wird in `as_backend_error` als Ausfall eingestuft, nur der `except` in `connect` lässt ihn aus.
3. `website/reference/errors.md:192-193`: `BackendConnectionError` „Worker startup when Valkey is not reachable“; Fehlerkatalog (`website/reference/agent/error-catalog-machine.md:37`): `BackendConnectionError` in der Phase „startup“, Aktion „Back off, retry“ (`:109`). Ein roher `valkey.exceptions.TimeoutError` steht nicht im Katalog.
4. `test_connect_sets_socket_timeouts` nennt den Zweck des Connect-Timeouts: „begrenzt zusätzlich den TCP-Handshake gegen einen blackholed Host“. Genau dieser Fall endet ungewandelt.

#### Realismus und Widerlegungsversuche

- *„Der Fall kommt nicht vor.“* Doch: Ein falscher Host oder Port in einem Netz, das Pakete verwirft (Security Group, Firewall `DROP`, falsche VPC-Adresse), ist eine häufige Fehlkonfiguration beim ersten Deploy, dazu ein angehaltener Container. Abweisende Ports (Valkey aus) ergeben dagegen `ConnectionError` und sind gedeckt.
- *„Die Folgen sind gleich.“* Für die CLI ja: `_startup_error_classes()` nimmt die rohen Klassen auf, und das Kind des Supervisors endet bei jeder Ausnahme mit Exit 1 (`process_manager/entry.py:260`, `logging_drain.exit_code_for`); der Kindprozess schreibt dann aber einen Traceback statt der strukturierten Zeile. Für eingebettete Nutzer nein: `except BackendConnectionError` nach Doku fängt den Fehler nicht.
- *Messung bestätigt* (`<scratchpad>/u5d/d02_zaehlen.py`, selbst ausgeführt): 31 Verbindungsversuche, 30 Backoffs, 18,6 s Backoff, dann `valkey.exceptions.TimeoutError`. Gegen einen abweisenden Port dagegen 6 Versuche und 3,1 s (`<scratchpad>/u17d/d02_refused.py`), dann korrekt `BackendConnectionError`.
- Zusammen mit R4M-D-03: Auch nach dem rohen `TimeoutError` bleibt `_client` gesetzt; ein zweiter `connect()` meldet dann Erfolg.

#### Vorschlag Register-Zeile

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| F4-nn (R4M-D-02) | P3 | `ValkeyBackend.connect()` fängt nur `ConnectionError`/`OSError`; ein Zeitablauf beim Verbinden oder beim `PING` (Host verwirft Pakete, angehaltener Server) kommt roh als `valkey.exceptions.TimeoutError` heraus, ebenso eine Fehlerantwort auf `PING` (z. B. `NOPERM`). Quelle: `contracts/backend.py:17`, Kommentar `connection.py:81-83`, `errors.md:192-193`, Fehlerkatalog. | Code: `except (ValkeyError, OSError) as exc: raise as_backend_error(str(exc), exc) from exc` wie in `_run` (eine `NOPERM`-Antwort wird damit `BackendResponseError`, ebenfalls ein `BackendConnectionError`). Nachweis: Probe `test_r4m_d02_connect_wraps_a_timeout` als `@pytest.mark.befund` in `tests/unit/backend/test_valkey_connection.py`. |

### R4M-D-02b – Dauer bis zur Ausnahme bei einem Host, der Pakete verwirft (rund 3 min)

#### Einstufung: **LÜCKE, P3** (Produktfrage)

- **Entschieden ist** die Befehls-Retry-Politik: Spike `docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md:44-47` („`retry=Retry(ExponentialBackoff(cap=2), retries=5)` … SET/GET transparent gegen Outages“), übernommen als „Reliability hardening (2026-04-17 toxiproxy spike)“ (`connection.py:241-247`); `grep -i "retry\|backoff"` im Register trifft nur Task-Retries (E-023, E-060) und den Unroutable-Backoff (E-004).
- **Nie entschieden** ist die Verschachtelung: valkey-py kopiert die Retry-Politik in jede Verbindung (`asyncio/connection.py:198-203`) und wiederholt dort `_connect` bei einem Zeitablauf, während der Client den Befehl wiederholt. Bei Zeitabläufen ergibt das 31 Versuche × 5 s Connect-Timeout + 18,6 s Backoff ≈ **2,9 min**, bis `connect()` überhaupt wirft. Der Spike hat nur kurze Ausfälle mit abweisendem Proxy gemessen. Die Absicht von 111983a („bound every hang path“) ist eine Grenze, keine Dauer von Minuten.
- Dieselbe Verschachtelung gilt zur Laufzeit für jeden Befehl ohne `op_timeout` gegen einen Host, der Pakete verwirft (Netzpartition). Das gehört zur selben Frage.
- **Produktfrage:** Wie lange darf `connect()` (und ein Befehl zur Laufzeit) gegen einen nicht antwortenden Host blockieren?
  - (a) Start: `PING` beim Verbinden mit einer Gesamtfrist (z. B. `asyncio.timeout` von 15–30 s, Ergebnis `BackendConnectionError`); Laufzeit bleibt.
  - (b) Die Verbindungs-Retry-Politik von der Befehls-Retry-Politik trennen (eigene `Retry` mit 0–1 Wiederholungen für den Verbindungsaufbau), damit die Wiederholung nicht multipliziert.
  - (c) So lassen und dokumentieren („bis zu rund 3 min gegen einen Host, der Pakete verwirft“).
  - **Empfehlung: (a) jetzt, (b) prüfen.** (a) ist klein, beschränkt den Startfall, der unter K8s die Readiness und den CrashLoop-Takt bestimmt, und passt zur Absicht von 111983a. (b) betrifft auch die Laufzeit und gehört mit einem Toxiproxy-Test (`timeout`-Toxic) in den Fix-Plan. Das Ergebnis gehört als Regel ins Register.

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| (R4M-D-02b) | P3 | LÜCKE: `connect()` gegen einen Host, der Pakete verwirft, wirft erst nach rund 3 min (31 Versuche, verschachtelte Retry-Politik). Nie entschieden. | Produktfrage (a)/(b)/(c), Empfehlung (a), dann Regel ins Register. |

---

### R4M-D-03 – Zweites `connect()` nach gescheitertem ersten meldet Erfolg ohne Valkey; die Eviction-Prüfung (E-005) entfällt

#### Einstufung: **FEHLER, P3** (Konfidenz hoch für den Mechanismus; Auslöser nur bei eingebetteter Nutzung mit Wiederholschleife)

Die Sichtung bat, LÜCKE zu erwägen. Dagegen sprechen drei Quellen, die den Zustand nach einem gescheiterten `connect()` festlegen, und eine Entscheidung (E-005), deren Zusage auf diesem Weg still entfällt.

#### Entscheidungs-Abgleich

- **E-005** (`e005-verdraengende-valkey-policy-blockiert-start.md`): „Bei einer nachweislich verdrängenden Policy verweigert der Worker … den Start“; „Ist die Policy nicht prüfbar, blockiert das nie, **es gibt nur die WARNING `valkey_eviction_policy_unknown`**.“ Nach dem zweiten `connect()` ist `eviction_check` `None`: `enforce_eviction_policy` lässt den Start zu (`worker/eviction_gate.py:19-21`), und `log_eviction_check` läuft gar nicht. Es gibt also weder die Sperre noch die Warnung. Die E-005-Tests (`tests/unit/worker/test_eviction_gate.py`, `test_config_valkey_eviction.py`) prüfen nur den ersten `connect()`.
- **Idempotenz-Zusage** in `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md:158`: „Idempotentes `connect()` ist essentiell weil … (2) **Reconnect-Logik `connect()` defensiv aufrufen können muss**.“ Das Muster `if self._client is not None: return` (`:117-121`) setzt voraus, dass `_client` nur nach erfolgreichem Verbinden gesetzt ist. Ein `connect()` als Wiederholungsversuch, der ohne Versuch „Erfolg“ meldet, verfehlt genau diesen Zweck.
- **Kein Beschluss** zum Zustand nach gescheitertem `connect()`: kein Register-Treffer, kein Plan, kein Test. `test_connect_twice_only_calls_from_url_once` (`test_valkey.py:303`) prüft nur den Erfolgsfall und bleibt mit dem Fix grün.
- **Gleiche Regel an anderer Stelle:** `RabbitMQBroker._connect_owned` (`broker/rabbitmq/connection.py:268-288`) setzt `self._connection` erst nach erfolgreichem `connect_robust`; scheitert der Aufbau, bleibt `None` und der nächste `connect()` versucht es erneut.

#### Quelle der Erwartung

1. Vertrag `ResultBackend.connect` „Idempotent; Treiberfehler → `BackendConnectionError`“ und `ping` „Wirft `BackendConnectionError` **vor `connect`** oder bei Störung“ (`contracts/backend.py:17`, `:23`). Nach einem gescheiterten `connect()` ist das Backend nicht verbunden; ein zweiter Aufruf muss denselben Versuch machen und denselben Fehler melden.
2. Fehlerkatalog `website/reference/agent/error-catalog-machine.md:109` (`.de.md:111`): „`BrokerConnectionError` / `BackendConnectionError` → Back off, retry — the library reconnects automatically.“ Die Doku empfiehlt also genau die Wiederholung, die in den Fehler läuft.
3. E-005 (s. o.).

#### Realismus und Widerlegungsversuche

- *„Niemand ruft `connect()` zweimal nach einem Fehler.“* `SwarmQ.__aenter__` (`app.py:126-133`) schließt bei einem Fehler im Backend nur den Broker, nicht das Backend. Ein eingebetteter Start mit Wiederholung nach Fehlerkatalog (`for …: try: async with app: await app.start_worker() except BackendConnectionError: await asyncio.sleep(…)`, Lifespan-Hooks mit `tenacity`) ruft beim zweiten Durchlauf `backend.connect()` auf derselben Instanz. `Worker.start` (`worker/lifecycle.py:409-410`) ruft `connect()` ebenfalls vor seinem `try`. Interne Aufrufer sind nicht betroffen: `setpoint_store.py:76/104` baut je Aufruf einen neuen Store, die CLI endet beim ersten Fehler.
- *„Ist Valkey noch weg, scheitert der nächste Befehl ohnehin.“* Ja, gewandelt; der Start bricht dann später ab. Der Schaden entsteht, wenn Valkey zwischen erstem und zweitem Versuch zurückkommt: Der Worker läuft dann ohne Eviction-Prüfung, auch unter `allkeys-lru`, ohne jede Meldung. Genau der Doppelausführungs-Fall, den E-005 verhindern soll (BZ-04).
- *Probe nachgestellt:* geschlossener Port, erster `connect()` wirft nach 3,1 s `BackendConnectionError`, zweiter kehrt sofort zurück (`DID NOT RAISE`).
- Nebenwirkung des Fixes: Der gescheiterte Client wird heute auch nicht geschlossen, das Backend hält ihn bis `close()`. Ein Fix mit `aclose()` im Fehlerzweig räumt das mit auf.

#### Vorschlag Register-Zeile

| ID | Prio | Befund | Fix-Art |
|---|---|---|---|
| F4-nn (R4M-D-03) | P3 | `ValkeyBackend.connect()` setzt `_client` vor dem `PING` und lässt ihn nach einem Fehlschlag stehen. Ein erneuter `connect()` auf derselben Instanz (Wiederholung nach Fehlerkatalog, `async with app` in einer Schleife) meldet sofort Erfolg, ohne Valkey zu erreichen, und überspringt die Eviction-Prüfung: Kommt Valkey mit verdrängender Policy zurück, startet der Worker im Modus `strict` ohne Sperre und ohne Warnung. Quelle: `contracts/backend.py:17/23`, Lösungsdoku 2026-04-15 `:158`, Fehlerkatalog `:109`, E-005. | Code: im Fehlerzweig von `connect()` den eigenen Client schließen (`aclose()`, Fehler dabei schlucken) und `self._client = None` setzen; nur für den eigenen Client (`_owns_client`). Nachweis: Probe `test_r4m_d03_…` als `@pytest.mark.befund` in `tests/unit/backend/test_valkey_connection.py`, dazu ein Test „zweiter `connect()` nach Rückkehr prüft die Policy“ (`eviction_check` gesetzt). |

---

### Stichprobe der Testlücken (die 5 mit der höchsten Wirkung aus Teil 4)

Frage wie bei E und F: Stimmt „Testlücke“ (Original richtig), oder ist das Original falsch? Geprüft durch Lesen des Originals und der vorhandenen Tests.

| Nr. | Lücke / Mutanten | Original geprüft | Ergebnis |
|---|---|---|---|
| 1 | Fehlerpfade der Stores: `LockManager.release__mutmut_14`, `renew__mutmut_18`, `extend__mutmut_27`, `observe__mutmut_27/29` | `locks.py:76-139`: `release`, `renew`, `extend` wandeln mit `except BackendConnectionError: raise` / `as_backend_error`. Richtig. Die Unit-Tests (`TestLockManagerErrorWrapping`) werfen nur Fehlerantworten und beliebige Ausnahmen, keinen Transportfehler | **Testlücke bestätigt.** Ergänzung: Den gleichen Fehlerpfad hat `chords.py` gar nicht; das ist R4M-D-01, kein Testlücken-Fall. Kleine Korrektur der Sichtung: Die Folge „Lock-Gate requeut statt zurückzustellen“ gilt für `acquire`; `release`/`renew` laufen im Abschluss bzw. im Keeper. Die Klasse des Fehlers entscheidet dort über `task_lock_lost` und die Log-Stufe, nahe an F3-33 |
| 2 | INFO-Rückfall der Eviction-Prüfung: `_from_info__mutmut_1–22`, `probe_eviction__mutmut_9`, `_from_config__mutmut_2–4` | `environment.py:38-77`: `_from_info` liest `maxmemory_policy` aus `INFO memory` (Schreibweise mit Unterstrich ist die von Valkey), `probe_eviction` versucht `CONFIG GET`, dann `INFO`. Richtig. `test_connect_tolerates_forbidden_config_get_but_says_so` (`test_befund_bz04_eviction_policy.py:115-135`) liefert `volatile-lru` über `INFO`, prüft aber nur, dass irgendeine Zeile „maxmemory“ oder „evict“ enthält; das erfüllt auch `valkey_eviction_policy_unknown` | **Testlücke bestätigt, höchste Wirkung der fünf:** Bei Managed-Diensten mit gesperrtem `CONFIG` trägt allein dieser Pfad die E-005-Sperre, und kein Test merkt, wenn er `unknown` liefert |
| 3 | Zeitbasis `now_ms` aus `TIME`: `LockManager.acquire__mutmut_3`, `RateLimiter.check__mutmut_3/6/9` | `locks.py:61-62`, `rate_limit.py:47-48`: `int(s) * 1000 + int(µs) // 1000`. Richtig. Alle Unit-Mocks liefern `µs = 0` (`test_valkey_locks.py:27`, `(1, 0)`; `test_valkey_rate_limit.py:29`; `test_rate_limiter.py:88`), deshalb ist `+`/`-` unsichtbar | **Testlücke bestätigt** (Mock mit `µs ≠ 0` und Prüfung des Skriptarguments genügt) |
| 4 | Retry-Politik des eigenen Clients: `connect__mutmut_4` (`retry=None`) | `connection.py:247`. Richtig. `test_connect_creates_client_and_pings` prüft nur `"retry" in call.kwargs` | **Testlücke bestätigt.** Korrektur der Wirkung: Mit `retry=None` und gesetztem `retry_on_error` baut valkey-py `Retry(NoBackoff(), 1)` (`asyncio/connection.py:198-200`), also eine sofortige Wiederholung statt fünf mit Backoff, nicht „keine“. Ein kurzer Abbruch würde damit fast immer als Ausfall gemeldet. Bezug zu R4M-D-02b: Ein Test der Politik sollte nach der Produktentscheidung geschrieben werden |
| 5 | `_decode_setpoint` mit gemischten Typen: `__mutmut_15–17` | `control.py:84-86`: verlangt für **beide** Werte `int` und nicht `bool`. Richtig. Die Tests prüfen nur Fälle, in denen beide Werte falsch sind | **Testlücke bestätigt**, Wirkung geringer als Nr. 1–4: Sollwerte schreibt in der Regel `swarmq scale` mit eigener Prüfung; ein von Hand gesetzter Wert mit gemischten Typen wäre der Auslöser |

**Ergebnis der Stichprobe:** Alle fünf Einordnungen als Testlücke stimmen; in keinem Fall ist das Original falsch. Zwei Wirkungsangaben sind zu korrigieren (Nr. 1: Folge bei `release`/`renew`; Nr. 4: `retry=None` heißt eine Wiederholung ohne Backoff). Die Reihenfolge würde ich ändern: Nr. 2 vor Nr. 1, weil Nr. 2 eine geltende Entscheidung (E-005) ungeprüft trägt, während Nr. 1 nach dem Fix von R4M-D-01 ohnehin mit Tests versehen wird.

### Bezug zu den offenen LÜCKEN aus Plan R4 (Gruppe D)

| Offene LÜCKE | Berührt durch | Wirkung |
|---|---|---|
| F3-33 (`task_lock_lost` nach Transport-Retry von `lock_release.lua`) | Testlücke Nr. 1 (`LockManager.release` bei Transportfehler) und R4M-D-02b (Retry-Politik) | Nur benachbart: Dieselbe Retry-Politik wiederholt ein Skript, das schon gelaufen ist. Eine Entscheidung zu R4M-D-02b (b) sollte F3-33 mitbetrachten; kein neuer Befund |
| FS-32 (`status()` nach `handle.cancel()`) | – | nicht berührt; R4M-D-01 betrifft den Advance, nicht den Cancel-Zweig |
| FS-25, FS-26, FS-29, F3-32, F3-34 bis F3-38, F3-41 | – | nicht berührt |

Keiner der drei Verdachte widerspricht einem Test mit `entscheidung`-Marker; die Nachweis-Tests können neben `tests/unit/backend/` und `tests/unit/worker/` stehen, ohne eine geltende Regel zu brechen.
