# Gegenprüfung B: FS-06 bis FS-11, FS-22 bis FS-25

Worktree `swarmq-fehlersuche` (ddeca96), nur gelesen. Bibliotheken im `.venv`: valkey 6.1.1, aiormq 7.0.0, aio-pika 10.0.1, Python 3.14.4.

Ausgeführt:
- Unit-Nachweise mit `--runxfail` für FS-06 (Teil 2), FS-07, FS-08, FS-09, FS-10 (Unit) und FS-11. Alle scheitern aus dem im Register genannten Grund. Ein Beispiel ist FS-08: „Wartender endete nach 0.30 s … mit PublishIndeterminate“. FS-09 meldet `Channel.Close 504 'RPC timeout on frame <Basic.Cancel>'` bzw. `<Basic.Consume>`.
- Probe-Skripte `mb/handshake_orphan.py` und `mb/blocked_index.py` erneut ausgeführt. Beide geben die Ausgabe aus dem Quellbericht wieder (siehe FS-23 und FS-24).
- Die Integrationsnachweise FS-06 (Teil 1) und FS-10 habe ich nicht erneut ausgeführt. Die gemeinsame flock-Sperre hielt eine Coverage-Serie einer anderen Sitzung. Für den Realismus habe ich deshalb den Quelltext von valkey-py gegen den Test geprüft.

---

## FS-06: Wiederholung nicht idempotenter Valkey-Befehle; Leader-Renew ohne `op_timeout`

Der Befund verbindet zwei unabhängige Sachverhalte. Ich bewerte sie getrennt und empfehle, sie im Register zu trennen (FS-06a/FS-06b).

### Teil 1 (K-3): Transport-Retry nach verlorener Antwort

**Einstufung: FEHLER.** Prio P2 bleibt. Konfidenz hoch.

**Entscheidungs-Abgleich.** Die Retry-Politik ist bewusst eingeführt. Eine Entscheidung, Doppelausführung in Kauf zu nehmen, gibt es nicht.
- `docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md:44-50`: Der Spike maß nur die Verfügbarkeit („SET/GET transparent gegen Outages … 0 Fehler“). Idempotenz wurde nicht betrachtet.
- Commit 08bdc00 (2026-04-17) „fix(reliability): ValkeyBackend retry policy“ sagt: „This hardens all call sites (result storage, cancel flags, rate-limit Lua, **unique dedup**, progress HASH)“. Der Unique-Claim wurde also bewusst unter den Retry gestellt, die Folge einer verlorenen Antwort wurde aber nicht bedacht.
- `src/swarmq/lua/lock_acquire.lua:27-30` sagt im Edge-Case-Block: „owner_token already … mutex: SET NX returns nil -> 0 … caller should not retry with the same token“. Die Annahme lautet also: Kein Aufrufer wiederholt mit demselben Token. Der Transport-Retry von valkey-py tut aber genau das. Das ist eine verletzte Annahme, keine Entscheidung.
- Verlorene Antworten wurden bisher nur für den Fall bedacht, dass die Ausnahme beim Aufrufer ankommt. Dafür stehen `src/swarmq/lua/sched_fire_claim.lua:17-18` („nach einem Claim mit unklarem Ausgang (Antwort verloren) kann der Claim jedes davon tragen“) und `src/swarmq/worker/gates/schedfire.py:160-162`. Das eigene, gerade gesendete `owner`-Token gehört nicht zu den Übernahme-Tokens.

**Quelle der Erwartung.** swarmq setzt an anderer Stelle selbst die Invariante durch, dass ein Transport-Retry idempotent sein muss.
- Commit d7d94c5 (2026-10-09) „fix(worker): Claim des Ausfuehrungsschutzes idempotent bei Transport-Retry“ beschreibt genau diesen Mechanismus: „Lief guard_claim.lua schon und ging nur die Antwort verloren, sah die Wiederholung den eigenen Running-Marker als fremd“.
- Dazu gehören `src/swarmq/lua/guard_claim.lua:35-37` („Wiederholung desselben Claims (Transport-Retry nach verlorener Antwort): idempotent“) und der Contract-Test `tests/contracts/execution_guard.py:76` (`test_repeated_claim_by_the_same_owner_is_idempotent`).
- Unique-Claim, Fire-Claim und Mutex fehlen in dieser Reihe.

**Realismus.**
- (a) Ich habe den Aufrufpfad in valkey 6.1.1 nachgelesen.
  - `asyncio/client.py:637-642`: `execute_command` → `conn.retry.call_with_retry(lambda: self._send_command_parse_response(...), lambda error: self._disconnect_raise(conn, error))`.
  - `_disconnect_raise` (`:608-619`) trennt und wirft nur bei nicht gelisteten Fehlern. Danach wird der ganze Befehl erneut gesendet.
  - Der Test wirft in `Valkey.parse_response` nach dem vollständigen Lesen. Das ist dieselbe Ebene, auf der ein echter `read_response`-Fehler ankommt. Der Mock ist also treffend.
  - Auch Skripte laufen über `execute_command` (EVALSHA).
  - Die Aufrufer übergeben wirklich frische Owner (`client/schedule.py:221/101`, `scheduler/fire.py:177`, `worker/gates/lock.py:251`).
- (b) Häufigkeit: Auslöser ist ein schmales Fenster bei Valkey-Neustart, Failover, Proxy-Reset oder `CLIENT KILL`. Es gibt einen zweiten, häufigeren Auslöser, den der Bericht nicht nennt. `TimeoutError` steht in `_TRANSIENT_ERRORS` (`connection.py:36`), und `socket_timeout` beträgt 10 s (`:86`). Hängt Valkey länger als 10 s, etwa bei einem Fork- oder AOF-Stall, und führt den Befehl danach aus, wird er ebenfalls wiederholt.
- (c) Die Folgen sind still. `Client.schedule` gibt eine nie publizierte `task_id` zurück (`client/schedule.py:237-238`), das Cron-Fire geht verloren, und der Mutex wird bis `hard_timeout + 30 s` blockiert. P2 ist angemessen.

**Test-Anpassung:** keine.

### Teil 2 (MC-4): Leader-Renew ohne Zeitlimit

**Einstufung: FEHLER**, weil die zugesagte Zeitgrenze verletzt ist. Die Auswirkung fällt aber unter ein akzeptiertes Risiko. **Prio P2 → P3.** Konfidenz mittel.

**Entscheidungs-Abgleich und Quelle der Erwartung.**
- `docs/plans/2026-06-03-001-fix-hardening-round-2-plan.md:96-102` (P1-5) verlangt: „nach N = max(1, lock_ttl//heartbeat - 1) Fehlern proaktiv Step-down …, **bevor die Lock-TTL sicher abgelaufen ist**“.
- Der Code-Kommentar `src/swarmq/scheduler/loop.py:187-194` wiederholt das: „one heartbeat BEFORE the lock TTL can have expired“.
- Das Zählen von Fehlern ist entschieden. Die Zusage „vor Ablauf der TTL“ gilt nur, wenn ein Renew schnell scheitert. Dass ein Renew ohne `op_timeout` läuft, ist nirgends entschieden. In `docs/plans/2026-04-17-001-…:133` wurde `backend_op_timeout` eingeführt, ohne den Renew zu erwähnen.

**Gegenbeleg zur Schwere:** `docs/plans/2026-04-17-002-feat-phase3c-scheduler-plan.md:1390-1391`.
- Dort ist das Risiko akzeptiert: „Leader-GC-pause > TTL → Lock-Loss → Standby übernimmt → alter Leader published nochmal | gering | niedrig“.
- Die Abmilderung lautet: Consumer-Dedup und Owner-Check in `sched_commit.lua`.

**Realismus.**
- (a) Die 65 s ergeben sich so: `socket_timeout` 10 s × (1 + 5 Wiederholungen) plus Backoff von rund 3 s (cap 2, base 0,05). Das gilt nur, solange jeder neue Verbindungsversuch auch hängt. Ist nur eine Pool-Verbindung halb offen, scheitert der erste Versuch nach 10 s, und der Reconnect gelingt. Der Renew dauert dann rund 10 s und bleibt unter der TTL.
- (b) Der lange Fall braucht also einen vollständig hängenden oder abgetrennten Valkey. Dann scheitert aber auch der Poll (`sched_poll.lua` braucht Valkey, `loop.py:527`), und der alte Leader kann nicht feuern. Schaden entsteht nur im Fenster nach der Heilung, also höchstens einen Heartbeat lang. Doppel-Publishes fangen dann Schedfire-Dedup und Owner-geprüfter Commit ab, genau wie im akzeptierten Risiko.
- (c) P3.

**Test-Anpassung:** keine. Der Test prüft die zugesagte Zeitgrenze.

---

## FS-07: Timeout nach erfolgreichem Lock- bzw. Unique-Claim

**Einstufung: FEHLER.** Prio P2 bleibt, Häufigkeit eher niedrig. Konfidenz hoch für den Mechanismus.

**Entscheidungs-Abgleich.**
- `src/swarmq/worker/gates/lock.py:550-552` hält fest: „Das Zeitlimit gilt INNERHALB des Shields: laeuft es ab, wird nichts registriert (ein Blackhole zaehlt als Backend-Ausfall).“ Es stammt aus Commit d634da7 (2026-10-07), „Gate-Backend-Aufrufe zeitlich begrenzt“.
- Bedacht ist dort nur „hängt und tut nichts“, nicht „tut es und antwortet zu spät“.
- `lock.py:587-590` (KTD11 der Logging-Härtung): „die Lock-TTL heilt den Fall selbst -> WARNING“. Das betrifft gescheiterte **Freigaben** und begründet nur die Log-Stufe. Für den Acquire-Timeout trifft es keine Entscheidung. Es ist aber ein schwaches Indiz, dass verwaiste Locks bis zur TTL hingenommen werden.
- `scheduler/fire.py:181-187` hat den Kommentar „Retry on next poll; commit not called.“ Erwartet wird also ein erneuter Versuch beim nächsten Poll. Den vereitelt das verwaiste Flag.

**Quelle der Erwartung.**
- Commit 7e6abec (2026-10-09) „fix(worker): Claim mit Client-Timeout gibt den eigenen Marker im Hintergrund frei“ regelt denselben Fall für den Ausführungsschutz: „Lief guard_claim im Store, ging aber die Antwort verloren (Timeout …), stand der eigene Running-Marker bis zu seiner TTL“.
- Umgesetzt ist das in `worker/gates/execution_guard.py:145-150` mit `_release_in_background`. Die Invariante ist im Nachbarpfad also schon durchgesetzt.

**Realismus.**
- (a) `bounded_backend_call` bzw. `_run` nutzen `asyncio.wait_for` (`backend/valkey/connection.py:279-283`). Beim Abbruch trennt valkey-py die Verbindung (`asyncio/connection.py:533-539`, `:602-604`). Ein bereits gesendeter Befehl liegt dann schon im Puffer des Servers und wird ausgeführt, sobald Valkey weiterläuft. Die Fakes bilden das korrekt ab.
- (b) Der Auslöser ist ein Valkey-Stall über `backend_op_timeout`, Default 5 s, mit Ausführung danach. Das ist selten, kommt aber vor (Fork bei großem Datensatz, AOF-fsync auf langsamer Platte).
- (c) Die Wirkung ist schwer. Beim Mutex steht alles bis `hard_timeout + 30 s`, beim Cron gehen alle Fires bis zum Ablauf von `unique_for` verloren.

**Ergänzung:** Auch der Client-Pfad lässt das Flag bei einem Claim-Timeout stehen. Der Rollback (`client/schedule.py:294-306`, `:634-636`) greift erst nach dem Claim. Die Aussage im Quellbericht K-4, der „Client-Pfad mit Rollback auf BaseException“ löse den Fall, stimmt daher nur für Fehler nach dem Claim. Ein Wiederholversuch des Aufrufers bekommt die nie publizierte `task_id` als Duplikat zurück. Es wäre sinnvoll, das in FS-07 aufzunehmen.

**Test-Anpassung:** keine. Optional ein dritter Nachweis für `Client.schedule`.

---

## FS-08: Publish in aiormqs Sperren-Warteschlange hängt nach Abriss bis zur Frist und endet als `PublishIndeterminate`

**Einstufung: FEHLER.** Prio P2 bleibt. Konfidenz hoch.

**Entscheidungs-Abgleich.** Aus dem Publish-Deadline-Plan `docs/plans/2026-08-28-001-fix-publish-deadline-port-guard-logfix-plan.md`:
- R1 (`:57`): Der Publish kehrt „spätestens nach Ablauf der … Gesamtdeadline“ zurück. Das ist erfüllt.
- R11 (`:63`) und KTD4 (`:182`): Ein **Deadline**-Fehler bedeutet „möglicherweise zugestellt“. Für sich genommen deckt das ein `PublishIndeterminate` nach Fristablauf.
- Risiken (`:396`): „Ein paralleler Publish auf demselben Kanal sieht dessen Ende; die vorhandene Wiederholschleife fängt genau diesen Fall bereits ab.“ Genau diese Annahme ist für den Wartenden in aiormqs Sperre falsch.
- R10 (`:62`) und KTD3 (`:181`) sollen verhindern, dass auf einer toten Verbindung „erneut die volle Deadline“ verbrannt wird.

Später wurde nachgeschärft, und das widerlegt die Lesart „Indeterminate ist entschieden“:
- Changelog 0.12.0rc2 B24 (`website/reference/changelog.de.md:154-173`): „Ist die Nachricht auf den Socket geschrieben, endet der Publish als `PublishIndeterminate` …; riss die Verbindung vorher, wiederholt swarmq wie bisher und meldet, wenn das nicht hilft, `BrokerConnectionError`.“
- Ebenso `:181-188` (kaputter Frame vor dem Schreiben ergibt `BrokerConnectionError`).
- `website/reference/errors.de.md:466-472` und `:485-487`: `PublishIndeterminate` gilt „beim Warten auf das Publisher-Confirm“, und „nach einem Verbindungsabbruch scheitern die folgenden Publishes, die nichts senden, als `BrokerConnectionError`“.
- `broker/rabbitmq/delivery.py:188-195` (BZ-16, `BrokerPublishBlocked`): Wenn bekannt ist, dass nichts gesendet wurde, meldet swarmq das schon heute ausdrücklich als „sicher wiederholbar“.

**Quelle der Erwartung:** die B24-Zusage im Changelog und `errors.de.md`. Der Frame des Wartenden ging nie auf den Socket. Laut Zusage hätte swarmq auf einem neuen Kanal wiederholen müssen.

**Realismus.**
- (a) Ich habe es gegen aiormq 7.0.0 nachgelesen.
  - `channel.py:143-147`: Die `lock`-Property prüft `is_closed` nur beim Zugriff.
  - `:652`: `async with countdown.enter_context(self.lock)` hält danach die Referenz.
  - `:678-690`: Neue `drain_future` und neues Confirm-Future entstehen im Store.
  - `base.py:47-63`: `reject_all` leert den Store. Danach angelegte Futures löst niemand auf.
  - Der Test verwendet den echten `aiormq.Channel`. Nur die Sperre ist beobachtbar gemacht, mit derselben `is_closed`-Prüfung.
- (b) Der Auslöser tritt bei jedem Verbindungsabriss mit nebenläufigen Publishes auf. Laut N-1 wurde er im Loadsuite-Lauf beobachtet: zwei Publishes hingen 120 s. Im Worker belegt ein solcher Publish einen Semaphor-Platz, im Leader den `_fire_lock`, bis zu 120 s lang.
- (c) P2.

**Test-Anpassung:** keine. Der Test akzeptiert Erfolg auf einem neuen Kanal oder einen klaren `BrokerConnectionError`, und beides entspricht B24. Produktfrage 5 im Register ist für FS-08 durch B24 bereits beantwortet. Offen bleibt sie nur für FS-02.

---

## FS-09: Consumer-Wächter bricht `basic.cancel`/`basic.consume` nach 2 s ab, aiormq schließt den Consume-Channel

**Einstufung: FEHLER.** Die Umsetzung weicht vom Plan ab, und die Folge widerspricht dessen Ziel. **Prio P2 → P3.** Konfidenz mittel-hoch.

**Entscheidungs-Abgleich.**
- `docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md:182` begrenzt nur das Warten auf den Kanal: „`get_underlay_channel()` wird mit Timeout begrenzt, weil es auf `connection.ready()` wartet.“ Ein Abbruch der RPCs selbst ist nicht entschieden.
- Der Code-Kommentar `broker/rabbitmq/consumer_watch.py:41-45` begründet `_RPC_TIMEOUT_S` ebenfalls nur mit `get_underlay_channel`.
- `:46-49` (`_STOP_TIMEOUT_S`) sagt sogar: „Ein mitten im RPC abgebrochener Cancel oder Consume hinterließe einen Tag, von dem niemand weiß, ob der Broker ihn noch kennt.“ Der Abbruch mitten im RPC galt also als unerwünscht.
- Die Fundstellen stammen aus Commit 9f8694c (U6). Ein Kommentar, der den Kanalverlust in Kauf nimmt, findet sich nicht.

**Quelle der Erwartung:**
- Plan-Ziel (`:17`): „Ein Worker arbeitet nach einem Consumer-Timeout des Brokers weiter, und kein Task läuft dadurch doppelt.“
- AE4 (`:108`).
- BZ-20 (Register `betrieb-ueber-zeit-2026-10.md:136`): ein gemeinsamer Kanal je Worker. Ein Close trifft also alle Consumer.

**Realismus.**
- (a) Ich habe aiormq 7.0.0 nachgelesen. In `channel.py:190-214` sendet `rpc` bei `CancelledError` bzw. `TimeoutError` selbst `Channel.Close(504)`.
- Ein Unterschied zum Test: Aio-pikas `Queue.cancel` (`aio_pika/queue.py:289-294`) ruft zuerst `get_underlay_channel()` und danach `basic_cancel` auf. Läuft die 2-s-Frist schon im ersten Schritt ab, bei getrennter Verbindung, entsteht kein Close. Das ist der beabsichtigte Fall. Der Test bildet nur den zweiten Schritt nach, und dort ist das Verhalten real.
- (b) Es braucht beides: eine Neuanmeldung, die selten ist (4.3-`consumer_timeout`, Restore-Fehler), und einen Broker, der länger als 2 s für das RPC braucht. Beides tritt zwar gerade in gestörten Lagen gemeinsam auf, bleibt aber selten.
- (c) Der Ausführungsschutz ist per Default aktiv (`config.py:364`, `execution_guard: bool = True`). Statt einer Doppelausführung entstehen dann Zurückstellungen und `delivery_settle_failed`. Deshalb P3.

**Test-Anpassung:** keine.

---

## FS-10: DLQ-Kopien behalten `x-delivery-count`/`x-acquired-count`

**Einstufung: FEHLER.** Prio P2 bleibt. Konfidenz hoch.

**Entscheidungs-Abgleich.**
- BZ-08/BZ-12 (Register `betrieb-ueber-zeit-2026-10.md:128`) entscheidet: „`x-death` bzw. `x-swarmq-dead-lettered-at` gilt als bewusstes Replay“. Das betrifft den Done-Marker, nicht den Zustellzähler.
- Plan `2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md` (MR 12) nennt Restlücken für UI-Replays:
  - `:463`: „Ein UI-Move setzt keine Base.“
  - `:487`: `max_age`/`enqueued-at` wird „für UI-Replays … dokumentiert“.
  - Zustellzähler fehlen in dieser Liste. Akzeptiert ist das also nicht.
- `tests/contracts/publisher_consumer.py:148` (`test_dead_letter_is_inspectable_with_all_original_headers`) und KTD9 im Plan 2026-08-26-001 (`:201`): „Dead-Letter landet inspizierbar mit allen Original-Headern“. Der Test prüft nur swarmq-Header (`x-swarmq-unique-hash`, `enqueued-at`). Für Broker-Zähler ist das keine Entscheidung. Ein Fix sollte aber beachten, dass der Zähler als Diagnose nützlich ist, etwa indem er ihn unter eigenem Namen weiterführt.

**Quelle der Erwartung.**
- Changelog 0.12.0rc1 BZ-08 (`website/reference/changelog.de.md:512-531`): „‚Move messages‘ in der RabbitMQ-UI, ein Shovel, swarmv … legen eine dead-gelettete Nachricht … zurück“ und werden jetzt ausgeführt.
- Die Migrationsanleitung empfiehlt den Shovel-Weg (`website/explanation/delivery-semantics.de.md:860-862`).
- Die Invariante im Nachbarpfad:
  - `delivery-semantics.de.md:172-175`: Retry-, Rate-Limit- und Lock-Kopien „lassen die Zustellzähler des Brokers … weg“.
  - Changelog `:1850-1858`: „RabbitMQ 4 behält einen vom Publisher gesetzten `x-delivery-count`“.
  - Changelog `:753-756`: `dlq`-Kopien „tragen keine Zustellzähler“.
- Nur die DLQ-Kopie des Workers ist ausgenommen.

**Realismus.**
- (a) Laut Register (`fehlersuche-runde-2-2026-10.md:98`) wurde auf RabbitMQ 4.3.6 belegt, dass der Zähler erhalten bleibt. Die Abkürzung im Integrationstest, eine Nachricht direkt mit `x-delivery-count: 19` zu publizieren, entspricht dem Zustand nach 19 echten Abstürzen in einer Quorum-Queue.
- (b) Betroffen ist gerade die Klasse von Nachrichten, die man nach einem Poison-Fix zurückspielt: Hänger und Abstürze. Für die DLQ-Kopie gibt es mit `swarmq dlq retry` einen funktionierenden Ausweg.
- (c) P2.

**Test-Anpassung:**
- Der Unit-Nachweis legt den Fix auf die Broker-Grenze fest: Die Header der Kopie dürfen keine Zähler tragen. Ein gleichwertiger Fix im Gate wäre zum Beispiel, die Zähler bei erkanntem Replay (`x-swarmq-dead-lettered-at`) zu ignorieren. Er ließe den Unit-Test rot, obwohl das Verhalten stimmt.
- Verbindlich sollte deshalb der Integrationsnachweis sein, der am Verhalten prüft. Den Unit-Test würde ich als Hinweis führen oder nach der Fix-Entscheidung anpassen.
- Der Fall „unter dem Limit (3)“ ist eher eine Budget-Frage als ein Fehler.

---

## FS-11: BZ-11-Zurückhaltung legt einen Cron eines auf 0 skalierten Pools still

**Einstufung: ENTSCHEIDUNG.** Die Frage ist, ob die Entscheidung überdacht werden soll; dafür gibt es ein neues Argument. **Prio P2 → Produktfrage (kein Fehler).** Konfidenz hoch.

**Entscheidungs-Abgleich.** Das Verhalten ist wörtlich so entschieden und dokumentiert:
- Register BZ-11 (`docs/befunde/betrieb-ueber-zeit-2026-10.md:131`): „Der Leader feuert keinen Cron, den **kein lebender Worker meldet** und den er selbst nicht kennt (eigenes Event).“
- Präzisierung durch Georg (`docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:48`): „Termine werden zurückgehalten, nicht verworfen.“
- Plan MR 11 (`:450-458`): fünf Bedingungen, Halten per Commit auf `now + seen_interval`.
- Nutzer-Doku `website/guides/scheduling-and-retries.de.md:303-328` („Crons, die niemand meldet, werden zurückgehalten“), `website/reference/logging.de.md:737`, `configuration.de.md:158` und `signals.de.md:88`.
- Test `tests/unit/scheduler/test_unowned.py:73` (`test_unowned_cron_is_held_instead_of_fired`) schreibt das Verhalten fest.

Auch der Prune nach 24 h ist entschieden:
- `docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md:88` (R11): „seit `scheduler_prune_after_s` von keinem Worker mehr gemeldet wird, wird entfernt“.
- `:110` (AE6): „Bs Crons bleiben, solange B innerhalb von `scheduler_prune_after_s` wieder meldet“.
- Nach dem Prune legt der nächste startende Worker den statischen Cron wieder an. Das heilt sich also selbst.

**Neues Argument (gültig):** Scale-to-zero über die Queue-Länge (KEDA) ergibt eine Verklemmung. Ohne Fire bleibt die Queue leer, und der Pool skaliert nie hoch. Weder Plan, Register noch Doku erwähnen KEDA oder Scale-to-zero als Einsatzform; gesucht habe ich nach `keda`, `scale-to-zero` und „auf 0 skal“. Die Entscheidung zielte auf entfernte Crons (BZ-11 Befundtext `:30`).

**Realismus:** Der Test bildet das Szenario korrekt ab: letzte Meldung vor 181 s, Lebendigkeitsfenster 3 × 60 s. Der Auslöser hängt vom Deployment ab.

**Test-Anpassung:** Der Nachweis prüft das Gegenteil der Entscheidung (`xfail strict`). Als „Befund offen“ ist er falsch geführt. Ich empfehle, ihn als Produktfrage 2 zu führen, ohne strict-xfail-Nachweis oder mit Begründung „Entscheidung offen“. Die Erwartung sollte erst nach Georgs Entscheidung festgelegt werden. Optionen sind feuern mit Warnung, Halten mit Obergrenze oder ein Schalter.

---

## FS-22 (P3): Leader-Rücktritt ohne zweiten Kandidaten kostet bis zu 2 × Lock-TTL

**Einstufung: ENTSCHEIDUNG.** P3 → kein Befund. Konfidenz hoch.

**Belege.**
- Plan `2026-10-09-2314-…:570-573`: „Rücktritt bei mindestens 2 Fehlern und mindestens `scheduler_publish_stepdown_s` … Danach Abkühlzeit 2 × Lock-TTL, ohne Handover-Marker.“
- `:74`: „Alle übrigen `BrokerConnectionError` zählen weiter“. Ein Broker-Ausfall zählt also bewusst.
- Nutzer-Doku `website/reference/configuration.de.md:120` („danach bewirbt er sich `2 × SWARMQ_SCHEDULER_LOCK_TTL` nicht … `0` schaltet den Rücktritt ab“) und `logging.de.md:736`.
- Code `scheduler/stepdown.py:1-16`.

**Begründung.**
- Die Rotation bei Fehlern, die jeden Kandidaten gleich treffen, wurde betrachtet (`_NOT_COUNTED`, Plan `:571`). Für Verbindungsfehler wurde bewusst anders entschieden.
- Mit einem einzigen Kandidaten ist der Schaden höchstens 60 s Verzug nach der Erholung, und es geht nichts verloren. Ein Abschalter existiert.
- Das Argument, dass es sich nur bei unterschiedlichen Brokern lohnt, ist nicht neu: Es ist der dokumentierte Zweck (BZ-10).

**Test-Anpassung:** entfällt (kein Nachweis).

---

## FS-23 (P3): Im Hintergrund gesperrter Slot bleibt in der Free-Queue; Sperre wird nie aufgehoben

**Einstufung: FEHLER** (eingeschränkt). Der Teil „nie aufgehoben“ ist **ENTSCHEIDUNG**. P3 bleibt. Konfidenz hoch.

**Belege.**
- CPU-Plan `docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:259` (KTD3): „Bleibt das Kind unreapbar (D-State), **bleibt der Slot blockiert**, Log `compute_child_unreaped`“.
- Risikotabelle `:388`: „Synchroner Kill mit Sentinel-Wartezeit, Slot bleibt blockiert“.
- Hint im Code `worker/compute/pool.py:602`: „slot stays blocked“.
- Eine dauerhafte Sperre ist also gewollt. BZ-27 („Abkühlzeit statt Dauersperre“) betrifft die Supervisor-Slots im `process_manager`, nicht den Rechen-Pool.

**Fehleranteil.** Wird der Kill im Hintergrund ausgelöst (`_recycle`/`_end_child`, `aclose`), nachdem `run()` den Index schon zurückgegeben hat (`pool.py:255-258`), bleibt der Index in `_free`. `run()` prüft `blocked` nach `_free.get()` nicht (`:216-222`). Das widerspricht KTD3 „Slot bleibt blockiert“. Reproduziert mit `mb/blocked_index.py`: „slot0.blocked: True … free queue: [0, 1] -> Slot 0 wird trotz Sperre weiter vergeben“.

**Realismus:** Der D-State ist selten. Die Folge ist ein weiterer Job auf dem gesperrten Slot und ein ungenutztes Ersatzkind. Gering.

**Test-Anpassung:** Ein Nachweis sollte nur „Index nicht mehr in `_free`“ prüfen, nicht die Aufhebung der Sperre.

---

## FS-24 (P3): Gescheiterter Handshake erzeugt asyncio-ERROR und doppelt gezählten Kindtod

**Einstufung: FEHLER** (P3). Konfidenz hoch.

**Entscheidungs-Abgleich:** keine Entscheidung gefunden (gesucht in Plänen, `docs/solutions` und Logging-Plänen).

**Quelle der Erwartung:** swarmq behandelt unabgeholte Future-Ausnahmen, die asyncio auf ERROR loggt, durchgehend als Fehler:
- Changelog 0.12.0rc2 (`website/reference/changelog.de.md:189-194`): „Kein ‚Future exception was never retrieved‘ mehr …; swarmq holt jetzt die Ausnahme genau dieses Futures ab.“
- Changelog `:1618-1626`: „… endet nicht mehr als ‚Task exception was never retrieved‘“.
- `docs/solutions/patterns/worker-cancel-safe-teardown-detached-tasks-2026-06-01.md` mit dem Symptom in der Frontmatter.

**Realismus.**
- Unter Python 3.14 meldet `asyncio.shield` die Ausnahme eines verwaisten inneren Futures über `_log_on_exception`; im Quelltext nachgelesen.
- Reproduziert mit `mb/handshake_orphan.py`: „loop errors: ['ChildDied exception in shielded future']“, `died=1`.
- Das tritt genau bei toten oder überlasteten Forkservern auf, je Nachricht, und flutet dann das Log.
- `stats.died` ist nicht exportiert. Dieser Teil ist kosmetisch.

---

## FS-25 (P3): Fortschritt aus dem Kind: ein Task je Nachricht, Reihenfolge unbestimmt

**Einstufung:** Reihenfolge → **LÜCKE**. „Unbegrenzt viele Tasks“ → **NICHT HALTBAR**. P3 → P3 (niedrig). Konfidenz mittel.

**Belege.**
- CPU-Plan `:314`: „progress (best effort) → Valkey“.
- `:521`: „`update_progress` im Kind … wird gedrosselt wie heute“.
- `src/swarmq/task.py:272` (`_PROGRESS_MIN_INTERVAL_S = 0.5`) und `:363-370`: „Throttled at 500ms per Task instance … Progress is best-effort“.
- Die Drossel greift im Kind vor dem Shim, denn `worker/compute/child.py:36-47` stellt nur `set_progress`/`publish_progress` bereit. Je Kind entstehen also höchstens rund 2 Fortschritts- und 2 Publish-Nachrichten pro Sekunde, plus die Endmeldung. Eine unbegrenzte Zahl von Tasks gibt es nicht.
- Eine Reihenfolge-Zusage existiert nicht. Auch im IO-Modus laufen HSET und Publish bewusst außerhalb der Sperre (`task.py:392-397`), sequenziell nur pro Aufrufer.

**Realismus:** Eine Umordnung verlangt, dass ein HSET mehr als 500 ms langsamer ist als der folgende. Nur die Endmeldung kommt ungedrosselt kurz nach einer 90-%-Meldung. Möglich ist das bei einem Valkey-Stall oder Reconnect. Das Ergebnis selbst ist davon nicht betroffen.

---

## Übersicht

| ID | Einstufung | Prio alt→neu | Kernbeleg |
|---|---|---|---|
| FS-06 (Teil 1, Retry) | FEHLER | P2→P2 | d7d94c5 / `guard_claim.lua:35-37`: Transport-Retry muss idempotent sein; `lock_acquire.lua:27-30` nimmt „kein Retry mit gleichem Token“ an |
| FS-06 (Teil 2, Renew) | FEHLER (Wirkung im akzeptierten Risiko) | P2→P3 | Plan 2026-06-03-001 P1-5 „bevor die Lock-TTL sicher abgelaufen ist“; Risiko akzeptiert in Plan 2026-04-17-002:1391 |
| FS-07 | FEHLER | P2→P2 | 7e6abec (Guard gibt nach Timeout im Hintergrund frei); `fire.py` „Retry on next poll“ |
| FS-08 | FEHLER | P2→P2 | Changelog rc2 B24 (`changelog.de.md:154-173`), `errors.de.md:485-487`; Plan 2026-08-28-001 Risiken `:396` |
| FS-09 | FEHLER | P2→P3 | Plan 2026-10-08-1541 `:182` begrenzt nur `get_underlay_channel`; `consumer_watch.py:46-49` |
| FS-10 | FEHLER | P2→P2 | Changelog BZ-08 (`changelog.de.md:512-531`); `delivery-semantics.de.md:172-175` (Kopien ohne Zähler) |
| FS-11 | ENTSCHEIDUNG (Neues Argument: KEDA) | P2→Produktfrage | Register BZ-11 `betrieb-ueber-zeit:131`; Plan `:48` „zurückgehalten, nicht verworfen“; Guide `scheduling-and-retries.de.md:303-328`; `test_unowned.py:73` |
| FS-22 | ENTSCHEIDUNG | P3→entfällt | Plan 2026-10-09-2314 `:572-573`; `configuration.de.md:120` |
| FS-23 | FEHLER (eingeschränkt); Dauersperre ist ENTSCHEIDUNG | P3→P3 | CPU-Plan KTD3 `:259`, `:388` „Slot bleibt blockiert“; reproduziert |
| FS-24 | FEHLER | P3→P3 | Changelog rc2 `:189-194` (never-retrieved gilt als Fehler); reproduziert |
| FS-25 | LÜCKE (Reihenfolge) / NICHT HALTBAR („unbegrenzt“) | P3→P3 (niedrig) | `task.py:272`, `:363-370` (500 ms Drossel, best effort); CPU-Plan `:314`, `:521` |

## Empfohlene Änderungen am Register und an den Nachweisen

1. FS-06 in Retry-Idempotenz (P2) und Renew-Zeitlimit (P3) teilen.
2. FS-07 um den Client-Pfad ergänzen (Claim-Timeout ohne Rollback).
3. FS-08: Produktfrage 5 gilt dafür als durch B24 beantwortet.
4. FS-10: Den Integrationsnachweis als verbindlich führen. Der Unit-Nachweis legt den Fix-Ort fest.
5. FS-11: Aus der Befundliste in die Produktfragen verschieben. Den strict-xfail-Nachweis erst nach der Entscheidung festlegen; er widerspricht heute dem entschiedenen und dokumentierten Verhalten.
6. FS-22: als entschieden streichen. FS-25: „unbegrenzt“ streichen.

## Quellen mit Entscheidungen außerhalb von `docs/befunde` (für ein künftiges Entscheidungsregister)

- `docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md`:
  - `:40-55` Präzisierungen Georgs (u. a. BZ-11 „zurückgehalten, nicht verworfen“, BZ-12 Restlücke, BZ-25)
  - `:74-75` (`PublishIndeterminate` zählt nicht für den Rücktritt, Georg 2026-10-11)
  - `:450-458` (BZ-11-Kriterien)
  - `:463` und `:487` (Restlücken bei UI-Replays)
  - `:570-577` (Rücktritt, Abkühlzeit 2 × TTL)
- `docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md`:
  - `:88-89` (R11/R13 Cron-Prune)
  - `:110` (AE6)
  - `:182` (Wächter: nur `get_underlay_channel` begrenzt)
  - `:233-235` (Prune-Schalter, nie gemeldete Crons)
- `docs/plans/2026-08-28-001-fix-publish-deadline-port-guard-logfix-plan.md`:
  - R1, R10, R11, R12 (`:57-65`)
  - KTD3/KTD4 (`:181-182`)
  - Risiken `:394-396`
- `docs/plans/2026-06-03-001-fix-hardening-round-2-plan.md:96-110`: P1-5 Rücktritt vor TTL-Ablauf, P1-6 Inflight-TTL.
- `docs/plans/2026-04-17-002-feat-phase3c-scheduler-plan.md:1390-1391`: akzeptiertes Split-Brain-Risiko.
- `docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md`:
  - KTD3 `:259` (Slot bleibt blockiert)
  - Risikotabelle `:388`
  - `:314` und `:521` (Fortschritt best effort, gedrosselt)
- `docs/plans/2026-08-26-001-refactor-restrukturierung-0-2-0-plan.md:201`: KTD9 „Dead-Letter … mit allen Original-Headern“.
- `docs/plans/2026-10-06-0831-refactor-logging-haertung-plan.md:221`: KTD11 Level-Regel („Lock-TTL heilt“ als WARNING-Begründung).
- `docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md:44-53`: Herkunft der Valkey-Retry-Politik.
- Nutzer-Doku:
  - `website/reference/changelog.de.md:154-173` (B24), `:181-194`, `:512-531` (BZ-08), `:753-756`, `:1618-1626`, `:1850-1858`
  - `website/reference/errors.de.md:465-487`
  - `website/explanation/delivery-semantics.de.md:158-175`, `:850-862`
  - `website/guides/scheduling-and-retries.de.md:303-328`
  - `website/reference/configuration.de.md:120`, `:158`
  - `website/reference/logging.de.md:736-737`
- Code-Kommentare:
  - `src/swarmq/lua/lock_acquire.lua:27-30`
  - `src/swarmq/lua/sched_fire_claim.lua:17-18`
  - `src/swarmq/lua/guard_claim.lua:35-37`
  - `src/swarmq/worker/gates/schedfire.py:160-162`
  - `src/swarmq/worker/gates/lock.py:550-552`, `:587-590`
  - `src/swarmq/broker/rabbitmq/consumer_watch.py:41-49`
  - `src/swarmq/scheduler/loop.py:187-194`
  - `src/swarmq/scheduler/stepdown.py:1-16`, `:47-55`
  - `src/swarmq/task.py:272-273`, `:363-370`, `:392-397`
- Commits:
  - 08bdc00 (Retry-Politik deckt bewusst auch den Unique-Dedup ab)
  - d7d94c5 und 7e6abec (Idempotenz bzw. Freigabe nach verlorener Antwort beim Guard)
  - d634da7 (Gate-Timeouts)
  - 9f8694c (Consumer-Wächter)
  - d98453e (proaktiver Rücktritt)
- Tests, die Verhalten festschreiben:
  - `tests/unit/scheduler/test_unowned.py:73`
  - `tests/contracts/execution_guard.py:76`
  - `tests/contracts/publisher_consumer.py:148`
