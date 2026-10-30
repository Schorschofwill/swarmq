---
title: Upgrade-Checkliste – private APIs von valkey-py, aiormq, aio-pika, pytest und asyncio, an denen swarmq hängt
date: 2026-10-19
category: docs/solutions/tooling-decisions
module: backend/valkey/connection.py / tests/kosten/valkey/conftest.py / broker/rabbitmq/blocked.py / broker/rabbitmq/delivery.py / broker/rabbitmq/publish_phase.py / broker/rabbitmq/header_guard.py / broker/rabbitmq/robust_channels.py / broker/rabbitmq/underlay.py / worker/lifecycle.py / broker/rabbitmq/consumer_watch.py / broker/rabbitmq/connection.py / client/schedule.py / logging_stdlib.py / logging_file.py / logging_queue.py / worker/compute/child.py / worker/compute/forkserver.py / tests/unit/test_slow_test_report.py
problem_type: tooling_decision
component: dependencies
severity: medium
applies_when:
  - valkey-py, aiormq, aio-pika, pytest oder Python werden angehoben (auch Patch-Versionen)
  - ein Plan will eine weitere private API einer Bibliothek nutzen
  - nach einem Abhängigkeits-Upgrade scheitern Tests zu Verbindungsaufbau, Blocked-Zustand, Kanal-Wiederherstellung, gelöschter konsumierter Queue, Abbruch-Markierung oder Slow-Report
  - nach einem aiormq- oder aio-pika-Upgrade erscheinen die WARNINGs channel_observer_release_unavailable oder consumer_recovery_disable_unavailable, oder die Leckmessungen in test_channel_callbacks_flat.py steigen
  - nach einem Python-Upgrade scheitern Tests zu Logging-Listenern, Fork-Hooks oder dem Forkserver der Rechenkinder
tags: [abhaengigkeiten, upgrade, valkey-py, aiormq, aio-pika, pytest, asyncio, logging, multiprocessing, private-api, checkliste]
entscheidungen: [E-001, E-019, E-025, E-041, E-042, E-053, E-075, E-078, E-083, E-089]
---

# Upgrade-Checkliste: private Bibliotheks-APIs

swarmq hängt an einigen privaten oder undokumentierten Stellen seiner
Abhängigkeiten. Ein Upgrade kann sie still ändern. Vor jedem Anheben der
betroffenen Bibliothek (auch eines Patch-Releases) die Einträge unten am neuen
Quelltext prüfen und die genannten Tests laufen lassen. Neue Kopplungen dieser
Art kommen hier dazu, mit Fundstelle, Prüfung und Rückfall.

Stand: swarmq 0.14 (Branch `feat/0-14`), valkey-py 6.1.1, aiormq 7.0.0,
aio-pika 10.0.1, pytest 9.1.1, Python 3.14.

Stand 2026-10-30 (Plan 2026-10-29-1945, U8; Branch `chore/aiormq-7-2`): aiormq 7.2.4
und aio-pika 10.2.1 (Grenzen `aiormq>=7.2.4,<7.3`, `aio-pika>=10.2.1,<10.3`). Die
aiormq- und aio-pika-Einträge sind gegen diese Versionen geprüft; Zeilenangaben
„in 7.2.4“ bzw. „in 10.2.1“ sind neu, ältere Angaben „in 7.0.0“/„in 10.0.1“ bleiben als
Fundstelle der damaligen Prüfung stehen. Neu: Frame-Locals in `basic_publish`,
`_raise_on_channel_close`, `closing` als Beobachter, `TaskWrapper`, Kanalöffnen ohne
Beobachter-Leck (`underlay.py`) und Consumer-Wiederherstellung von aio-pika. Entfernt:
der FS-08-Wächter mit der WARNING `publish_channel_closing_unreadable`.

## valkey-py

### `Connection._connect` (privat, seit 0.14, U3)

- **Wo:** `_mark_tcp_connect_timeouts` in `src/swarmq/backend/valkey/connection.py`,
  aufgerufen in `make_marked_connection` von `_retry_handshake`. Wickelt
  `connection._connect` jeder Pool-Verbindung: Ein `TimeoutError` wird zu
  `_TcpConnectTimeout` (weder `OSError` noch `ValkeyError`).
- **Annahmen über valkey-py:**
  - `Connection.connect` ruft den TCP-Aufbau über `self._connect()` auf, innerhalb von
    `self.retry.call_with_retry(...)`, und wiederholt nur Fehler aus
    `retry._supported_errors`.
  - `Connection.connect` wickelt jede andere Ausnahme mit
    `except Exception as exc: raise ConnectionError(exc) from exc`. Daraus wird die
    Befehls-Retry (`retries=5`) wieder wirksam, und `ValkeyConnection._ping_until_deadline`
    findet `_TcpConnectTimeout` über `__cause__`.
  - Ein Zeitablauf beim TCP-Aufbau kommt aus `_connect` als `TimeoutError`
    (`socket.timeout`/`asyncio.TimeoutError`); `Connection.connect` macht daraus sonst
    `TimeoutError("Timeout connecting to server")`.
  - `ConnectionPool.get_connection` baut eine neue Verbindung außerhalb der Befehls-Retry.
- **Prüfen:** `valkey/asyncio/connection.py`, Methode `Connection.connect` (in 6.1.1 ab
  Z. 306), und `valkey/asyncio/retry.py`.
- **Tests:** `uv run just unit -k "valkey_handshake or valkey_connect_timeout or befund_f4_1"`
  (Fake-Uhr und drei `timing`-Tests mit echtem SYN-Blackhole), dazu
  `tests/chaos/test_backend_partition.py` als Regressionswächter. Die Tests patchen
  `valkey.asyncio.connection.Connection._connect` der echten Bibliothek.
- **Rückfall:** Fehlt `_connect`, lässt `_mark_tcp_connect_timeouts` die Verbindung
  unverändert. Dann wiederholt der Treiber Zeitabläufe beim TCP-Aufbau wieder intern
  (Stand 0.13: bis zu 30 TCP-Versuche je Befehl), und `connect()` endet gegen einen Host,
  der Pakete verwirft, wieder nach rund 33 s statt bis zur Frist zu warten. Kein
  Absturz, aber E-075 (Nachtrag 0.14) wäre verletzt; die Tests oben werden rot.

### Instanz-Patches an `ConnectionPool` und `Connection` (öffentliche Namen)

`_retry_handshake` ersetzt `pool.make_connection` und `pool.get_connection` der
Instanz, `_mark_handshake_resets` ersetzt `connection.on_connect` (F4-14, E-075).
Die Namen sind öffentlich, das Zusammenspiel ist es nicht: Prüfen, ob
`get_connection` weiter `make_connection` aufruft und `on_connect` nach dem TCP-Aufbau
läuft. Tests wie oben.

### `AbstractConnection.send_packed_command` als Zählpunkt (undokumentiert, Testwerkzeug, Plan 2026-10-24-1118, U4)

- **Wo:** Fixture `leitung` in `tests/kosten/valkey/conftest.py`. Sie ersetzt per
  `monkeypatch` `valkey.asyncio.connection.AbstractConnection.send_packed_command` der
  Klasse durch eine Hülle, die jeden Aufruf als einen Roundtrip zählt und die
  RESP-Arrays darin als Befehle. Daraus entstehen die Valkey-Werte in
  `tests/kosten/basislinie.toml` (`valkey.roundtrips`, `valkey.befehle`, `valkey.<BEFEHL>`).
- **Annahmen über valkey-py:**
  - Jeder Schreibvorgang einer Verbindung läuft über `send_packed_command`: Einzelbefehle
    über `send_command`, Pipelines und MULTI-Blöcke als ein Aufruf mit allen Befehlen
    (`Pipeline._execute_transaction` und `_execute_pipeline`, in 6.1.1 `client.py`
    Z. 1409 und 1479), Pub/Sub über `send_command` der Pub/Sub-Verbindung, Handshake
    (`on_connect`) und Health-Check-`PING` über `send_command` derselben Verbindung.
  - Keine Unterklasse (`Connection`, `UnixDomainSocketConnection`, SSL) überschreibt
    `send_packed_command`; sonst greift die Hülle an der Klasse nicht.
  - Das Argument ist `bytes`, `str` oder eine Folge von `bytes`, deren Verkettung
    vollständige RESP-Arrays aus Bulk-Strings ergibt (`pack_command`, `pack_commands`).
  - `ScriptPipeline` (`src/swarmq/backend/valkey/script_pipeline.py`) schreibt über
    dieselbe Methode; ein Stapel zählt als ein Roundtrip.
- **Prüfen:** `valkey/asyncio/connection.py`, `AbstractConnection.send_packed_command`
  und `send_command` (in 6.1.1 ab Z. 500), und die Aufrufer in `valkey/asyncio/client.py`
  (`grep -n send_packed_command`).
- **Tests:** `uv run just kosten-valkey`. Der Test
  `test_a_multi_block_counts_as_one_roundtrip_with_every_command` prüft die Hülle selbst
  (ein MULTI-Block, ein Roundtrip, vier Befehle); die Szenarien vergleichen exakt mit der
  Basislinie.
- **Rückfall:** keiner im Produktcode, die Hülle ist reines Testwerkzeug. Ändert valkey-py
  den Schreibweg, wird `kosten-valkey` rot (Hüllentest oder Abweichung von der Basislinie)
  oder zählt zu wenig. Dann die Hülle an den neuen Schreibweg hängen und erst danach die
  Basislinie mit `uv run just kosten-basislinie "<Grund>"` nachführen, nicht umgekehrt.

## aiormq

### `Connection.__connection_unblocked` (privat, seit 0.12, BZ-16; seit 0.14 auch U6 und F4-21)

- **Wo:** `unblocked_event`, `blocked_waiters` und `blocked_close_waiters` in
  `src/swarmq/broker/rabbitmq/blocked.py` lesen das Attribut
  `_Connection__connection_unblocked` (ein `asyncio.Event`, gesetzt heißt „nicht
  blockiert“).
- **Annahmen:** aiormq leert das Event bei `connection.blocked`
  (`__handle_connection_blocked`) und setzt es bei `connection.unblocked` und beim Start
  des Lesers einer neuen Verbindung (`__reader`).
- **Rückfall:** fail-open (E-025). Ist das Attribut nicht lesbar, loggt swarmq einmal
  `publish_blocked_state_unreadable` und publiziert ungeprüft wie vor 0.12; das Close
  eines verworfenen Kanals läuft sofort wie vor 0.14.
- **Stand 7.2.4:** unverändert (`__connection_unblocked` Z. 438, geleert Z. 720, gesetzt
  Z. 726 und 729; `Connection.ready()` wartet weiter auf Verbindung und Freigabe, Z. 445–447).

### `Connection.closing` (Future, seit 0.14, U6 und F4-21; seit aiormq 7.2 ein Beobachter je Zugriff)

- **Wo:** `blocked_waiters` liest `connection.transport.connection.closing`, beim
  Verwurf des Publish-Kanals über `blocked_close_waiters` (PF-19 a) und im Warten eines
  Publish auf die Freigabe (`_wait_while_blocked` in `broker/rabbitmq/delivery.py`,
  F4-21). `wait_until_unblocked` wartet auf das Unblocked-Event **oder** dieses Future,
  beim Close höchstens die Publish-Frist, beim Publish ohne eigene Frist (die
  Publish-Frist liegt außen). Ein erledigtes `closing` gilt wie „nicht verbunden“: Der
  Publish wartet dann in `_publish_attempts` auf den Reconnect.
- **Annahmen:** `closing` ist ein `asyncio.Future`, das erledigt ist, sobald die
  aiormq-Verbindung endet. aiormq setzt das Unblocked-Event beim Schließen **nicht**
  (in 7.0.0); täte es das künftig, endete das Warten ebenso, `closing` schadet nicht.
- **Rückfall:** Ist `closing` kein `asyncio.Future`, liefert `blocked_close_waiters`
  `None`, und der Kanal wird sofort geschlossen wie vor 0.14 (fail-open). Im Alarm
  erscheinen dann wieder `publish_channel_close_incomplete` und die aiormq-ERROR „Got
  frame for closed channel“. Ein wartender Publish wartet dann allein auf das Event wie
  bis 0.13, ohne eigene WARNING: Reißt die Verbindung im Alarm, endet er erst mit der
  Publish-Frist als `BrokerPublishBlocked` (F4-21 wieder offen).
- **Stand 7.2.4 (aiormq #240):** `closing` ist eine Property von `aiormq.base.Base`
  (Z. 124), die bei jedem Zugriff einen **neuen** Beobachter liefert. Der hängt einen
  Callback an das innere Future `_closing` (Z. 118), bis er selbst endet; erst wenn
  `_closing` erledigt ist, liefert `closing` das innere Future selbst. Wer `closing`
  liest und nicht bis zum Ende der Verbindung wartet, muss seinen Beobachter darum
  abbrechen, sonst bleibt je Lesen ein Callback an der Verbindung (Leck).
  `wait_until_unblocked` in `blocked.py` bricht seinen Beobachter seit U5 nach jedem
  Warten ab (KTD2). Ein abgebrochener Beobachter berührt `_closing` nicht.
- **Prüfen:** `aiormq/connection.py`, Attribut `__connection_unblocked` (in 7.0.0
  Z. 433, gesetzt und geleert Z. 692–705), und `aiormq/base.py`, `closing` (Z. 96,
  `_create_closing_future`; in 7.2.4 Z. 118–124 und die Beobachter-Logik darunter).
- **Tests:** `uv run just unit -k "publish_blocked or rabbitmq or befund_f4_21"`
  (`TestChannelCloseAfterUnblocked`, darunter der Fall „`closing` unlesbar“, und die
  vier F4-21-Tests mit Fake-Transport) und
  `uv run just integration -k "bz16 or blocked_cancel or pf19 or befund_f4_21"` gegen den
  echten Broker. Die Beobachter-Semantik halten
  `tests/unit/broker/test_aiormq_7_2_semantics.py` (ohne Broker) und
  `tests/integration/broker/test_channel_callbacks_flat.py` (Callbacks an `_closing`
  über 10 000 Publishes und Wartezeiten flach) fest.

### Kanal-`closing` über `get_underlay_channel()` (undokumentiert, 0.12.2 bis 0.15.x, FS-08) – entfernt

- **Entfernt 2026-10-30 (Plan 2026-10-29-1945, U5, KTD2; Nachtrag E-042):** Der
  FS-08-Wächter (`_closing_of`, `_publish_against_closing` in `delivery.py`) und seine
  WARNING `publish_channel_closing_unreadable` sind weg. aiormq 7.2 prüft `is_closed` in
  der Kanal-Sperre auch nach dem Erwerb (#240): Ein Publish, der beim Abriss in ihrer
  Warteschlange steht, endet selbst mit `ChannelInvalidStateError`. Unter 7.2 hätte der
  Wächter je Publish einen `closing`-Beobachter an der Verbindung gelassen (Eintrag
  oben). swarmq liest beim Publish kein `closing` mehr.
- **Prüfen bei einem aiormq-Upgrade:** ob die Sperre (`Channel.lock`, eine Property um
  `__lock`) weiter vor und nach dem Erwerb `is_closed` prüft. Fällt die zweite Prüfung
  weg, kehrt der FS-08-Hänger zurück (Warten in der Kanal-Sperre bis zur Publish-Frist,
  dann `PublishIndeterminate`).
- **Tests:** `uv run just unit tests/unit/broker/test_aiormq_7_2_semantics.py`
  (`test_the_channel_lock_checks_is_closed_before_and_after_acquiring`) und
  `uv run just integration tests/integration/broker/test_befund_fs08_lock_waiter_aiormq_7_2.py`
  (der Wartende endet nach dem Proxy-Abriss sofort, nicht gesendet).

### Frame-Locals `sent` und `drain_future` in `Channel.basic_publish` (undokumentiert, seit 0.12, B24; `sent` seit aiormq 7.2)

- **Wo:** `publish_phase.py` (`sent_before_failure`, `enqueued_before_failure`, `_enqueued`)
  sucht im Traceback einer gescheiterten Publish-Ausnahme den Rahmen von aiormqs
  `basic_publish` (Funktionsname, Modul `aiormq*`) und liest dessen Locals. Davon hängt
  die Einstufung ab: nicht gesendet (wiederholen), gesendet (`PublishIndeterminate`),
  eigener Abbruch vor dem Einreihen (`PublishCancelledUnsent`); E-041, E-042, B24.
- **Annahmen (in 7.2.4 Z. 939–971):** `basic_publish` läuft unter
  `async with (self._raise_on_channel_close(), asyncio.timeout(...))` und darin unter
  `async with self.lock`. Erst unter der Sperre werden `drain_future = None` und
  `sent = False` gebunden; `sent = True` steht nach `await self.write_queue.put(...)`,
  danach `await drain_future` und das Confirm. `sent` wahr heißt „in der `write_queue`“,
  nicht „auf dem Socket“; das entscheidet weiter `drain_future` (erfüllt: geschrieben).
  `sent` ungebunden (Ende in der Warteschlange der Sperre) oder falsch heißt „nicht
  eingereiht“ (KTD3).
- **Rückfall:** Fehlt `sent` in `co_varnames`, gilt der Publish als eingereiht; ist
  `drain_future` kein Future, als gesendet. Unbestimmt ist nie falsch, kann aber
  unnötige `PublishIndeterminate` erzeugen. Keine WARNING; die Tests unten werden rot.
- **Prüfen:** `aiormq/channel.py`, `basic_publish`: Namen und Reihenfolge der Locals,
  dass beide unter der Sperre gebunden werden, und dass `collect_orphaned_confirm`
  weiter unnötig ist (aiormq holt abgelehnte Futures selbst ab, #253; `FutureStore.create_future`).
- **Tests:** `uv run just unit -k "publish_phase or publish_connection_loss or aiormq_7_2_semantics"`
  (`test_basic_publish_has_the_locals_sent_and_drain_future`,
  `test_sent_is_set_after_the_frames_are_enqueued_and_bound_under_the_lock`) und
  `uv run just integration tests/integration/broker/test_befund_b24_publish_connection_loss.py`.

### `Channel._raise_on_channel_close` (privat, seit aiormq 7.2, Test-Doubles)

- **Wo:** `basic_publish` läuft in diesem Contextmanager. `FakeTransport` in
  `tests/support/aiormq_transport.py` ruft den echten Code aus aiormq mit sich selbst als
  Kanal auf und trägt dafür `_closing` und `_close_exception` wie aiormq.
- **Annahmen (in 7.2.4 Z. 879–901):** Hat der Kanal den laufenden Task selbst
  abgebrochen (`_closing` erledigt mit `_close_exception`, `cancelling() == 1`, der Grund
  als einziges Argument des `CancelledError`), nimmt aiormq den Abbruch zurück
  (`uncancel`) und wirft den Grund; ein fremder Abbruch bleibt `CancelledError`. Im
  Publish-Pfad von swarmq laufen Publishes nicht in aiormqs Consumer-Tasks; die
  Rücknahme trifft dort nur Code, der direkt im Consumer-Callback publiziert.
- **Rückfall:** keiner; benennt aiormq ihn um, scheitert `FakeTransport`, und die Unit-Tests
  des Publish-Pfads werden rot.
- **Tests:** `uv run just unit -k "aiormq_7_2_semantics or publish_connection_loss or publish_channel_closing"`.

### `TaskWrapper` macht den Abbruch des Aufrufers zum Kanalfehler (Verhalten, seit 0.14.4, F4-28)

- **Wo:** `_raise_if_cancel_was_swallowed` in `src/swarmq/worker/lifecycle.py`; dieselbe
  Regel „nur ein Anstieg von `cancelling()` zählt“ in `delivery.settle` und im
  Publish-Pfad (E-001, KTD9).
- **Verhalten (in 7.2.4 `aiormq/abc.py` Z. 26–61, `base.py` `reject_all`):** Jeder RPC
  läuft als eigener Task hinter einem `TaskWrapper`. Schließt der Kanal (`reject_all` →
  `TaskWrapper.throw(exc)`) und trifft danach der Abbruch den Aufrufer, wirft das
  Warten `exc` statt `CancelledError`; der Aufrufer trägt den Abbruch weiter in
  `cancelling()`. aiormq #239 (in 7.2) reicht den Grund nur an `Task.cancel(msg)` weiter,
  damit er im Traceback steht; am Ausgang für den Aufrufer ändert es nichts. Der Schutz
  aus F4-28 bleibt nötig.
- **Rückfall:** keiner nötig; liefert eine künftige Version `CancelledError`, ist die
  Prüfung überflüssig, aber nicht falsch.
- **Tests:** `uv run just unit tests/unit/broker/test_aiormq_7_2_semantics.py`
  (`test_the_task_wrapper_still_turns_the_callers_cancel_into_the_closing_error`, gegen
  aiormqs echten `FutureStore`) und
  `uv run just unit tests/unit/worker/test_befund_f4_28_abbruch_im_start.py`.

### `Connection.connection_tune.frame_max` (undokumentiert, seit 0.12, BZ-17)

- **Wo:** `negotiated_frame_max` in `src/swarmq/broker/rabbitmq/header_guard.py` (Z. 56)
  liest `connection.transport.connection.connection_tune.frame_max`; `guard_headers`
  begrenzt die Header jedes Publish auf die Hälfte davon.
- **Annahmen:** aiormq legt den `Connection.Tune`-Frame des Handshakes unter
  `connection_tune` ab (in 7.0.0 Z. 365, gesetzt Z. 587; in 7.2.4 Z. 367, gesetzt
  Z. 637) und überschreibt `frame_max` dort mit dem ausgehandelten Wert.
- **Rückfall:** Fehlt der Pfad (`AttributeError`) oder ist der Wert keine positive
  Zahl, gilt `DEFAULT_FRAME_MAX` (131072, RabbitMQ-Standard). Handelt ein Broker einen
  kleineren Wert aus, können zu große Header die Verbindung wieder schließen (BZ-17).
- **Tests:** `uv run just unit -k header_guard`.

### `Channel.consumers` (undokumentiert, seit 0.11, Consumer-Wächter)

- **Wo:** `ConsumerWatch.tick` in `src/swarmq/broker/rabbitmq/consumer_watch.py`
  (Z. 263) vergleicht die angemeldeten Tags mit `underlay.consumers` des aktuellen
  aiormq-Kanals; die Settle-Regel in `delivery.py` (Z. 641) prüft
  `tag in message.channel.consumers`, bevor ein Ack/Nack auf das Abmelden wartet.
- **Annahmen:** `consumers` ist ein `dict` Tag → Callback am konkreten aiormq-`Channel`
  (in 7.0.0 Z. 99), aiormq trägt bei `basic.consume_ok` ein und entfernt bei
  `basic.cancel` vom Broker (Z. 397). Am ABC von aiormq steht es nicht. In 7.2.4
  unverändert (`consumers` Z. 117, Eintrag Z. 791, `_on_cancel_frame` Z. 596–603). Neu
  in 7.2: `on_consumer_cancel_callbacks` (aio-pika hängt dort seine Wiederherstellung an,
  siehe „Consumer-Wiederherstellung“ unten), und ein `CancelOk` an den Broker erst, wenn
  er `accept_consumer_cancel_ok` meldet (ab RabbitMQ 4.3.7).
- **Rückfall:** keiner. Fehlt das Attribut, scheitert jeder Wächter-Takt
  (`consumer_watch_tick_failed`) und jedes Settle eines beobachteten Tags; die Tests
  unten werden rot.
- **Tests:** `uv run just unit -k "consumer_watch or settle"` und
  `uv run just integration tests/integration/broker/test_consumer_timeout.py`.

### `Connection.channel()` fängt `CancelledError` beim Öffnen nicht (7.0.0, seit 0.14, PF-20; berichtigt 0.14.1)

- **Verhalten:** `aiormq.Connection.channel` trägt den Kanal vor `channel.open()` in
  `channels[n]` ein und räumt den Eintrag nur in `except Exception` wieder aus. Die
  Folgen eines Abbruchs sind enger, als dieser Abschnitt bis 0.14.0 sagte
  (Gegenprüfung 0.14.1, Proben `probe_2a`, `probe_2c`, `probe_2d_*`):
  - **Im Ressourcenalarm entsteht kein Eintrag.** aio-pika wartet vor dem Öffnen
    `transport.ready()` ab (`Channel._open`), und aiormqs `Connection.ready()` wartet
    neben `connected` auch auf „unblocked“. Ein Abbruch im Alarm trifft dieses Warten,
    nicht das Öffnen (`probe_2c`: `channels` bleibt `{1}` nach dem Fristablauf von
    `_open_probe_channel` und von `_ensure_publish_channel`).
  - **Ein Abbruch mitten im Öffnen räumt sich selbst auf.** Läuft das RPC
    `Channel.Open` schon, fängt `Channel.rpc` den `CancelledError`, sendet
    `Channel.Close(504)` (WARNING „Closing channel … because RPC call … cancelled“), und
    der Eintrag verschwindet mit dem `Close-Ok` (`probe_2d_*`: nach höchstens 5 s
    wieder `{1}`).
  - **Nur ein Abbruch vor dem Anlaufen der inneren RPC-Task** (Eintrag schon gesetzt,
    `Channel.Open` noch nicht geschrieben) lässt einen Eintrag ohne Broker-Kanal
    stehen (`probe_2a`: `channels` behält `2`, beim Broker nur Kanal 1). Der Eintrag
    belegt bis zum Ende der Verbindung eine Kanalnummer; aio-pika kann ihn nicht
    schließen (`Channel.close` meldet nur „Channel not opened“). Einen halb offenen
    Kanal beim Broker gibt es dabei nicht.
- **Folge für swarmq:** `queue_exists_on_throwaway` in `broker/rabbitmq/robust_channels.py`
  (aufgerufen von `_consume_queue_exists` in `connection.py`) bricht das Öffnen des
  Wegwerf-Kanals nicht ab (`asyncio.shield`), sondern wartet es im `finally` ab und
  schließt den Kanal, beides zusammen höchstens `_RPC_TIMEOUT_S` (E-083). Nötig ist das
  nur für das schmale Fenster vor der RPC-Task; schaden kann es nicht.
  `_open_probe_channel` in `arg_conflict.py` bricht das Öffnen dagegen mit seiner Frist
  ab; im Alarm trifft der Abbruch das Warten auf „unblocked“ (siehe oben), sonst ist die
  Verbindung ohnehin tot. Der Verdacht „halb offener Probe-Kanal“ ist NICHT HALTBAR
  (Befundregister Runde 4, „NICHT HALTBAR“).
- **Prüfen bei einem aiormq-Upgrade:** ob `channel()` den Eintrag auch bei einem
  Abbruch ausräumt (`except BaseException` oder `finally`), ob `Channel.rpc` bei einem
  Abbruch weiter `Channel.Close` sendet und ob `Connection.ready()` weiter auf
  „unblocked“ wartet. Fällt eins davon weg, gilt die Einschätzung oben nicht mehr.
- **Stand 7.2.4 (nur Quelltext gelesen, nicht gemessen):** `Channel.open` fängt jetzt
  `BaseException` (`channel.py` Z. 396–409): Ein Abbruch, bevor `Channel.Open`
  geschrieben ist, schließt den Kanal lokal und gibt die Nummer sofort frei
  (`_close_locally`, WARNING „Closing channel … locally because RPC call … cancelled
  before Channel.Open was sent“); danach sendet `__close_after_failure` wie bisher
  `Channel.Close(504)`. Der Rest-Eintrag aus `probe_2a` entsteht damit nicht mehr; das
  Abwarten in `queue_exists_on_throwaway` bleibt unschädlich. `Connection.ready()`
  wartet weiter auf „unblocked“ (Z. 445–447).
- **Tests:** `uv run just unit -k consumer_watch_missing_queue`
  (`test_a_check_aborted_while_the_channel_opens_closes_the_channel_once_it_is_open`).

## aio-pika

### `RobustConnection.__channels` (privat, seit 0.14, F4-20)

- **Wo:** `release_channel` in `src/swarmq/broker/rabbitmq/robust_channels.py` liest
  `_RobustConnection__channels` (ein `weakref.WeakSet` der `RobustChannel`) und entfernt
  daraus den verworfenen Publish-Kanal (`_discard_publish_channel` in
  `broker/rabbitmq/connection.py`, vor dem im Alarm zurückgestellten Close) und die
  Kanäle, die `close()` auf einer injizierten Verbindung oder einem Pool schließt. Seit
  0.14.1 (F4-24) auch den Publish- oder Consume-Kanal, dessen Öffnen in
  `_ensure_channel_tracked` ohne Erfolg endet (Frist, Abbruch, Fehler; `try/finally`).
- **Annahmen:** `RobustConnection.channel()` trägt jeden neuen Kanal in dieses
  `WeakSet` ein, und zwar schon beim Anlegen, bevor er geöffnet ist (`channel()` ist
  synchron, erst `await` öffnet). `_on_connected` stellt nach einem Reconnect genau die
  Kanäle darin wieder her. Ein Kanal, der nicht darin steht, wird nicht wieder geöffnet.
  Endet das Öffnen eines `RobustChannel` mit einer Ausnahme oder einem Abbruch, schließt
  aio-pikas `Channel._open` (`except BaseException`, in 10.0.1 Z. 192–196) den
  aiormq-Kanal selbst, nimmt ihn aber nicht aus dem `WeakSet`. Ein Abbruch erst in
  `RobustChannel._on_open` (`Basic.Qos`, nach Open-Ok und Select-Ok) ließ den Kanal bis
  0.14.0 darin, und jeder Reconnect öffnete ihn als Waise (F4-24, Probe D). swarmq
  schließt darum nichts selbst, es meldet nur ab.
- **Rückfall:** Ist das Attribut kein `WeakSet`, loggt swarmq je Verbindung einmal die
  WARNING `robust_channel_release_unavailable` und meldet nichts ab (fail-open). Ein
  verworfener oder geschlossener robuster Kanal kann dann nach einem Reconnect wieder
  aufgehen und beim Broker offen bleiben wie vor 0.14; die Wegwerf-Kanäle (unten) sind
  davon nicht betroffen. Kein Absturz, aber „kein Leck“ aus E-078 (Nachtrag F4-20) gilt
  dann über einen Reconnect nicht mehr.
- **Prüfen:** `aio_pika/robust_connection.py`: `self.__channels` (in 10.0.1 Z. 67),
  `channel()` mit `self.__channels.add(channel)` (Z. 233–247), `_on_connected`
  (Z. 107–137). In 10.2.1: `__channels` Z. 62, `channel()` Z. 248–262,
  `_on_connected` Z. 106–147 (stellt jetzt in Runden wieder her und schiebt Kanäle mit
  fehlender Entität nach hinten).

### Fehler beim Restore: `_on_connected` stellt auch geschlossene `RobustChannel` wieder her (aio-pika 10.0.1)

- **Verhalten:** `RobustConnection._on_connected` ruft nach jedem Reconnect `restore()`
  für **jeden** Kanal im `WeakSet`, der noch nicht eingesammelt ist, auch für explizit
  geschlossene. `RobustChannel.close()` leert nur das Event `__restored`
  (`robust_channel.py`, in 10.0.1 Z. 123–129) und entfernt den Kanal nicht aus dem
  `WeakSet`; `restore()` (Z. 85–98) öffnet ihn trotzdem neu, danach hält aiormq ihn
  stark. Folgen bis 0.13 (F4-20): Die Kanalzahl beim Broker wuchs mit jedem Reconnect,
  ein wiederhergestellter Tiefen-Kanal auf eine gelöschte Queue ließ den Reconnect
  endlos an `NOT_FOUND` scheitern; wirft ein `restore()`, bricht `_on_connected` den
  ganzen Reconnect ab (Z. 131–137).
- **Folge für swarmq:** Wegwerf-Kanäle sind nicht robust (nächster Eintrag), robuste
  Kanäle meldet swarmq beim Verwerfen und Schließen selbst ab (`release_channel`, oben).
- **Prüfen bei einem aio-pika-Upgrade:** ob `close()` den Kanal jetzt aus dem `WeakSet`
  nimmt oder `restore()` einen geschlossenen Kanal überspringt. Dann wären Abmeldung und
  Wegwerf-Kanäle überflüssig, aber nicht falsch.
- **Stand 10.2.1:** `RobustChannel.close()` setzt `_close_called` (Z. 151–157), und
  `restore()` überspringt einen solchen Kanal (Z. 97–115). Ein per `close()`
  geschlossener Kanal bleibt also über einen Reconnect zu. Nicht abgedeckt sind Kanäle,
  deren `close()` noch nicht lief: der verworfene Publish-Kanal mit zurückgestelltem
  Close im Alarm (E-025) und der Kanal aus einem abgebrochenen Öffnen (F4-24). Die
  Abmeldung (`release_channel`) bleibt darum; die F4-20/F4-22/F4-24-Tests sind unter
  10.2.1 grün (U6).

### `aio_pika.Channel` auf einer `RobustConnection` (öffentliches API, seit 0.14, F4-20)

- **Wo:** `open_throwaway_channel` in `robust_channels.py` legt auf einer
  `RobustConnection` direkt einen `aio_pika.Channel(connection)` an statt
  `connection.channel()` (das einen `RobustChannel` liefert); `await` öffnet ihn.
  Genutzt für die Deklarationskette (`arg_conflict.py`), Queue-Tiefe
  (`_throwaway_channel_message_count`), Queue-Status (`_passive_declarations`) und die
  DLQ (`_dlq_queue`).
- **Annahmen:** Der Konstruktor nimmt die Verbindung als erstes Argument
  (`channel.py`, in 10.0.1 Z. 69, in 10.2.1 Z. 75), und ein so angelegter Kanal steht **nicht** im
  `WeakSet`: Nur `RobustConnection.channel()` trägt ein. Ein vom Broker geschlossener
  Kanal (`NOT_FOUND`, 406) bleibt damit zu, und ein Reconnect legt keine seiner Queues
  neu an.
- **Rückfall:** keiner nötig, das API ist öffentlich. Trüge eine spätere Version auch
  solche Kanäle ein, wäre F4-20 für die Wegwerf-Kanäle wieder offen; die Tests unten
  werden dann rot.
- **Tests (alle drei aio-pika-Einträge):**
  `uv run just unit -k "robust_channels or befund_f4_20"` (echtes aio-pika über einem
  Fake der aiormq-Verbindung: verworfener Publish-Kanal, Tiefe, Deklaration, DLQ; dazu
  fail-open und injizierte Verbindung) und
  `uv run just integration -k befund_f4_20` (Garbage Collection aus, Abriss über die
  Management-API: beim Broker nur die lebenden Kanäle; Reconnect nach der Tiefe einer
  gelöschten Queue binnen 10 s). Für das abgebrochene Öffnen (F4-24):
  `uv run just unit tests/unit/broker/test_befund_f4_24_aborted_channel_open.py` und
  `uv run just integration tests/integration/broker/test_befund_f4_24_aborted_channel_open.py`
  (`Basic.Qos` hängt, das Öffnen reißt die Frist, zwei Abrisse; Publish und Consume).

### Restore-Fenster und `reopen_callbacks` (öffentliches API, Ablauf, seit 0.14, F4-22)

- **Wo:** `_restored_in_time`, `_retire_replaced_channel` und `_close_after_restore` in
  `broker/rabbitmq/connection.py`, aufgerufen aus `_ensure_channel_tracked`.
- **Annahmen über den Ablauf in aio-pika 10.0.1 (in 10.2.1 gleich, F4-22-Tests grün):** `_on_connected` setzt zuerst
  `connected` und stellt danach die Kanäle aus einer Tupel-Kopie des `WeakSet` wieder
  her; ein schon abgemeldeter Kanal in dieser Kopie wird trotzdem geöffnet.
  `RobustChannel.ready()` wartet auf Verbindung und Restored-Event.
  `RobustChannel.reopen()` ruft `reopen_callbacks` nach dem Öffnen; `restore()` setzt das
  Restored-Event danach noch einmal, einen Schritt der Event-Loop später (die leere
  Callback-Sammlung gibt ein `STUB_AWAITABLE` zurück, das einmal abgibt). Ein Close vor
  diesem Schritt ließe `_on_close` den Kanal gleich wieder öffnen; darum schließt
  `_close_after_restore` erst nach `asyncio.sleep(0)`.
- **Rückfall:** keiner; ändert aio-pika den Ablauf, öffnet sich ein ersetzter
  Publish-Kanal schlimmstenfalls noch einmal, und jedes weitere Wiederöffnen schließt
  ihn erneut. Die Tests unten werden rot.
- **Tests:** `uv run just unit tests/unit/broker/test_befund_f4_22_channel_replaced_in_restore_window.py`
  (Wiederherstellung im Fenster, langsamer als die Frist, gescheitert, während des
  Ersatzes fertig, nach `broker.close()`, Consume) und
  `uv run just integration tests/integration/broker/test_befund_f4_22_channel_replaced_in_restore_window.py`
  (drei Ausfälle, genau ein Kanal beim Broker).

### `RobustChannel._queues` (privat, seit 0.14, PF-20)

- **Wo:** `release_queue` und `readmit_queue` in `robust_channels.py`, aufgerufen vom
  Consumer-Wächter (`_queue_present` in `broker/rabbitmq/consumer_watch.py`). Fehlt eine
  konsumierte Queue beim Broker (passives Declare auf einem Wegwerf-Kanal,
  `_consume_queue_exists` in `connection.py`), nimmt `release_queue` die `RobustQueue`
  aus `_queues` ihres `RobustChannel` (Name → Menge der `RobustQueue`); gibt es sie
  wieder, legt `readmit_queue` sie zurück. Duck-typed: jeder Kanal mit einem `dict`
  unter `_queues` gilt.
- **Annahmen:** `RobustChannel.declare_queue(..., robust=True)` (auch `get_queue`, das
  passiv deklariert) trägt die Queue in `_queues[name]` ein (in 10.0.1 Z. 277);
  `_on_open` stellt nach einem Reconnect genau die Queues darin wieder her
  (`queue.restore()`, Z. 140), und `RobustQueue.restore()` deklariert passiv neu und
  meldet die Consumer aus `_consumers` wieder an (`robust_queue.py`, Z. 57–74). Ein
  passives Declare mit NOT_FOUND lässt `_on_open` scheitern; daran scheitert der ganze
  Reconnect (siehe `_on_connected` oben). `RobustQueue.cancel` entfernt den Tag aus
  `_consumers` (öffentliches API, Z. 157); der Wächter meldet den Tag vorher ab.
  Einen öffentlichen Ersatz gibt es nicht: `RobustChannel.queue_delete` entfernt den Eintrag
  auch, löschte aber eine inzwischen neu angelegte Queue.
- **Rückfall:** Ist `_queues` an einem `RobustChannel` kein `dict`, loggt swarmq je
  Kanal einmal die WARNING `robust_queue_release_unavailable` und lässt die Queue in
  der Liste (fail-open). Der Wächter meldet die Queue trotzdem als fehlend und sendet
  kein `basic.consume`; die übrigen Queues laufen weiter, aber ein Reconnect scheitert
  wie vor 0.14 alle 5 s an `NOT_FOUND`, bis der Worker neu startet (E-083).
- **Prüfen:** `aio_pika/robust_channel.py`: `self._queues = defaultdict(set)` (Z. 67),
  `_on_open` (Z. 135–155), `declare_queue` (Z. 242–278); `aio_pika/robust_queue.py`:
  `restore` und `cancel`. In 10.2.1: `_queues` Z. 78, `_on_open` Z. 164–188, Eintrag in
  `declare_queue` Z. 406; `RobustQueue.restore` Z. 69, `cancel` entfernt den Tag Z. 234.
  Neu in 10.2: Auch `_restore_missing_consumers` liest `_queues` (Probe-Kanal und
  Anmeldung nach `basic.cancel`); für swarmqs Kanäle ist das abgeschaltet (E-089, unten).
- **Tests:** `uv run just unit -k "robust_channels or consumer_watch_missing_queue"`
  (echter `RobustChannel` und `RobustQueue`: Abmelden, Zurücklegen, fail-open) und
  `uv run just integration tests/integration/broker/test_consumed_queue_deleted.py`
  (Queue gelöscht, Reconnect mit `separate` und `shared` ohne `NOT_FOUND`, Queue neu
  angelegt).

### `RobustQueue._consumers` und `RobustQueue.cancel` (privat bzw. Verhalten, seit 0.11)

- **Wo:** Der Consumer-Wächter (`_reregister` in `consumer_watch.py`, Modul-Docstring
  Z. 4–13 und Z. 301–304) meldet einen vom Broker beendeten Tag über das öffentliche
  `RobustQueue.cancel` ab, bevor er neu anmeldet. Gelesen wird `_consumers` nur in
  `tests/integration/broker/test_consumer_timeout.py` (Z. 212, 236).
- **Annahmen:** `RobustQueue.consume` trägt den Tag in `_consumers` ein
  (`robust_queue.py`, in 10.0.1 Z. 141), `cancel` entfernt ihn (Z. 157), und
  `restore()` meldet nach einem Reconnect genau die Tags darin wieder an (Z. 67).
  aio-pika merkt ein `basic.cancel` des Brokers nicht; der tote Tag bleibt sonst stehen.
  In 10.2.1: Eintrag Z. 193, Entfernen Z. 234, `restore` Z. 69. Seit 10.2 merkt aio-pika
  ein `basic.cancel` doch (`_on_consumer_cancel`) und meldet den Tag selbst neu an; swarmq
  schaltet das ab (nächster Eintrag), der Wächter bleibt allein zuständig.
- **Rückfall:** keiner. Entfernte `cancel` den Tag nicht mehr, legte ein Restore den
  toten Consumer neben dem neu angemeldeten wieder an; der Integrationstest wird rot.
- **Tests:** `uv run just unit -k consumer_watch` und
  `uv run just integration tests/integration/broker/test_consumer_timeout.py`.

### Kanalöffnen ohne Beobachter-Leck: `Channel._open` und `UnderlayChannel` (privat, seit Plan 2026-10-29-1945, U6, KTD6)

- **Verhalten in 10.2.1:** `UnderlayChannel.create` (`abc.py` Z. 587–601) hängt den
  `close_callback` an `connection.closing` und `channel.closing`; seit aiormq 7.2 sind
  das Beobachter (oben). `UnderlayChannel.close` (Z. 605–617) liest beide noch einmal, um
  den Callback zu entfernen, und erzeugt dabei neue, leere Beobachter. Je geschlossenem
  Kanal bleiben zwei Beobachter an `_closing` der Verbindung, der erste hält den ganzen
  Kanal. Ein vom Broker geschlossener und von aio-pika wiederhergestellter
  `RobustChannel` läuft nie durch `close` und lässt seinen Beobachter ebenso zurück.
- **Wo:** `src/swarmq/broker/rabbitmq/underlay.py`. `_ReleasingUnderlay` hält seinen
  Beobachter selbst und bricht ihn ab, sobald der aiormq-Kanal zu ist; `_open` ist eine
  Kopie von `aio_pika.Channel._open` (10.2.1, `channel.py` Z. 183–207), nur mit
  `_ReleasingUnderlay`. `ReleasingChannel` trägt die Wegwerf-Kanäle
  (`open_throwaway_channel`, auf jeder aio-pika-Verbindung), `ReleasingRobustChannel`
  Publish- und Consume-Kanal auf swarmqs eigenen Verbindungen (`CHANNEL_CLASS` der
  `RobustConnection`-Unterklasse in `connection.py`), auch beim Wiederherstellen. Der
  Publish-Kanal auf einer injizierten Verbindung öffnet über deren Kanalklasse und
  leckt wie bei aio-pika.
- **Fingerabdruck:** `_fits_aio_pika` vergleicht `aio_pika.Channel._open.__code__.co_names`
  mit `_OPEN_NAMES` und die Felder des Dataclass `UnderlayChannel` mit
  `_UNDERLAY_FIELDS` (`channel`, `close_callback`). Ohne aiormq-Verbindung
  (Test-Doubles) öffnet aio-pikas eigenes `_open`.
- **Rückfall:** Weicht der Fingerabdruck ab, öffnet der Kanal über aio-pikas `_open`,
  einmal je Verbindung WARNING `channel_observer_release_unavailable`. Kein Absturz, aber
  der Speicher wächst mit jedem Wegwerf- oder verworfenen Kanal (E-078).
- **Prüfen bei einem aio-pika-Upgrade:** ob `UnderlayChannel.close` seine Beobachter
  inzwischen selbst abbricht (dann wäre `underlay.py` überflüssig, aber nicht falsch);
  sonst `_open` gegen die neue Vorlage abgleichen und erst danach `_OPEN_NAMES` und
  `_UNDERLAY_FIELDS` nachführen, nicht umgekehrt.
- **Tests:** `uv run just unit tests/unit/broker/test_underlay.py` (Fingerabdruck,
  fail-open, Abbruch im Öffnen) und
  `uv run just integration tests/integration/broker/test_channel_callbacks_flat.py`
  (10 000 Wegwerf-Kanäle, 1 000 Verwürfe des Publish-Kanals, 100 vom Broker geschlossene
  und wiederhergestellte Kanäle: Callbacks an `_closing` flach).

### Consumer-Wiederherstellung nach `basic.cancel` (privat, neu in aio-pika 10.2, abgeschaltet seit U7, E-089)

- **Verhalten in 10.2.1:** `RobustChannel._on_open` (Z. 164–188) hängt
  `_on_consumer_cancel` an `on_consumer_cancel_callbacks` des aiormq-Kanals. Ein
  `basic.cancel` des Brokers für einen bekannten Tag weckt über `_request_recovery` die
  Verbindungsfabrik (`RobustConnection.__connection_factory`, Z. 149–207); bei lebender
  Verbindung ruft sie `_restore_consumers` jedes `RobustChannel` (Z. 199–), das je Queue
  einen Probe-Kanal öffnet (`timeout=5`), passiv deklariert und den Tag mit `timeout=5`
  auf dem Consume-Kanal neu anmeldet. Gemessen (U3, `test_consumer_cancel_recovery.py`):
  530 „reuse consumer tag“ auf RabbitMQ 4.3.6, Probe-Kanäle je `reconnect_interval`
  gegen eine gelöschte Queue, Fristen auf dem Consume-Kanal (gegen E-053),
  `recovery.cancel()` bricht ein `probe.close()` ab (504, wie F4-30).
- **Wo:** `ReleasingRobustChannel` in `underlay.py` überschreibt `_on_consumer_cancel`
  (tut nichts) und `_restore_consumers` (liefert `False`). Die Wiederherstellung nach
  einem Kanal-Close (`_on_close`) und nach einem Reconnect (`_on_connected`) bleibt wie
  in 10.0.
- **Fingerabdruck:** `_consumer_recovery_entries_fit` prüft, ob `_on_consumer_cancel` in
  `aio_pika.RobustChannel._on_open.__code__.co_names` steht und `_restore_consumers` in
  den `co_names` von `RobustConnection._RobustConnection__connection_factory`.
- **Rückfall:** Passt der Fingerabdruck nicht, meldet `_on_open` einmal je Verbindung
  WARNING `consumer_recovery_disable_unavailable`; die Überschreibungen bleiben, aber
  aio-pika kann über neue Einstiege selbst wieder anmelden (auf 4.3 mit dem alten Tag,
  530 schließt die Verbindung).
- **Prüfen bei einem aio-pika-Upgrade:** die beiden Namen oben, ob die Fabrik bei
  lebender Verbindung noch andere Wege zur Anmeldung hat, und ob RabbitMQ ab 4.3.7
  (`accept_consumer_cancel_ok`) die Zombie-Prämisse aufhebt (Überdenken in E-089).
- **Tests:** `uv run just unit tests/unit/broker/test_underlay.py` (E-089-Tests) und
  `uv run just integration tests/integration/broker/test_consumer_cancel_recovery.py`
  (RabbitMQ 4.3.6 und 4.2.9: ein Consumer, kein 530, keine Probe-Kanäle),
  `test_consumer_timeout.py`, `test_consumed_queue_deleted.py`.

## pytest

### `_pytest.timing.MockTiming` (privat, seit 0.14, U7)

- **Wo:** `tests/unit/test_slow_test_report.py`, Fixture `_virtual_clock`: Die inneren
  `pytester`-Läufe messen mit `timing.MockTiming().patch(mp)`, damit der Slow-Report
  unter CI-Last exakte Listen liefert.
- **Annahmen:** `_pytest.timing.MockTiming` existiert, hat `patch(monkeypatch)`, und
  `timing.sleep` im inneren Test rückt die Uhr vor; pytest misst `report.duration` über
  `_pytest.timing`.
- **Rückfall:** keiner. Fehlt die Klasse, scheitert das Testmodul beim Lauf; das ist
  ein Testwerkzeug, kein Produktverhalten.
- **Tests:** `uv run --extra test pytest tests/unit/test_slow_test_report.py`.

### Modulname `_pytest.logging` (privat, Testwerkzeug)

- **Wo:** `tests/unit/test_log_capture.py` (Z. 138) erkennt die Capture-Handler von
  pytest am Modulnamen `_pytest.logging`.
- **Rückfall:** keiner; benennt pytest das Modul um, wird der Test rot.
- **Tests:** `uv run --extra test pytest tests/unit/test_log_capture.py`.

## asyncio (stdlib)

### `asyncio.gather` verliert die Markierung `PublishCancelledUnsent` (bekannte Lücke, U4)

- **Verhalten:** `asyncio.gather(..., return_exceptions=True)` ersetzt den Abbruch eines
  Kinds durch einen neuen `CancelledError('')`; eine Unterklasse wie
  `PublishCancelledUnsent` kommt nicht beim Aufrufer an (geprüft mit Python 3.14.4).
- **Folge für swarmq:** `schedule_many` hält die Markierung deshalb je Index in einer
  Hülle um den Publish fest (`_publish_marking_unsent` in `src/swarmq/client/schedule.py`).
  Wer an anderer Stelle Publishes über `gather` bündelt und auf die Markierung baut,
  braucht dasselbe Muster; sonst gilt der Abbruch als unbestimmt, und der
  `unique_for`-Claim bleibt (konservativ, E-041).
- **Prüfen bei einem Python-Upgrade:** ob `gather` die Instanz des Kinds weiterreicht.
  Dann wäre die Hülle überflüssig, aber nicht falsch.
- **Tests:** `uv run just unit -k cancel_keeps_unique`
  (`test_schedule_many_releases_only_the_claim_of_a_publish_marked_unsent`,
  `test_cancel_of_schedule_many_with_publishes_marked_unsent_releases_their_claims`).

## logging (stdlib)

### `logging._lock` und `logging._handlerList` (privat, seit 0.9)

- **Wo:** nur `src/swarmq/logging_stdlib.py` (`module_lock`, `handler_refs`); genutzt
  für die Lock-Reihenfolge im Fork-Hook (`logging_fork.py`), zum Vergessen aufgegebener
  Handler (`logging_drain.py`) und um Handler vor `dictConfig` zu verbergen. Tests setzen
  `_handlerList` per `monkeypatch` und halten `logging._lock` direkt
  (`test_logging_fork.py`, `test_logging_drain.py`, `test_logging_configure.py` u. a.).
- **Annahmen:** `_lock` ist das `RLock`, das `dictConfig`, die Handler-Registrierung
  und `logging.shutdown` serialisiert; `_handlerList` ist die Liste der
  Schwachreferenzen, die `shutdown` schließt; die stdlib setzt beide im Fork-Kind selbst
  zurück (`_after_at_fork_child_reinit_locks`).
- **Rückfall:** keiner; ein `AttributeError` im Logging-Setup. Anzupassen ist nur
  `logging_stdlib.py`.
- **Tests:** `uv run just unit -k "logging_fork or logging_drain or logging_configure"`.

### `QueueListener._thread` und `QueueListener._monitor` (privat, seit 0.8 bzw. 0.9)

- **Wo:** `_thread_of` und `_forget_thread` in `src/swarmq/logging_file.py` (Z. 464,
  471), `_report_from_listener` (Z. 661 liest `_monitor`, Z. 683 ersetzt es durch eine
  Hülle, die Fehler des Handlers ratenbegrenzt meldet) und `logging_queue.py` (Z. 702,
  Drain eines laufenden Listeners). Tests lesen `listener._thread`.
- **Annahmen:** `QueueListener.start` legt den Thread unter `_thread` ab und startet ihn
  mit `target=self._monitor`; `stop` setzt `_thread` auf `None`; `start` wirft, solange
  `_thread` gesetzt ist.
- **Rückfall:** `_thread_of` liefert bei fehlendem Attribut `None` (der Listener gilt als
  nicht gestartet); `_monitor` hat keinen Rückfall, ohne ihn scheitert das Einrichten
  eines Datei-Listeners.
- **Tests:** `uv run just unit -k "logging_queue or logging_file or logging_configure"`.

## multiprocessing (stdlib)

### `forkserver._forkserver._forkserver_alive_fd` und `._forkserver_pid` (privat, seit 0.5)

- **Wo:** `release_forkserver_alive_fd` in `src/swarmq/worker/compute/child.py`
  (Z. 79–85) schließt im Rechenkind das geerbte Schreib-Ende `_forkserver_alive_fd`,
  sonst hielte ein lebendes Kind den Forkserver nach dem Tod des Loop-Prozesses am Leben
  und das Parent-Death-Signal feuerte nie. `forkserver_pid` in
  `worker/compute/forkserver.py` (Z. 157) liest `_forkserver_pid` (KTD14).
- **Annahmen:** Das Modul-Singleton `multiprocessing.forkserver._forkserver` trägt beide
  Attribute; jedes Kind des Forkservers erbt das Alive-fd.
- **Rückfall:** Fehlt `_forkserver_alive_fd`, schließt das Kind nichts (kein Absturz,
  aber verwaiste Rechenkinder nach einem Absturz des Loop-Prozesses möglich). Fehlt
  `_forkserver_pid`, liefert `forkserver_pid` `None`, und die Prüfungen auf
  Forkserver-Threads werden zu Warnungen.
- **Tests:** `uv run just unit -k "compute and (child or forkserver)"` und
  `uv run just integration -k "compute_child or app_dir_spawn"`.

## Tests gegen aio-pika-Interna (F4-20, F4-22)

Die Nachweise zu F4-20 und F4-22 treiben ein echtes `RobustConnection` über einem Fake
der aiormq-Verbindung und spielen den Reconnect von Hand nach:
`connection.transport = _FakeTransport(...)` bzw. `= None`, `connection.connected.set()`
und `.clear()` und ein direkter Aufruf von `await connection._on_connected()`
(`tests/unit/broker/test_befund_f4_20_restore_closed_channels.py` Z. 134–150,
`tests/unit/broker/test_befund_f4_22_channel_replaced_in_restore_window.py`
`_connected_broker`, `_drop`, `_reconnect` und die Tests mit `_HeldAiormq`).

- **Annahmen:** `RobustConnection._on_connected` (in 10.0.1 Z. 107, in 10.2.1 Z. 106) setzt über
  `Connection._on_connected` `connected` (`connection.py` Z. 137) und stellt danach die
  Kanäle wieder her; der Abriss besteht aus `transport = None` und `connected.clear()`.
  Die Tests ersetzen damit `connect()` und den Reconnect-Takt.
- **Rückfall:** keiner; ändert aio-pika die Reihenfolge oder die Namen, werden die Tests
  rot oder prüfen das Fenster nicht mehr. Dann zuerst gegen die Integrationstests
  (`uv run just integration -k "befund_f4_20 or befund_f4_22"`) prüfen, ob das
  Verhalten stimmt, und die Fakes nachziehen.
- Ebenso setzen viele Unit-Tests das aiormq-Attribut `_Connection__connection_unblocked`
  an Fakes (`test_publish_blocked.py`, `test_rabbitmq.py`, F4-20/F4-21), die
  Integrationstests lesen `channels` der aiormq-Verbindung (`test_declare_chain.py`,
  `test_pf19_close_after_unblocked.py`) und `test_robust_channels.py`
  `_RobustConnection__channels`; sie folgen den Einträgen oben.

## Ablauf bei einem Upgrade

1. Für jede angehobene Bibliothek die Einträge oben am neuen Quelltext prüfen.
2. Die genannten Tests laufen lassen, dann `uv run just all` und für valkey-py,
   aiormq und aio-pika zusätzlich `SWARMQ_REQUIRE_CHAOS=1 uv run just chaos`. Bei einem
   Python-Upgrade auch die Einträge zu `logging` und `multiprocessing`.
3. Bei einer Abweichung erst die betroffene Entscheidung lesen (E-001, E-019, E-025,
   E-041, E-042, E-053, E-075, E-078, E-083, E-089), dann anpassen. Einen Test mit `entscheidung`-Marker nicht lockern.
4. Diese Datei mit den neuen Versionen und Zeilen nachführen.
5. Für aiormq und aio-pika in Integration, Chaos und Loadsuite auf die Rückfall-WARNINGs
   achten (`publish_blocked_state_unreadable`, `robust_channel_release_unavailable`,
   `robust_queue_release_unavailable`, `channel_observer_release_unavailable`,
   `consumer_recovery_disable_unavailable`): Sie ersetzen rote Tests, wenn eine Stelle
   still auf den Rückfall fällt. Danach die Leck-Ausnahmen in
   `tests/support/known_leaks.py` probeweise entfernen (TS-01 bis TS-03).
