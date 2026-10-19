---
title: Upgrade-Checkliste – private APIs von valkey-py, aiormq, pytest und asyncio, an denen swarmq hängt
date: 2026-10-19
category: docs/solutions/tooling-decisions
module: backend/valkey/connection.py / broker/rabbitmq/blocked.py / client/schedule.py / tests/unit/test_slow_test_report.py
problem_type: tooling_decision
component: dependencies
severity: medium
applies_when:
  - valkey-py, aiormq, aio-pika, pytest oder Python werden angehoben (auch Patch-Versionen)
  - ein Plan will eine weitere private API einer Bibliothek nutzen
  - nach einem Abhängigkeits-Upgrade scheitern Tests zu Verbindungsaufbau, Blocked-Zustand, Abbruch-Markierung oder Slow-Report
tags: [abhaengigkeiten, upgrade, valkey-py, aiormq, pytest, asyncio, private-api, checkliste]
entscheidungen: [E-025, E-041, E-075, E-078]
---

# Upgrade-Checkliste: private Bibliotheks-APIs

swarmq hängt an einigen privaten oder undokumentierten Stellen seiner
Abhängigkeiten. Ein Upgrade kann sie still ändern. Vor jedem Anheben der
betroffenen Bibliothek (auch eines Patch-Releases) die Einträge unten am neuen
Quelltext prüfen und die genannten Tests laufen lassen. Neue Kopplungen dieser
Art kommen hier dazu, mit Fundstelle, Prüfung und Rückfall.

Stand: swarmq 0.14 (Branch `feat/0-14`), valkey-py 6.1.1, aiormq 7.0.0,
aio-pika 10.0.1, pytest 9.1.1, Python 3.14.

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

## aiormq

### `Connection.__connection_unblocked` (privat, seit 0.12, BZ-16; seit 0.14 auch U6)

- **Wo:** `unblocked_event` und `blocked_close_waiters` in
  `src/swarmq/broker/rabbitmq/blocked.py` lesen das Attribut
  `_Connection__connection_unblocked` (ein `asyncio.Event`, gesetzt heißt „nicht
  blockiert“).
- **Annahmen:** aiormq leert das Event bei `connection.blocked`
  (`__handle_connection_blocked`) und setzt es bei `connection.unblocked` und beim Start
  des Lesers einer neuen Verbindung (`__reader`).
- **Rückfall:** fail-open (E-025). Ist das Attribut nicht lesbar, loggt swarmq einmal
  `publish_blocked_state_unreadable` und publiziert ungeprüft wie vor 0.12; das Close
  eines verworfenen Kanals läuft sofort wie vor 0.14.

### `Connection.closing` (Future, seit 0.14, U6)

- **Wo:** `blocked_close_waiters` liest beim Verwurf des Publish-Kanals
  `connection.transport.connection.closing`. `wait_until_unblocked` wartet auf das
  Unblocked-Event **oder** dieses Future, höchstens die Publish-Frist (PF-19 a).
- **Annahme:** `closing` ist ein `asyncio.Future`, das erledigt ist, sobald die
  aiormq-Verbindung endet.
- **Rückfall:** Ist `closing` kein `asyncio.Future`, liefert `blocked_close_waiters`
  `None`, und der Kanal wird sofort geschlossen wie vor 0.14 (fail-open). Im Alarm
  erscheinen dann wieder `publish_channel_close_incomplete` und die aiormq-ERROR „Got
  frame for closed channel“.
- **Prüfen:** `aiormq/connection.py`, Attribut `__connection_unblocked` (in 7.0.0
  Z. 433, gesetzt und geleert Z. 692–705), und `aiormq/base.py`, `closing` (Z. 96,
  `_create_closing_future`).
- **Tests:** `uv run just unit -k "publish_blocked or rabbitmq"`
  (`TestChannelCloseAfterUnblocked`, darunter der Fall „`closing` unlesbar“) und
  `uv run just integration -k "bz16 or blocked_cancel or pf19"` gegen den echten Broker.

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

## Ablauf bei einem Upgrade

1. Für jede angehobene Bibliothek die Einträge oben am neuen Quelltext prüfen.
2. Die genannten Tests laufen lassen, dann `uv run just all` und für valkey-py und
   aiormq zusätzlich `SWARMQ_REQUIRE_CHAOS=1 uv run just chaos`.
3. Bei einer Abweichung erst die betroffene Entscheidung lesen (E-025, E-041, E-075,
   E-078), dann anpassen. Einen Test mit `entscheidung`-Marker nicht lockern.
4. Diese Datei mit den neuen Versionen und Zeilen nachführen.
