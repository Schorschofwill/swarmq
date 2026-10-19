---
title: "Spike PF-19 (a): Close des verworfenen Publish-Kanals erst nach connection.unblocked"
date: 2026-10-18
category: spikes
module: broker/rabbitmq/connection
problem_type: spike
component: broker
status: implemented
applies_when:
  - ein Plan will das Channel.Close im Ressourcenalarm bis connection.unblocked zurückstellen (PF-19 a)
  - Änderungen an _discard_publish_channel, _close_channel_detached oder _CHANNEL_CLOSE_TIMEOUT_S
  - eine Fehlersuche meldet „Got frame for closed channel“ oder publish_channel_close_incomplete im Alarm
tags: [broker, blocked, alarm, kanal, close, aiormq, pf-19, r4i-04b, e-025, spike]
entscheidungen: [E-025, E-041, E-042, E-078]
---

# Spike PF-19 (a): Kanal-Close nach `unblocked`

Stand: Worktree `fix/fehlersuche-runde-4-p2` (HEAD b022439), Test-RabbitMQ 4.3.6
(`swarmq-test-rabbit`, Port 5699), aio-pika/aiormq aus `uv.lock`. Kein Produktionscode geändert.

## Frage

Im Alarm läuft das `Channel.Close` eines verworfenen Publish-Kanals in die 5-s-Frist
(`_CHANNEL_CLOSE_TIMEOUT_S`): WARNING `publish_channel_close_incomplete` mit Traceback, bei der
Aufhebung aiormq-ERROR „Got frame for closed channel N: <Channel.CloseOk>“. Lässt sich das Close
sauber bis `connection.unblocked` zurückstellen? Werden Kanalnummern dabei doppelt vergeben
(R4I-04b), bleibt ein Kanal beim Broker offen?

## Erfolgskriterium

Aus PF-19 (Register, Spalte Empfehlung): Integrationstest auf einem Speicheralarm, der auch die
Kanalnummer-Vergabe prüft. Erfolg: kein aiormq-ERROR, keine Close-Frist-Warnung mit Traceback, Kanal
nach der Aufhebung beim Broker geschlossen (kein Leck), keine doppelte Kanalnummer, Ausgänge der
Publishes unverändert (E-041/E-042).

## Aufbau

- `RabbitMQBroker` mit Verbindungsnamen `spike-pf19`, Classic-Queue, Publish-Frist 3 s.
- Ablauf: Publish `m0` (Kanal 1 offen), Alarm an (`set_vm_memory_high_watermark 0.0000001`), Publish
  `m1` (löst die Blockade aus, Frist reißt, Kanal wird verworfen), Publish `m2` startet im Alarm
  (Frist 60 s, wartet in `_wait_while_blocked`), 9 s warten (die heutige 5-s-Close-Frist reißt),
  Wasserlinie exakt zurück (`absolute 3221225472`), `m2` abwarten, nachlaufen lassen.
- Gemessen: Ausgänge, Kanalnummern in der aiormq-Tabelle und beim Broker (Management-API, Kanäle der
  Verbindung), Log-Zeilen ab WARNING mit Zeitstempel.
- Varianten: `heute`; `nach-unblocked` (Prototyp im Probe-Prozess: `_close_channel_detached` wartet
  bei blockierter Verbindung auf das Unblocked-Event von `unblocked_event(...)`, dann das heutige
  Close mit 5-s-Frist).
- Unter der Testsperre, Wasserlinie per `trap` in jedem Fall zurück. Probe-Code (flüchtig):
  Scratchpad `fix4/spikes/pf19_probe.py`.

## Messwerte

Zeitachse (s seit Start; Alarm aktiv ab ≈ 7,4 s, Aufhebung bei ≈ 19,9 s):

| Ereignis | `heute` | `nach-unblocked` |
|---|---|---|
| `m1` | `PublishIndeterminate` nach 3 s (10,4) | `PublishIndeterminate` nach 3 s (10,5) |
| Close des Kanals 1 | gesendet 10,4; aiormq-WARNING „Closing channel … RPC call cancelled“ und `publish_channel_close_incomplete` **mit Traceback** bei 15,4 | zurückgestellt 10,5; gesendet und **geschlossen** bei 20,04 (direkt nach `unblocked`) |
| aiormq bei der Aufhebung | **ERROR** „Got frame for closed channel 1: <Channel.CloseOk>“ | WARNING „Delivery tag 2 confirmed <Basic.Ack> was ignored“ |
| `m2` | bestätigt auf Kanal 2 (19,9) | bestätigt auf Kanal 2 (20,0) |
| aiormq-Kanäle vor der Aufhebung | [1] | [1] (Close offen: 1) |
| Broker-Kanäle der Verbindung am Ende | [2] | [2] |
| offene Close-Tasks am Ende | 0 | 0 |

Befunde:

1. Das zurückgestellte Close gelingt in Millisekunden nach `unblocked`; keine Close-Frist, kein
   Traceback, kein aiormq-ERROR. Kein Leck: Der Broker führt am Ende nur Kanal 2.
2. Kanalnummern: In beiden Varianten bekommt der neue Publish-Kanal die 2; eine Doppelvergabe der 1
   (R4I-04b) trat nicht auf. Mit dem zurückgestellten Close steht Kanal 1 bis zum Close in der
   aiormq-Tabelle, die Vergabe `max(channels) + 1` kann die 1 dann gar nicht wählen.
3. Neu sichtbar: Der Broker bestätigt `m1` nach der Aufhebung (Ack auf Kanal 1, von aiormq als
   „ignored“ verworfen). Der auslösende Publish ist also angekommen, obwohl er als
   `PublishIndeterminate` endete; das ist die Grenze D-1 aus E-025 und kein falscher Ausgang
   (E-041/E-042 unverändert). Die aiormq-ERROR-Zeile wird durch eine WARNING ersetzt.
4. Grenzen des Prototyps (nach Code, nicht gemessen): Reißt die Verbindung während des Alarms,
   bleibt das Event des alten aiormq-Objekts ungesetzt und der Close-Task wartet bis zum
   `close()` des Brokers (dort nach 5 s abgebrochen, `_drain_pending_channel_closes`). Eine Umsetzung
   braucht daher eine Schranke: warten auf `unblocked` **oder** Schließen der Verbindung, höchstens
   die Publish-Frist. Ein Shutdown im Alarm dauert wie heute höchstens 5 s länger.

## Entscheidungen

- **E-025:** Kein Widerspruch. Nachtrag (3) nennt das Zurückstellen als Prüfpunkt; die Regel
  (Publish wartet bis zur Frist, Verbindung und Kanal bleiben) ist unberührt, nur der Zeitpunkt des
  Close nach einem Verwurf ändert sich. Bei Umsetzung: Nachtrag (3) ergänzen, Event-Katalog um die
  aiormq-WARNING „Delivery tag … was ignored“ statt der ERROR-Zeile.
- **E-041/E-042:** unverändert (`m1` bleibt `PublishIndeterminate`).

## Empfehlung für Georg

**Umsetzen (klein, P4), nach Georgs Ok:** `_close_channel_detached` wartet bei blockierter Verbindung
auf `unblocked` oder das Schließen der Verbindung, höchstens die Publish-Frist, und schließt dann mit
der bisherigen 5-s-Frist. Festschreibender Integrationstest unter `exclusive` mit genau diesem
Ablauf: kein aiormq-ERROR, Kanal beim Broker geschlossen, neue Kanalnummer verschieden. Die schon
beschlossene Doku (b) bleibt richtig; der Kommentar an `_CHANNEL_CLOSE_TIMEOUT_S` ist mit beiden
Wegen zu berichtigen. Keine „Entscheidung überdenken?“-Markierung nötig.

## Umsetzung (0.14, U6)

Plan `docs/plans/2026-10-19-1500-feat-swarmq-0-14-plan.md` (R6, KTD5, U6), umgesetzt wie empfohlen,
mit der Schranke aus Befund 4:

- `blocked_close_waiters(connection)` in `broker/rabbitmq/blocked.py` liest beim Verwurf das
  Unblocked-Event **und** das `closing`-Future derselben aiormq-Verbindung. Nicht blockiert, Zustand
  oder `closing` unlesbar: `None`, sofortiges Close wie bisher (fail-open, E-025).
- `wait_until_unblocked(...)` wartet auf `unblocked` oder `closing`, höchstens die Publish-Frist
  (`_effective_publish_timeout()`, im ersten Schritt des Close-Tasks gelesen). Der Hilfs-Task für
  `Event.wait()` wird danach abgebrochen; auch beim Abbruch durch `broker.close()` bleibt kein Task
  zurück.
- `_close_channel_detached` schließt danach wie bisher mit `_CHANNEL_CLOSE_TIMEOUT_S` (5 s).
  `_drain_pending_channel_closes` ist unverändert.

Festgeschrieben (Nachtrag E-025, Präzisierung E-078):

- Unit: `tests/unit/broker/test_rabbitmq.py::TestChannelCloseAfterUnblocked` (fünf Tests: Close erst
  nach `unblocked`; Alarm länger als die Frist; Verbindungsende; `closing` unlesbar;
  `broker.close()` im Warten) und zwei neue Tests in `test_publish_deadline_channel_discard.py`
  (übrige Confirms enden bestätigt bzw. mit ihrer eigenen Frist).
- Integration (`exclusive`, `memory_alarm`): `tests/integration/broker/test_pf19_close_after_unblocked.py`,
  Ablauf wie oben mit Publish-Frist 15 s und Aufhebung nach 8 s. Ohne die Änderung rot an der
  aiormq-ERROR „Got frame for closed channel 1: <Channel.CloseOk>“; mit ihr keine ERROR, beim Broker
  nur Kanal 2, `m2` bestätigt. Zweiter Test: Verbindungsende im Alarm (Management-API wie
  `rabbitmqctl close_connection`), das wartende Close endet sofort.
- Chaos (4.3.6-Stack): `tests/chaos/test_publish_deadline.py::TestDeferredCloseUnderAlarm`, Alarm plus
  Toxiproxy `down` während des Wartens. Rot, wenn das Warten `closing` nicht beachtet.

Nebenbeobachtungen bei der Umsetzung (nicht Teil von PF-19 a, Gegenprüfung offen):

1. aio-pikas `RobustConnection` stellt nach einem Reconnect jeden `RobustChannel` aus ihrem `WeakSet`
   wieder her, der noch nicht eingesammelt ist, auch explizit geschlossene oder verworfene
   (`_on_connected` ruft `restore()` für alle, `close()` entfernt den Kanal nicht). Mit abgeschalteter
   Garbage Collection standen nach einem Abriss 9 bis 11 zusätzliche Kanäle beim Broker, mit und ohne
   PF-19 a; mit GC meist keiner, in einem von drei Testläufen einer.
2. Nach einem Verbindungsabriss im Alarm zeigt `connection.transport` bis zum Reconnect (rund 5 s) auf
   die tote aiormq-Verbindung, deren Unblocked-Event nie mehr gesetzt wird. Ein Publish in diesem
   Fenster wartet in `_wait_while_blocked` bis zur Publish-Frist und endet als `BrokerPublishBlocked`,
   statt nach dem Reconnect zu senden (einmal in sechs Integrationsläufen beobachtet). Die Schranke
   „unblocked oder `closing`“ aus PF-19 a wäre dort dieselbe.
