---
title: "Spike PF-18 (b): Retry-Kopie im RabbitMQ-Speicheralarm ohne Slot warten lassen"
date: 2026-10-18
category: spikes
module: worker/retry, broker/rabbitmq
problem_type: spike
component: worker
status: findings-captured
applies_when:
  - ein Plan will Retry- oder DLQ-Kopien im Ressourcenalarm aus dem Slot nehmen (PF-18 b)
  - Änderungen am Verhältnis von Prefetch und concurrency oder an der Slot-Freigabe der Pipeline
  - eine Fehlersuche meldet einbrechenden Konsum eines Workers im Speicher- oder Plattenalarm
tags: [broker, blocked, alarm, retry, slot, prefetch, pf-18, e-025, e-019, spike]
entscheidungen: [E-025, E-019, E-041, E-043]
---

# Spike PF-18 (b): Retry-Kopie ohne Slot warten lassen

Stand: Worktree `fix/fehlersuche-runde-4-p2` (HEAD b022439), Test-RabbitMQ 4.3.6
(`swarmq-test-rabbit`, Port 5699), Valkey 6399 (DB 14, eigener Namensraum). Kein Produktionscode
geändert.

## Frage

Im Speicheralarm wartet eine Retry-Kopie in ihrem Slot bis zur Publish-Frist (E-025, Nachtrag 2);
bei jeder Fehlerrate über null bricht der Konsum ein. Steigt der Durchsatz, wenn der Worker vor dem
Publish den Blocked-Zustand prüft und die Kopie ohne Slot warten lässt (Nachricht unbestätigt,
Prefetch begrenzt, Slot frei)? Was kostet das, und was sagen die Entscheidungen dazu?

## Erfolgskriterium

Aus PF-18 (Register, Spalte Empfehlung): Integrationstest auf einem Speicheralarm mit wenigen Slots,
zwei immer scheiternden und vielen schnellen Tasks. Erfolg: Der Durchsatz der schnellen Tasks im
Alarm liegt deutlich über dem heutigen Wert (Ziel: nahe am Wert ohne Alarm), ohne neue Verluste oder
Doppelläufe und ohne die Regel „wartet bis zur Publish-Frist, dann `BrokerPublishBlocked`“ (E-025)
aufzugeben.

## Aufbau

- In-Process-Worker (`SwarmQ.start_worker`), `concurrency=4`, `publish_timeout_s=10`,
  getrennte Publish-Verbindung (Default).
- 2 Tasks, die immer scheitern (`max_retries=10000`, `fixed`, 0,2 s), und 1500 schnelle Tasks
  (0,1 s), alle **vor** dem Alarm eingestellt.
- Alarm per `rabbitmqctl set_vm_memory_high_watermark 0.0000001` (nur unter der Testsperre
  `scripts/with-test-lock.sh`, Container-Port geprüft), dann Worker starten, 30 s messen, Wasserlinie
  exakt auf den gelesenen Wert zurück (`absolute 3221225472`, auch im Fehlerfall per `trap`),
  15 s nachmessen.
- Varianten (Prototypen nur im Probe-Prozess):
  - `heute`: unverändert.
  - `ohne-slot`: `GuardedBroker.retry_later` kehrt bei blockierter Publish-Verbindung sofort
    zurück (Slot frei); ein Hintergrund-Task wartet höchstens die Publish-Frist auf `unblocked`,
    publiziert dann die Kopie (samt Ack/Superseded wie heute) oder stellt das Original per
    `redeliver` zurück (E-025/E-041 bleiben).
  - `ohne-slot-qos`: zusätzlich `basic.qos` auf dem Konsumkanal je wartender Kopie +1.
  - `heute-prefetch8`, `ohne-slot-prefetch8`: Consumer mit Prefetch 8 statt 4 angemeldet
    (Semaphor bleibt 4).
- Probe-Code (flüchtig): Scratchpad `fix4/spikes/pf18_probe.py`, `alarm.py`, `run_locked.sh`.

## Messwerte

Nennleistung ohne Alarm: 4 Slots / 0,1 s = 40/s; gemessen nach der Aufhebung 36,8–38,9/s.

| Variante | Prefetch | schnelle Tasks/s im Alarm (31 s) | nach Aufhebung | Kopien im Alarm wartend ohne Slot | `BrokerPublishBlocked` | `PublishIndeterminate` | Redeliver nach Frist |
|---|---|---|---|---|---|---|---|
| `heute` | 4 | **19,0** | 36,8 | – | 5 | 1 | – |
| `ohne-slot` | 4 | 19,4 | 38,3 | 7 | 0 | 1 | 5 |
| `ohne-slot-qos` | 4 → 6 (dynamisch) | 19,2 | 36,9 | 7 | 0 | 1 | 5 |
| `heute-prefetch8` | 8 | 20,1 | 38,9 | – | 3 | 1 | – |
| `ohne-slot-prefetch8` | 8 | **35,7** | 25,5¹ | 7 | 0 | 1 | 5 |

¹ Backlog nach rund 8 s abgearbeitet (1118 + 382 = 1500).

Schnelle Tasks je 5 s (Alarm bis ≈ 31 s):

| Variante | 0–5 | 5–10 | 10–15 | 15–20 | 20–25 | 25–30 | 30–35 | 35–40 | 40–45 |
|---|---|---|---|---|---|---|---|---|---|
| `heute` | 92 | 94 | 94 | 94 | 94 | 94 | 170 | 184 | 184 |
| `ohne-slot` | 92 | 94 | 94 | 94 | 94 | 94 | 184 | 190 | 192 |
| `ohne-slot-prefetch8` | 144 | 144 | 194 | 196 | 192 | 196 | 196 | 192 | 46 |

Kanalnummern der Publish-Verbindung in allen Läufen: 1, nach dem Verwurf im Alarm 2. In jedem Lauf
genau eine Kopie `PublishIndeterminate` (der auslösende Publish vor `connection.blocked`, D-1) und
die aiormq-Zeile „Got frame for closed channel 1“ bei der Aufhebung (PF-19).

## Befunde

1. **Slot frei allein hilft nicht.** Der Consumer hat Prefetch = `concurrency`. Die wartende,
   unbestätigte Nachricht hält ihren Prefetch-Platz; der Broker liefert nicht nach. Der Durchsatz
   bleibt bei der Hälfte (19,4/s statt 19,0/s).
2. **Prefetch nachträglich erhöhen wirkt nicht.** `basic.qos` mit `global=false` gilt in RabbitMQ
   nur für danach angemeldete Consumer; der laufende Consumer behält 4 (`ohne-slot-qos`: 19,2/s).
3. **Erst mit Prefetch-Puffer wirkt es.** Prefetch 8 und Slot-Freigabe: 35,7/s im Alarm, 92 % des
   Werts ohne Alarm. Prefetch 8 allein ändert nichts (20,1/s), weil die Kopien den Slot halten.
4. **Der D-1-Slot bleibt.** Die erste Kopie geht auf den Socket, bevor der Worker die Blockade kennt
   (`PublishIndeterminate` nach 10 s); diesen Slot rettet keine Variante (erste 10 s: 144 statt 196
   je 5 s).
5. **Reihenfolge mit dem Ausführungsschutz (nach Code, nicht gemessen).** Eine Entkopplung unterhalb
   von `GuardedBroker` (in `RabbitMQBroker.retry_later`) wäre gefährlich: `GuardedBroker` setzte den
   Superseded-Marker, bevor die Kopie existiert; scheitert sie später und kommt das Original zurück,
   überspränge der Ausführungsschutz es als `superseded`, der Task ginge verloren. Der Prototyp
   entkoppelt deshalb oberhalb; dann gibt aber das `finally` der Pipeline den Running-Marker frei
   (`ExecutionGuard.settle`), und nach dem späteren Publish fehlt der Superseded-Marker. Für
   F4-15/E-043 (`retry_ack_failed_after_publish`, Neuzustellung als `superseded` überspringen) wäre
   das ein Rückschritt. Eine echte Umsetzung braucht eine Übergabe der Lease an den Hintergrund-Task
   in `pipeline.py` (heute 795 Zeilen, Stop bei 800, KTD5).

## Entscheidungen

- **E-025:** Kein Widerspruch zur Regel selbst: Der Prototyp wartet höchstens die Publish-Frist und
  stellt dann zurück. Geändert würde die Bewusste Folge aus Nachtrag (2) („wartet im Slot“), die den
  Prüfauftrag (b) ausdrücklich nennt; bei Umsetzung braucht E-025 einen Nachtrag.
- **E-019 (Prefetch pro Consumer):** berührt. Wirksam ist (b) nur mit Prefetch > `concurrency`; ein
  Worker hielte dann mehr unbestätigte Nachrichten (K Queues × Prefetch), die gegen den
  `consumer_timeout` laufen und anderen Workern fehlen. Das verschiebt die dokumentierte Grenze.
- **E-041/E-043:** Die Rückstellung nach der Frist bleibt E-041. E-043 (Retry-Kopie, F4-15) hängt
  am Superseded-Marker, siehe Befund 5.
- **E-011, E-047:** kein Bezug gefunden. Beide regeln Scheduler-Publishes (Cron-Unique, Doppel-Leader);
  der Kopie-Pfad des Workers berührt sie nicht.

## Empfehlung für Georg

**Nicht umsetzen in 0.13.** Der Gewinn (im Beispiel 19 → 36 schnelle Tasks/s, nur während eines
Alarms und nur bei scheiternden Tasks) setzt drei Änderungen voraus, die jede für sich größer ist als
der Nutzen: Prefetch-Puffer über `concurrency` (E-019), Lease-Übergabe in der Pipeline am
Zeilenlimit, und ein neuer Nachtrag zu E-025. Die beschlossene Option (a), eine kürzere
Worker-Publish-Frist dokumentieren, bleibt der billige Hebel (Einbruch ∝ Frist). Falls Alarme mit
hoher Fehlerrate im Betrieb häufig werden: eigener Plan mit kleinem Prefetch-Puffer (z. B.
`concurrency + 1`), Lease-Übergabe und Integrationstest auf diesem Aufbau als Festschreibung.
Keine „Entscheidung überdenken?“-Markierung nötig.
