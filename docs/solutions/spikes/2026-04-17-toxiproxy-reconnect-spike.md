---
title: Toxiproxy + aio_pika + valkey-py Reconnect-Spike
date: 2026-04-17
category: testing-infrastructure
status: findings-captured
---

# Toxiproxy Reconnect-Spike

## Ziel

Bevor der Chaos-Testing-Plan geschrieben wird: validieren, dass
**toxiproxy + aio_pika + valkey-py** tatsächlich so reconnecten wie im
Brainstorm angenommen. Jeder der acht geplanten Chaos-Tests fußt auf dieser
Annahme — wenn das Tooling anders funktioniert als gedacht, wächst der
Plan-Aufwand.

Harness: `docker compose` mit RabbitMQ 3.13 + Valkey 8 + Shopify/Toxiproxy.
Toxiproxy proxied AMQP (5672) und Valkey (6379). Admin-API auf 8474.

## Vier Messpunkte + Ergebnisse

### Spike 1 — aio_pika Producer + Consumer (`connect_robust`)

- `aio_pika.connect_robust(url, reconnect_interval=1.0)` reconnected die
  **Connection** automatisch nach `proxy.down()` + `proxy.up()`.
- **Consumer-Verhalten:** Consumer resumt nach Outage. Im Spike: 16 Messages
  vor Outage konsumiert, 4 nach Outage (Rest wurde nicht gepublished weil
  Producer-Channel kaputt war, siehe unten).
- **Producer-Verhalten — Überraschung:** Der zuvor geöffnete Channel wird
  nach Outage **nicht automatisch neu erstellt**. Jeder `publish()`-Call
  nach Reconnect wirft `ChannelInvalidStateError`.

**Konsequenz für SwarmQ:** Client-Code der langlebige Channels hält
(`RabbitMQBroker._publisher_channel`?) muss nach Connection-Recovery den
Channel neu öffnen. Entweder über einen Channel-Pool mit Lazy-Recreate oder
über `connect_robust`-internen Callback.

**Aktion:** Beim Chaos-Plan: Test dass `RabbitMQBroker` nach Broker-Restart
wieder publizieren kann. Falls nicht: Fix-Unit für Channel-Recreate.

### Spike 2 — valkey-py Key-Value

- Mit `retry=Retry(ExponentialBackoff(cap=2), retries=5)` +
  `retry_on_error=[ConnectionError, TimeoutError, OSError]` +
  `health_check_interval=1` sind SET/GET transparent gegen Outages.
- **Ergebnis:** 5 SET vor Outage + 10 SET nach Outage, **0 Fehler**.

**Konsequenz für SwarmQ:** Die aktuelle `ValkeyBackend`-Konfiguration muss
verifiziert werden — nutzt sie diese Retry-Policy? Falls nicht, ist
Key-Value-Stabilität unter Netzflattern schlechter als nötig.

**Aktion:** `grep "retry_on_error\|health_check_interval" src/swarmq/backend/`
zum Audit.

### Spike 3 — valkey-py Pub/Sub-Subscriber (KRITISCH)

- **Pub/Sub reconnectet NICHT automatisch.** Auch mit identischer Retry-
  Policy wie in Spike 2 sieht der Subscriber nach `proxy.down()` + `proxy.up()`:
  - `ConnectionError: Error while reading from ... : (104, 'Connection reset by peer')`
  - **0 Messages nach Recovery empfangen**, obwohl der Publisher erfolgreich
    wieder publishet.
- Die Retry-Policy von valkey-py bezieht sich auf einzelne Commands, nicht
  auf den langlebigen Pub/Sub-Socket. Nach einer Socket-Disruption muss der
  Client **`pubsub.subscribe(channel)` erneut aufrufen**, um wieder Messages
  zu empfangen.

**Das bestätigt den ce-review P2-Finding REL-002 / ADV-003 zu R3.7:**
`Client.watch_progress` hat ein Except-Sleep-Continue-Loop ohne Re-
Subscribe. Unter realem Netzflattern **verliert der Subscriber den Stream
dauerhaft**. Der im Commit 4158746 gelieferte "HASH-Fallback" liefert zwar
einen Snapshot, schließt aber die Pub/Sub-Lücke nicht.

**Aktion:** Follow-up-Fix für `watch_progress`: bei Exception im Pub/Sub-
Loop nicht nur HASH lesen, sondern auch `await pubsub.subscribe(channel)`
erneut aufrufen und den drain-confirmation konsumieren. Siehe §"Empfehlung"
unten.

### Spike 4 — AMQP Redelivery nach Connection-Break

- Quorum-Queue + unacked Message + Connection-Break (via toxiproxy
  `enabled:false`) → Message wird nach Reconnect **automatisch redelivered**
  mit `msg.redelivered == True`.
- **Ergebnis:** Delivery-Count 2, `redelivered=True` korrekt gesetzt.

**Konsequenz für SwarmQ:** Die Redelivery-Annahme des Worker-Pipelines hält
unter echter Netz-Chaos-Bedingung. Der `redelivered`-Header kann im Chaos-
Test als Signal verwendet werden, um Worker-Crash-Szenarien zu verifizieren.

## Empfehlungen für den Plan

1. **toxiproxy-Stack ist tragfähig.** Docker-Compose + Admin-API-REST-Calls
   aus dem Test funktionieren. Keine Blocker.

2. **Neues Chaos-Test-Szenario hinzufügen:**
   "RabbitMQ-Broker-Publish nach Netz-Outage" — verifiziert Channel-
   Recreate-Logik im `RabbitMQBroker`. Dieses Szenario war im Brainstorm
   nicht explizit; wird aber durch Spike 1 als reales Risiko sichtbar.

3. **R3.7 Pub/Sub-Resubscribe als separater Fix vor dem Chaos-Plan:**
   Der Spike zeigt, dass der aktuelle `watch_progress` unter Pub/Sub-
   Disruption Messages dauerhaft verpasst. Der Chaos-Test würde das
   ohnehin aufdecken — aber es ist ein bereits bekannter Bug (REL-002),
   der ohne Chaos-Stack behoben werden kann und dann im Chaos-Test
   verifiziert wird. **Reihenfolge:** erst Fix, dann Chaos-Test als
   Regression-Schutz.

4. **valkey-py Retry-Policy im Code auditieren:**
   Falls `ValkeyBackend` nicht bereits `retry=Retry(...)` +
   `retry_on_error=[...]` + `health_check_interval` setzt, ist das ein
   Plan-Unit für sich (einfach, aber bedeutsam).

5. **Chaos-Test-Harness-Bibliothek:**
   Der Spike zeigt, dass die REST-API von toxiproxy über `httpx` trivial
   ansprechbar ist. Kein Bedarf für `toxiproxy-python`-Package —
   Test-Fixture kann direkt `httpx.AsyncClient` nutzen. Das reduziert
   Abhängigkeiten.

## Artefakte

Spike-Code: `/tmp/toxispike/` (wird beim nächsten Boot verworfen — zur
Aufnahme in den Plan idealerweise nach `tests/chaos/_spike_archive/`
migrieren, falls als Referenz gewünscht).

Compose-Stack: `/tmp/toxispike/docker-compose.yml` — 25 LoC,
direkt in `tests/chaos/compose.yml` übernehmbar (mit angepassten Ports
für CI-Parallelität).

## Ausstehende Fragen

- **Channel-Pool-Strategie:** Hat `RabbitMQBroker` bereits einen Channel-
  Pool oder einen lazy-recreate-Mechanismus? Falls nein, ist das eine
  Erweiterung vor dem Chaos-Test.
- **Valkey-Pub/Sub unter lang anhaltendem Outage:** Der Spike testete nur
  2s Outage. Was passiert bei 30s+? valkey-py hat keinen automatischen
  Resubscribe — wir müssen ihn selbst implementieren. Wie lange soll der
  Subscriber in `watch_progress` hängen bleiben und resubscriben, bevor
  wir dem Caller eine Exception geben?
