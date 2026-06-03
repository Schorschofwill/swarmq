---
date: 2026-06-02
topic: inject-existing-pools
---

# Bestehende RabbitMQ-/Valkey-Pools injizieren (Producer)

## Summary

`RabbitMQBroker` und `ValkeyBackend` sollen beim Konstruieren ein bereits bestehendes Verbindungsobjekt entgegennehmen können — statt nur eine URL, aus der swarmq selbst eine neue Verbindung aufbaut. Eine sendende App kann so ihren vorhandenen Pool wiederverwenden, anstatt eine zweite, parallele Verbindung zu öffnen.

---

## Problem Frame

Eine App, die swarmq nur zum Senden von Tasks nutzt (`schedule`, `run`, `get_result`, `apply`, `cancel`), hält in der Regel bereits eigene Verbindungen zu RabbitMQ und/oder Valkey — etwa für eigene Publishes, Caching oder andere Bestandteile derselben Infrastruktur. Heute ist das nicht teilbar: `RabbitMQBroker(url)` und `ValkeyBackend(url)` bekommen nur eine URL und bauen in `connect()` zwingend eine eigene robuste Connection plus Channels bzw. einen eigenen Valkey-Client auf.

Das Resultat sind doppelte, parallele Verbindungen zum selben Server für denselben Prozess: mehr offene Sockets, mehr Handshakes, mehr Ressourcen auf Client- und Serverseite, und zwei getrennte Lebenszyklen, die die App separat überwachen muss. Für eine reine Producer-Integration in eine App mit bestehendem Verbindungsbestand ist das unnötiger Overhead.

---

## Requirements

**Injektion am Konstruktor**
- R1. `RabbitMQBroker` muss alternativ zur AMQP-URL ein bestehendes Verbindungsobjekt annehmen können: entweder eine High-Level-aio-pika-(Robust-)Connection oder ein Low-Level-aio-pika-Pool-Primitiv.
- R2. `ValkeyBackend` muss alternativ zur Valkey-URL ein bestehendes Verbindungsobjekt annehmen können: entweder einen High-Level-`valkey.asyncio.Valkey`-Client oder einen Low-Level-`ConnectionPool`.
- R3. URL-basierte und injektionsbasierte Konstruktion müssen pro Backend unabhängig wählbar sein — Mischen ist erlaubt (z. B. Broker per URL, Backend per injiziertem Client).
- R4. Wird ein injiziertes Objekt übergeben, baut swarmq für dieses Backend in `connect()` keine eigene Verbindung mehr auf, sondern nutzt das übergebene Objekt. Bei einer geborgten RabbitMQ-Connection öffnet swarmq sich darauf weiterhin seinen eigenen Publish-Channel.

**Lifecycle / Ownership**
- R5. Injizierte Ressourcen (Client, Pool, Connection) dürfen beim `__aexit__` / `close()` von swarmq **nicht** geschlossen werden — sie gehören der aufrufenden App.
- R6. Verbindungsartefakte, die swarmq selbst auf einer geborgten Ressource erzeugt (insbesondere die Channels auf einer geborgten RabbitMQ-Connection), schließt swarmq beim Teardown sehr wohl.

**Producer-Grenze**
- R7. `start_worker()` muss eine klare `ConfigurationError` werfen, wenn der übergebene Broker oder das Backend mit einem injizierten Pool/Client konstruiert wurde. Injizierte Pools sind ausschließlich für den Producer-/Client-Pfad vorgesehen.

**Konfiguration**
- R8. Die Config-Validierung muss `broker_url` bzw. `backend_url` nur dann verlangen, wenn für das jeweilige Backend kein Verbindungsobjekt injiziert wurde. Bei Injektion entfällt die URL-Pflicht für dieses Backend.

---

## Acceptance Examples

- AE1. **Covers R4, R5.** Gegeben eine App hat einen eigenen `valkey.asyncio.Valkey`-Client; wenn sie `ValkeyBackend(client=...)` konstruiert und die App via `async with` betreibt, dann nutzt swarmq diesen Client für alle Backend-Operationen und lässt ihn beim `__aexit__` offen.
- AE2. **Covers R4, R6.** Gegeben eine App reicht eine bestehende aio-pika-Connection in `RabbitMQBroker(connection=...)`; wenn swarmq publiziert, öffnet es sich einen eigenen Publish-Channel auf dieser Connection; beim Teardown schließt swarmq diesen Channel, lässt die Connection aber offen.
- AE3. **Covers R7.** Gegeben ein Broker wurde mit injiziertem Pool konstruiert; wenn `start_worker()` aufgerufen wird, bricht swarmq mit einer `ConfigurationError` ab, bevor ein Consumer startet.
- AE4. **Covers R3, R8.** Gegeben eine App übergibt eine `broker_url`, aber einen injizierten Valkey-Client; wenn die Config validiert wird, ist die fehlende `backend_url` kein Fehler.

---

## Success Criteria

- Eine reine Producer-App mit bestehenden RabbitMQ-/Valkey-Verbindungen kann Tasks senden, ohne dass swarmq eine zweite Verbindung zum selben Server öffnet — beobachtbar an der Verbindungszahl auf dem Server.
- Das Schließen der swarmq-App beendet keine von der App geborgte Verbindung; die App kann sie danach weiter nutzen.
- Fehlbedienung (injizierter Pool im Worker) schlägt sofort und mit klarer Meldung fehl statt still in undefiniertes Verhalten zu kippen.
- `ce-plan` kann ohne weitere Produktentscheidungen die API-Signaturen, Typ-Erkennung und Validierungslogik ausarbeiten.

---

## Scope Boundaries

- Worker-Pfad mit injizierten Pools ist nicht im Scope — bewusst ausgeschlossen, um den Lifecycle der Worker-Manager (RateLimiter, LockManager, WorkflowAdvancer, Scheduler-Lua-Scripts) mit geborgten Clients nicht mitprüfen zu müssen. Stattdessen aktiver Guard (R7).
- Kein eigener Connection-Pool-Mechanismus für RabbitMQ jenseits des bestehenden 1-Connection-+-2-Channel-Modells; injiziert wird, was die App hält.
- Keine Änderung am bestehenden URL-basierten Standardpfad — er bleibt unverändert das Default-Verhalten.

---

## Key Decisions

- Injektion am Konstruktor von `RabbitMQBroker` / `ValkeyBackend` statt über die Config oder dishka: Die Pools sind bewusst keine DI-Kandidaten (Ist-Architektur), und der Konstruktor ist der vorhandene, natürliche Einstiegspunkt.
- Sowohl High-Level-Client/Connection als auch Low-Level-Pool akzeptieren (Union mit Typ-Erkennung): maximale Flexibilität, weil eine App mal das eine, mal das andere hält. Kosten: größere API-Oberfläche, mehr Validierung und Doku.
- „Don't close what you don't own" als Ownership-Regel (R5/R6): verhindert, dass swarmq der App die Verbindung wegzieht, behält aber die Verantwortung für selbst erzeugte Channels.
- Producer-Grenze hart per Guard statt nur per Doku (R7): verhindert stille Fehlbedienung; Kosten sind eine Prüfung plus ein Marker am Broker/Backend.

---

## Dependencies / Assumptions

- Annahme: Eine geborgte Valkey-Client/Pool-Instanz unterstützt alle vom Producer-Pfad genutzten Operationen (inkl. der Lua-Scripts, die der Client für `get_result`/`apply` braucht). Falls der Producer-Pfad bestimmte Backend-Fähigkeiten voraussetzt, in der Planung gegen die tatsächlich genutzten Operationen prüfen.
- Abhängigkeit: `SwarmQ.__init__` liest heute `broker._url` / `backend._url`, um die Config zu bauen. Dieser Pfad muss mit fehlenden URLs umgehen können (verknüpft mit R8).

---

## Outstanding Questions

### Deferred to Planning

- [Affects R1, R2][Technical] Wie wird der Objekttyp erkannt (isinstance gegen die konkreten aio-pika-/valkey-Klassen vs. Duck-Typing)? Welche genauen Typen gelten als „High-Level" bzw. „Low-Level"?
- [Affects R3][Technical] Verhalten bei Mehrdeutigkeit, wenn für dasselbe Backend gleichzeitig URL **und** Verbindungsobjekt übergeben werden. Default-Vorschlag aus dem Brainstorm: klarer Fehler statt stillem Vorrang — in der Planung final festlegen.
- [Affects R4][Technical] Bei einem injizierten Low-Level-Pool (aio-pika `Pool` / valkey `ConnectionPool`): wie bezieht swarmq daraus Connection/Channel bzw. Client, und wie verhält sich das zum bestehenden 1-Connection-+-2-Channel-Modell des Brokers?
- [Affects R7][Technical] Wie wird der „injiziert"-Zustand am Broker/Backend markiert, damit `start_worker()` ihn zuverlässig erkennt?
