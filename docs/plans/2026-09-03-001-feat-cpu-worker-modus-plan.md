---
title: CPU-Worker-Modus - Plan
type: feat
date: 2026-09-03
topic: cpu-worker-modus
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# CPU-Worker-Modus - Plan

## Goal Capsule

- **Objective:** swarmq bekommt einen CPU-Betriebsmodus: Ein Loop-Prozess, der nie rechnet, verteilt Tasks auf kurzlebige Rechenkinder aus einem vorgewärmten Forkserver, mit echter Killbarkeit über `hard_timeout` und Cancel (Phase 1). Als eigene, unabhängig lieferbare Phase im selben Plan bekommt der normale IO-Worker einen Lebenszeichen-Kanal zum Supervisor und Selbst-Recycling nach Taskzahl oder Speicher (Phase 2). Beide Phasen erscheinen als 0.5.0.
- **Product authority:** Der Product Contract in diesem Dokument. `docs/cpu-worker-machbarkeit.md` ist die Messgrundlage: Entscheidung, verworfene Alternativen, 52 Messpunkte und die zwölf Auflagen. Bei Widerspruch zwischen beiden gilt dieser Plan; Abweichungen von der Machbarkeitsdoku sind in Key Decisions benannt.
- **Not active scope:** Ein Task-Marker für Offload je Task (Ansatz A), `--autoscale` im CPU-Modus und Autoscaling auf Slot-Ebene sind benannte spätere Stufen, keine Anforderungen dieses Plans.
- **Open blockers:** Keine. Alle offenen Punkte sind an die Planung delegiert (Outstanding Questions).

---

## Product Contract

### Summary

Ein Worker startet mit `--pool cpu` in einem Betriebsmodus, in dem der Loop-Prozess ausschließlich Broker, Health, Metriken und Aufsicht bedient und jeden Task in ein Rechenkind aus einem vorgewärmten Forkserver auslagert. Tasks ohne DI bekommen ein frisches Kind je Task, Tasks mit DI ein wiederverwendetes Kind mit Recycling nach Speicher und Taskzahl. `hard_timeout` und Cancel töten das Kind und führen in den bekannten Fehlerpfad. Unabhängig davon läuft jeder Worker künftig unter einem Supervisor, meldet Lebenszeichen und kann sich nach konfigurierbaren Schwellen selbst recyceln.

### Problem Frame

swarmq ist reines asyncio und lagert rechenintensive Task-Körper nicht aus. Ein synchroner Pillow-, numpy- oder ImageMagick-Aufruf blockiert den Event-Loop und mit ihm alle nebenläufigen Tasks des Workers. Daraus folgen drei Probleme: CPU-Tasks laufen nicht wirklich parallel, ein `hard_timeout` greift nicht, weil asyncio-Cancel eine blockierte Loop nie erreicht, und jeder Betreiber mit CPU-Last baut das Offloading im Task-Körper selbst.

Der erste echte Anwendungsfall ist die Phoenix-Migration von taskiq: Bildpipeline (Konvertierung, Crop) und DTF-Verifier (ImageMagick). Dort laufen die Tasks heute in einem ThreadPool, der funktioniert, solange die Bibliothek die GIL freigibt. Er scheitert an zwei Stellen: Ein Thread ist nicht killbar, und ein Leak im Thread ist ein Leak im Worker. In Produktion gibt es OOM-Kills. Ihre Ursache ist unbegrenzte Nebenläufigkeit plus glibc-Speicherwachstum von rund 240 MB je 180 Bilder, nicht fehlende Killbarkeit. Der ThreadPool ist damit die Messlatte: Der CPU-Modus muss ihn im Durchsatz bei Vollauslastung mindestens erreichen und zusätzlich Isolation und Killbarkeit liefern. Die Latenz eines einzelnen Bildes bei niedriger Last darf mit einem Thread je Kind steigen.

Die Machbarkeit ist mit 52 Messpunkten aus sieben Serien belegt. Der tragende Fund: Ein Kind aus einem vorgewärmten Forkserver startet in 6 ms statt 114 ms bei `spawn` mit swarmq-Import. Bei 6 ms wird das Recycling-Problem gegenstandslos, weil ein frisches Kind je Task nichts kostet, was ein Leak überleben könnte.

### Key Decisions

- **Ansatz B: CPU-Modus als eigener Betriebsmodus mit Loop-Prozess und Rechenkindern aus dem Forkserver.** Nur die Prozessgrenze liefert Isolation und Killbarkeit; Offload-Primitive im Task (A), Aufsicht statt Auslagerung (C), Free-Threading, Subinterpreter und ThreadPool scheitern daran. (session-settled: user-approved — chosen over Ansatz A, Ansatz C, Free-Threading, Subinterpreter und ThreadPool: Abbrechbarkeit und Leak-Recycling sind Isolationsprobleme, und Isolation gibt es in CPython nur an der Prozessgrenze.) Governs R2, R6.
- **Volle Ausbaustufe als erste Lieferung.** Die Phoenix-Migration startet mit dem kompletten CPU-Modus, nicht mit einer Zwischenlösung, obwohl die OOM-Ursache Nebenläufigkeit ist. (session-settled: user-directed — chosen over Concurrency-Deckel plus Selbst-Recycling im IO-Worker als erste Stufe: die Migration soll nicht auf einer Zwischenlösung aufsetzen.)
- **Aktivierung per Worker-Flag.** Ein Worker läuft komplett im CPU-Modus; der Betreiber trennt CPU-Last per Queue-Bindung. (session-settled: user-directed — chosen over Task-Marker in `TaskInfo` und Queue-Eigenschaft in der Konfiguration: Prefetch, Ack-Reihenfolge und Slot-Zahl sind Eigenschaften des Worker-Prozesses, nicht eines Tasks.) Governs R1.
- **`--processes` kombinierbar, `--concurrency` bedeutet Slots, `--autoscale` abgelehnt.** Kein neues Flag für die Slot-Zahl; Autoscaling-Schwellwerte sind auf IO-Worker zugeschnitten. (session-settled: user-directed — chosen over Exklusivität mit genau einem Loop-Prozess, Autoscale auf Slots und voller Orthogonalität: der Supervisor ist ohnehin Teil der Topologie, Autoscale braucht eigene Schwellwerte.) Governs R3, R4.
- **`hard_timeout` und Cancel töten das Rechenkind; der Task folgt dem Fehlerpfad wie heute.** Das ist die Killbarkeit, die der Modus verspricht. Auflage 5 der Machbarkeitsdoku (kein harter Kill mitten in der Rechnung) gilt für Recycling-Kills, nicht für deklarierte Timeouts. (session-settled: user-directed — chosen over reines Soft-Verhalten in Stufe 1 und Kill mit direkter DLQ-Zustellung: ein deklarierter Timeout ist eine Entscheidung des Task-Autors, und Retry-Semantik bleibt einheitlich.) Governs R8, R9.
- **Frisches Kind je Task ohne DI, wiederverwendetes Kind mit Recycling bei DI.** Maximale Isolation, wo sie nichts kostet; Amortisation des Verbindungsaufbaus, wo sie zählt. (session-settled: user-approved — chosen over einheitlicher Wiederverwendung: bei 6 ms Kindstart ist Isolation je Task kostenlos, nur DI-Verbindungen rechtfertigen Wiederverwendung.) Governs R6, R7.
- **Supervisor wird Standard, auch bei einem Prozess.** Lebenszeichen, Recycling und Restart funktionieren überall gleich. (session-settled: user-directed — chosen over Recycling nur unter Supervisor und Exit-Code-Recycling für systemd: Schutz soll kein Opt-in sein, und der Supervisor-Pfad existiert bereits für `--reload`.) Governs R20.
- **Lebenszeichen standardmäßig an, Recycling-Schwellen Opt-in.** Kein stiller Verhaltenswechsel für Bestandsnutzer durch ungefragte Neustarts. (session-settled: user-approved — chosen over aktiven Recycling-Voreinstellungen: Schwellen hängen vom Workload ab und gehören dem Betreiber.) Governs R21, R22.
- **Beide Phasen als 0.5.0.** Ein Release, eine Ankündigung, keine Halbversion in Produktion. (session-settled: user-directed — chosen over getrennten Minors je Phase: die Phasen teilen die Recycling-Mechanik und sollen zusammen ankommen.) Governs R24.
- **Phase 2 im selben Plan statt eigenem Plan.** Beide Phasen teilen die Recycling-Mechanik; ein Dokument hält die Begrifflichkeit zusammen, die Umsetzung bleibt trennbar. (session-settled: user-directed 2026-08-31 — chosen over eigenem Plan für den IO-Worker-Schutz: der Rest von Ansatz C bekommt eigenständigen Umfang, aber keine eigene Begriffswelt.) Governs R23.
- **Der CPU-Modus ist POSIX-only.** Der Forkserver existiert auf Windows nicht; der IO-Worker-Schutz bleibt plattformneutral. (session-settled: user-approved — chosen over einem Spawn-Fallback für Windows: 114 ms Kindstart machen frische Kinder je Task unbezahlbar.) Governs R5.
- **Middleware-Hooks und Signale laufen im Loop-Prozess um den Offload; nur die DI-Auflösung passiert im Kind.** Übernommen aus der Machbarkeitsdoku; Hooks, die Kontext aus dem Task-Körper brauchen, sind eine Planungsfrage. Governs R12.

<!-- ce-section: work-relationships -->
### How This Work Fits Together

Dieser Plan besitzt zwei Phasen, die getrennt lieferbar sind und im selben Release erscheinen: Phase 1 ist der CPU-Betriebsmodus (R1 bis R19), Phase 2 der Schutz des IO-Workers (R20 bis R23). Die Aufteilung darüber hinaus ist der heutige Stand, keine festgelegte Roadmap.

- **Phase 2 — IO-Worker-Schutz.** Shares die Recycling-Mechanik mit Phase 1 (R23). Can proceed independently of Phase 1; ist ohne Phase 1 lieferbar.
- **Task-Marker für Offload je Task (Ansatz A).** Depends on Phase 1. Still to decide: ob ein Mischbetrieb in einem Worker den zweiten Bedeutungsraum für `--concurrency` und Prefetch rechtfertigt.
- **`--autoscale` im CPU-Modus und Autoscaling auf Slot-Ebene.** Depends on Phase 1 und eigenen Schwellwerten für Rechenslots.
- **Lasttest-Suite** (`docs/plans/2026-08-27-001-feat-swarmq-lasttest-suite-plan.md`). Enables die Abnahme des CPU-Modus über 24 Stunden und den `consumer_timeout`-Nachweis, die dieser Plan nicht führt. Voraussetzung: Der Lasttest-Plan bekommt ein Szenario, in dem die CPU-Worker-Container mit `--pool cpu` laufen; heute offloaden seine CPU-Archetypen im Task-Code. Ohne dieses Szenario ist der 24-Stunden-Nachweis für den CPU-Modus nicht erbracht.
- **Phoenix-Migration von taskiq.** Enabled by Phase 1; erster Anwendungsfall mit Bildpipeline und DTF-Verifier.

### Actors

- A1. **Betreiber** — startet Worker mit Flags, bindet Queues, setzt Schwellen und Umgebungsvariablen, liest Logs und Metriken. Im ersten Anwendungsfall das Phoenix-Team.
- A2. **Task-Autor** — schreibt Tasks mit `@task`, deklariert `hard_timeout`, `FromDishka`-Abhängigkeiten und Retry-Verhalten. Soll den Modus nicht in der Task-Signatur sehen.
- A3. **Supervisor** — Elternprozess ohne Event-Loop; startet, überwacht, ersetzt Worker- und Loop-Prozesse.
- A4. **Loop-Prozess** — der Worker im CPU-Modus; konsumiert vom Broker, prüft Gates, verteilt auf Slots, bestätigt Nachrichten, meldet Health und Metriken. Rechnet nie.
- A5. **Rechenkind** — aus dem Forkserver geforkt; führt genau den Task-Körper aus, löst DI auf, meldet Ergebnis und Fortschritt über den Rückkanal.
- A6. **IO-Worker-Prozess** — der heutige Worker mit Event-Loop unter dem Supervisor; sendet Lebenszeichen und recycelt sich nach Schwellen (Phase 2).

### Requirements

**Aktivierung und Topologie (Phase 1)**

- R1. Der Worker startet mit `--pool cpu` komplett im CPU-Modus. Ohne das Flag bleibt das Verhalten des IO-Workers unverändert.
- R2. Im CPU-Modus bedient der Loop-Prozess ausschließlich Broker, Health, Metriken, Gates und Aufsicht; jeder Task-Körper läuft in einem Rechenkind aus einem vorgewärmten Forkserver. Der Loop-Prozess führt nie einen Task-Körper aus.
- R3. `--concurrency` bedeutet im CPU-Modus die Zahl der Rechenslots. Die Voreinstellung ist die Zahl der dem Worker zugeteilten CPUs (CPU-Affinität und cgroup-Quote, aufgerundet), nicht die Kernzahl des Hosts und nicht 10. Bei `--processes N` wird die Voreinstellung durch N geteilt (mindestens 1), so dass die Summe der Slots die zugeteilten CPUs nicht übersteigt; eine explizite `--concurrency` übersteuert diese Aufteilung.
- R4. `--processes N` startet N Loop-Prozesse unter dem Supervisor, jeder mit eigenem Forkserver und eigenen Slots. `--autoscale` zusammen mit `--pool cpu` wird beim Start mit einer Fehlermeldung abgelehnt, die auf die spätere Stufe verweist.
- R5. Der CPU-Modus ist auf POSIX-Plattformen mit Forkserver verfügbar. Auf anderen Plattformen bricht der Start mit einer klaren Fehlermeldung ab.

**Ausführung, Isolation und Recycling (Phase 1)**

- R6. Ein Task ohne DI-Abhängigkeit läuft in einem frischen Rechenkind, das nach dem Task endet. Ein Task mit DI-Abhängigkeit läuft in einem wiederverwendeten Kind seines Slots; das Kind wird recycelt, wenn sein RSS nach Taskende eine Schwelle überschreitet (primär) oder es eine Taskzahl erreicht hat (zweite Linie). Gemessen wird nach Taskende, nie währenddessen.
- R7. Recycling ist rollierend: Das Ersatzkind ist startbereit, bevor das alte Kind endet, der Slot liegt nicht brach. Jedes Recycling wird mit Grund und Messwert als Log-Eintrag und als Metrik ausgegeben.
- R8. Überschreitet ein Task seinen `hard_timeout` oder wird er abgebrochen, tötet der Loop-Prozess das Rechenkind. Der Kill trifft das Rechenkind samt allen von ihm gestarteten Prozessen, weil jedes Kind eine eigene Prozessgruppe bildet; nach dem Kill lebt kein Nachkomme des Kindes mehr. Der Task folgt dem heutigen Fehlerpfad: Retry bis `max_retries`, danach DLQ. Der Slot ist danach sofort wieder nutzbar.
- R9. Stirbt ein Rechenkind ohne Ergebnis, etwa durch den OOM-Killer, wird der Task bei at-least-once-Zustellung wie ein Taskfehler behandelt und geht nie verloren: Er läuft durch die Retry-Bewertung des Workers mit Retry-Zähler, Backoff und DLQ nach `max_retries`, nicht über Nack mit Requeue, damit auch ein deterministisch sterbender Task die DLQ erreicht. Der Loop-Prozess beendet verbliebene Nachkommen des toten Kindes. Der Slot ist danach sofort wieder nutzbar.
- R10. Bei at-least-once-Zustellung bestätigt der Loop-Prozess eine Nachricht erst, nachdem das Ergebnis des Kindes vorliegt oder der Fehlerpfad entschieden ist, nie vor dem Offload. Tasks im Zustellmodus fire-and-forget werden wie heute vor der Ausführung und damit vor dem Offload bestätigt; stirbt ihr Kind, ist der Task wie heute verloren.
- R11. Der Loop-Prozess hält höchstens so viele Nachrichten unbestätigt, wie er Rechenslots hat.
- R12. Expiry, Rate-Limit, Lock und `max_concurrency` greifen im Loop-Prozess vor dem Offload, unverändert zum IO-Worker. Middleware-Hooks und Signale laufen im Loop-Prozess um den Offload. Der Task-Körper umfasst `pre_execution`, `run` und `post_execution`; alle drei laufen im Rechenkind, weil ihre `FromDishka`-Injektion den REQUEST-Container des Kindes braucht. `update_progress()` läuft über den Rückkanal vom Kind zum Loop-Prozess, der nach Valkey schreibt. Cancel erkennt der Loop-Prozess wie heute per Valkey-Poll und tötet das Kind (per R8).
- R13. Für den Task-Autor bleiben Signatur, Rückgabewert, Retry-, Lock- und Rate-Limit-Semantik unverändert. Drei Verhaltensänderungen sind dokumentiert: APP-scoped DI-Singletons leben nur so lange wie ihr Rechenkind, N Slots bedeuten N Verbindungspools, und bei `hard_timeout`, Cancel oder Kindtod laufen keine REQUEST-Scope-Finalizer, also kein Rollback durch yield-Provider; der Task-Autor legt Transaktionen so an, dass ein abgebrochener Prozess sie nicht halb festschreibt.

**Startbedingungen des Forkservers (Phase 1)**

- R14. Der Forkserver-Preload importiert, rechnet aber nie. Der Worker prüft nach dem Preload, dass der Forkserver genau einen Thread hat, und bricht sonst den Start ab.
- R15. Die Thread-Limits der Rechenbibliotheken (`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`, `MAGICK_THREAD_LIMIT`) sind gesetzt, bevor der Forkserver startet. Voreinstellung ist ein Thread je Kind; der Betreiber kann sie überschreiben.
- R16. `MALLOC_TRIM_THRESHOLD_` und `MALLOC_ARENA_MAX` sind wie die Thread-Limits aus R15 in der Umgebung des Loop-Prozesses gesetzt, bevor der Forkserver startet, und gelten damit für alle Rechenkinder; im Kind gesetzt wären sie wirkungslos, weil glibc die Werte nur beim Prozessstart liest. Die Voreinstellung und ihre Laufzeitkosten von rund 14 Prozent je Bild sind dokumentiert; der Betreiber kann sie überschreiben.
- R17. Der DI-Container wird ausschließlich im Rechenkind gebaut, nie im Forkserver. Ein Provider, der beim Import eine Verbindung öffnet, ist als Fehler des Task-Autors dokumentiert.
- R18. Eine `FromDishka`-Annotation, die keinen Dependency-Key ergibt (etwa `FromDishka[X] | None = None`), führt beim Worker-Start zu einem `ConfigurationError`. Das gilt in allen Worker-Modi.
- R19. Die maximale Tasklaufzeit ist eine Brokereinstellung. Die Dokumentation des CPU-Modus benennt RabbitMQs `consumer_timeout` von 30 Minuten, und der Worker warnt beim Start, wenn ein registrierter Task einen `hard_timeout` über dem konfigurierten Broker-Timeout hat.

**Schutz des IO-Workers (Phase 2)**

- R20. `swarmq worker` startet immer einen Supervisor mit einem Worker-Prozess darunter, auch ohne `--processes` oder `--autoscale`. Der Changelog von 0.5.0 nennt diesen Wechsel im Startverhalten. Endet der Worker-Prozess sauber mit Exit-Code 0, etwa nach dem Drain im `--burst`-Modus, füllt der Supervisor den Slot nicht nach und endet selbst mit Exit-Code 0, sobald kein Worker mehr läuft. Die Recycling-Absicht nach R22 meldet der Worker dem Supervisor über einen eigenen Kanal, nicht über den Exit-Code.
- R21. Jeder Worker-Prozess sendet dem Supervisor regelmäßig ein Lebenszeichen aus seinem Event-Loop; das ist standardmäßig an. Bleibt das Lebenszeichen länger als eine konfigurierbare Frist aus, gilt der Worker als hängend, und der Supervisor ersetzt ihn. Die Nachrichten des hängenden Workers gehen per Redelivery an andere Worker. Eine Nachricht, deren Worker wegen ausbleibendem Lebenszeichen ersetzt wurde, zählt als fehlgeschlagener Zustellversuch; nach dem Delivery-Limit der Queue landet sie in der DLQ, damit ein einzelner blockierender Task nicht fortlaufend Worker ersetzt. Die Dokumentation nennt Quorum-Queues mit Delivery-Limit als Voraussetzung, und der Worker warnt beim Start, wenn eine konsumierte Queue kein Limit trägt. Der Lebenszeichen-Kanal zum Supervisor ist ein neuer Kanal, getrennt vom bestehenden In-Process-Signal `WORKER_HEARTBEAT`, das nur Hook-Handler innerhalb des Workers erreicht.
- R22. Ein Worker recycelt sich selbst, wenn sein RSS nach Taskende eine Schwelle überschreitet oder er eine Taskzahl erreicht hat. Beide Schwellen sind Opt-in und standardmäßig aus. Das Recycling ist graceful: Der Worker nimmt keine neuen Nachrichten an, beendet laufende Tasks und endet; der Supervisor startet den Ersatz, bevor der alte Worker endet.
- R23. Recycling-Messung, Recycling-Grund und die Form von Log-Eintrag und Metrik sind in Phase 1 und Phase 2 dieselben (per R7).

**Release und Dokumentation**

- R24. Beide Phasen erscheinen als 0.5.0. Phase 2 ist ohne Phase 1 lieferbar; scheitert Phase 1, verschiebt sich nur Phase 1. Der Changelog führt neben dem Startverhaltens-Wechsel aus R20 auch die neue Startprüfung aus R18 als Breaking Change auf, mit dem betroffenen Signaturmuster und der Korrektur.
- R25. Die Dokumentation bekommt eine Seite zum CPU-Modus mit Aktivierung, CPU-Dimensionierung (Slots = zugeteilte CPUs, Pillow skaliert nur bis zur Kernzahl), Speicherdimensionierung (Speicherbedarf = Slots × RSS-Schwelle + Loop-Prozess + Forkserver + ein Ersatzkind, Schwelle aus dem cgroup-Limit abgeleitet), dem Latenz-Trade-off eines Threads je Kind samt Übersteuerung der Thread-Limits für latenzkritische Queues, Verbindungsvervielfachung, Lebensdauer von APP-Singletons, fehlenden Finalizern nach Kill, MALLOC-Voreinstellungen samt Kosten, `consumer_timeout` und dem Verhältnis zu `--processes`.

### Key Flows

- F1. Task ohne DI im CPU-Modus
  - **Trigger:** Der Loop-Prozess erhält eine Nachricht und hat einen freien Slot.
  - **Actors:** A4, A5
  - **Steps:** Gates prüfen (R12); frisches Kind aus dem Forkserver forken; Task-Körper im Kind ausführen; Ergebnis über den Rückkanal empfangen; Ergebnis speichern, Signale auslösen; Nachricht bestätigen (R10); Kind endet.
  - **Covered by:** R2, R6, R10, R12

- F2. Task mit DI, Kind-Wiederverwendung und Recycling
  - **Trigger:** Ein Task mit `FromDishka`-Abhängigkeit trifft auf einen Slot mit warmem Kind.
  - **Actors:** A4, A5
  - **Steps:** Task an das Kind des Slots übergeben; Kind löst DI auf und führt aus; Ergebnis über den Rückkanal; Loop-Prozess misst RSS des Kindes nach Taskende; liegt es über der Schwelle oder ist die Taskzahl erreicht, startet das Ersatzkind, dann erhält das alte Kind das Ende-Signal; Recycling wird mit Grund und Messwert geloggt und gemessen.
  - **Covered by:** R6, R7, R12, R13

- F3. `hard_timeout` oder Cancel im CPU-Modus
  - **Trigger:** Ein Task überschreitet seinen `hard_timeout`, oder ein Cancel für seine Task-ID liegt vor.
  - **Actors:** A4, A5
  - **Steps:** Loop-Prozess tötet das Rechenkind; Slot wird freigegeben; Task geht in den Fehlerpfad (Retry oder DLQ); Signale wie heute; Nachricht wird bestätigt.
  - **Covered by:** R8, R10

- F4. Kindtod ohne Ergebnis
  - **Trigger:** Ein Rechenkind endet unerwartet, etwa durch den OOM-Killer.
  - **Actors:** A4, A5
  - **Steps:** Loop-Prozess erkennt das Ende ohne Ergebnis; Task geht in den Fehlerpfad; Slot bekommt bei Bedarf ein neues Kind; Nachricht wird nicht vor der Entscheidung bestätigt.
  - **Covered by:** R9, R10, R11

- F5. Startbedingung verletzt
  - **Trigger:** `--pool cpu` mit `--autoscale`, auf einer Plattform ohne Forkserver, mit einem Preload, der rechnet, oder mit einer wirkungslosen `FromDishka`-Annotation.
  - **Actors:** A1, A4
  - **Steps:** Worker prüft die Bedingung beim Start; bricht mit einer Fehlermeldung ab, die die verletzte Bedingung benennt; kein Prozess bleibt zurück.
  - **Covered by:** R4, R5, R14, R18

- F6. IO-Worker recycelt sich selbst (Phase 2)
  - **Trigger:** Nach Taskende überschreitet der RSS die gesetzte Schwelle oder die Taskzahl ist erreicht.
  - **Actors:** A3, A6
  - **Steps:** Worker meldet Recycling-Absicht mit Grund und Messwert; Supervisor startet den Ersatz; Worker nimmt keine neuen Nachrichten an, beendet laufende Tasks, endet; Log und Metrik wie in Phase 1.
  - **Covered by:** R20, R22, R23

- F7. IO-Worker hängt (Phase 2)
  - **Trigger:** Das Lebenszeichen eines Workers bleibt länger als die konfigurierte Frist aus.
  - **Actors:** A3, A6
  - **Steps:** Supervisor markiert den Worker als hängend; startet den Ersatz; beendet den hängenden Worker mit SIGTERM, nach Frist mit SIGKILL; unbestätigte Nachrichten gehen per Redelivery an andere Worker.
  - **Covered by:** R20, R21

### Acceptance Examples

- AE1. **Covers R6, R7.** Given ein Slot mit warmem DI-Kind und einer RSS-Schwelle von 300 MB, when ein Task endet und der RSS des Kindes bei 320 MB liegt, then startet ein Ersatzkind, das alte Kind erhält das Ende-Signal, und Log sowie Metrik nennen Grund `rss` und den Messwert 320 MB.
- AE2. **Covers R6.** Given ein Task ohne `FromDishka`-Parameter, when er zehnmal hintereinander läuft, then wurde jeder Lauf in einem eigenen Rechenkind ausgeführt, und nach dem zehnten Lauf lebt keines dieser Kinder mehr.
- AE3. **Covers R8.** Given ein Task mit `hard_timeout=5` und `max_retries=1`, when sein Körper 60 Sekunden rechnet, then ist das Rechenkind nach etwa 5 Sekunden tot, der Task wird einmal wiederholt, landet danach in der DLQ, und der Slot hat inzwischen weitere Tasks ausgeführt.
- AE4. **Covers R9, R10.** Given ein Rechenkind wird während der Ausführung von außen mit SIGKILL beendet, when der Loop-Prozess das bemerkt, then wurde die Nachricht bis dahin nicht bestätigt, der Task folgt dem Fehlerpfad, und derselbe Slot führt den nächsten Task aus.
- AE5. **Covers R11.** Given ein Loop-Prozess mit 4 Slots und 100 wartenden Nachrichten, when alle Slots belegt sind, then hält der Loop-Prozess genau 4 Nachrichten unbestätigt, und ein zweiter Worker an derselben Queue erhält die übrigen.
- AE6. **Covers R4.** Given der Aufruf `swarmq worker --pool cpu --autoscale 2,8`, when der Worker startet, then bricht er vor dem Broker-Connect mit einer Fehlermeldung ab, die die Kombination als nicht unterstützt benennt.
- AE7. **Covers R14.** Given ein Preload-Modul, das beim Import eine numpy-Berechnung mit mehreren Threads ausführt, when der Worker im CPU-Modus startet, then stellt er fest, dass der Forkserver mehr als einen Thread hat, und bricht den Start ab.
- AE8. **Covers R18.** Given ein Task mit Parameter `db: FromDishka[Database] | None = None`, when der Worker startet, then endet er mit einem `ConfigurationError`, der den Task und den Parameter benennt.
- AE9. **Covers R12, R13.** Given ein Task mit Rate-Limit und `FromDishka`-Abhängigkeit im CPU-Modus, when Nachrichten schneller eintreffen als das Limit erlaubt, then greift das Rate-Limit im Loop-Prozess vor dem Offload, und die Abhängigkeit wird im Rechenkind aufgelöst, ohne dass der Task-Autor seine Signatur ändern musste.
- AE10. **Covers R20, R22.** Given `swarmq worker` ohne `--processes` und mit einer Taskzahl-Schwelle von 500, when der 500. Task endet, then startet der Supervisor einen Ersatz-Worker, der alte Worker nimmt keine neuen Nachrichten an, beendet laufende Tasks und endet, und kein Task ist verloren.
- AE11. **Covers R21.** Given ein IO-Worker, dessen Event-Loop durch einen synchronen Aufruf blockiert ist, when die Lebenszeichen-Frist verstreicht, then ersetzt der Supervisor den Worker, und die unbestätigten Nachrichten werden an einen anderen Worker zugestellt. Blockiert dieselbe Nachricht auch den Ersatz-Worker, landet sie nach dem Delivery-Limit der Queue in der DLQ, statt weitere Worker zu ersetzen.
- AE12. **Covers R22.** Given `swarmq worker` ohne gesetzte Recycling-Schwellen, when der Worker 10.000 Tasks ausführt, then wurde er kein einziges Mal recycelt.
- AE13. **Covers R8.** Given ein Task mit `hard_timeout=5`, der ImageMagick als Subprozess startet und 60 Sekunden rechnet, when der Timeout greift, then sind Rechenkind und ImageMagick-Prozess tot, und kein Prozess aus der Prozessgruppe des Kindes lebt weiter.

### Success Criteria

- Die 52 Messpunkte aus `docs/cpu-worker-machbarkeit.md` sind gegen die Implementierung reproduzierbar, insbesondere Kindstart im einstelligen Millisekundenbereich, Ack-Reihenfolge und Redelivery der I-Serie und das Recycling der H-Serie.
- Bildpipeline und DTF-Verifier aus Phoenix laufen im CPU-Modus mit Slots = zugeteilte CPUs, ohne dass der Loop-Prozess für Health oder Broker-Heartbeat blockiert.
- Bildpipeline und DTF-Verifier erreichen im CPU-Modus bei Vollauslastung mindestens den Durchsatz des heutigen ThreadPool-Betriebs in Phoenix, gemessen an einer Baseline aus dem Messfenster vor der Migration.
- Unter Dauerlast der Bildpipeline bleibt der Gesamt-RSS eines Loop-Prozesses samt Rechenkindern unter Slots × RSS-Schwelle plus Grundlast, gemessen nach Taskende, und über die Abnahmedauer tritt kein OOM-Kill auf.
- Ein Task, dessen Kind stirbt oder gekillt wird, ist im Backend als fehlgeschlagen oder in der DLQ sichtbar, nie verschwunden.
- Jedes Recycling ist in Logs und Metriken mit Grund und Messwert nachvollziehbar.
- Bestehende Unit-, Integrations- und e2e-Suiten bleiben grün; der IO-Worker verhält sich ohne gesetzte Schwellen wie vor 0.5.0, abgesehen vom zusätzlichen Supervisor-Prozess und dem Ersatz bei ausbleibendem Lebenszeichen.

### Scope Boundaries

**Deferred for later**

- Task-Marker für Offload je Task (Ansatz A) und Mischbetrieb von IO- und CPU-Tasks in einem Worker.
- `--autoscale` im CPU-Modus und Autoscaling auf Slot-Ebene.
- Recycling-Kill mitten in der Rechnung, etwa bei RSS-Überschreitung während eines Tasks. Den Host vor OOM schützt das cgroup-Limit.
- Dauerlauf über 24 Stunden und der Nachweis, dass `consumer_timeout` bei Langläufern greift. Beides gehört in die Lasttest-Suite, die dafür ein Szenario mit `--pool cpu` braucht (siehe How This Work Fits Together).
- Windows-Unterstützung des CPU-Modus.
- Automatische Erkennung von Providern, die beim Import verbinden (R17 dokumentiert, prüft aber nicht).

**Outside this product's identity**

- ThreadPool-Offload als eingebaute Alternative. Er ist die Messlatte, nicht das Ziel.
- Subinterpreter als Isolationsmechanismus. NumPy lädt dort nicht.

### Dependencies / Assumptions

- Python 3.14 mit funktionierendem `forkserver`-Startverfahren und Preload auf der Zielplattform.
- Der Branch `cpu-worker-machbarkeit` (Merge Request !44 mit `docs/cpu-worker-machbarkeit.md`, den Prototypen `prototypes/cpu_worker_*.py` und der Korrektur von `docs/worker-architecture.md`) ist in `main` gemergt, bevor die Planung beginnt; die hier zitierten Dateien existieren bis dahin nur dort.
- Der Host-OOM-Schutz kommt vom cgroup-Limit des Deployments, nicht vom Worker.
- RabbitMQ 4.x mit `consumer_timeout` von 30 Minuten als Broker des ersten Anwendungsfalls; Valkey als Backend für Ergebnis, Fortschritt und Cancel.
- Der Supervisor-Pfad existiert und wird heute bei `--processes` größer 1, `--autoscale` und `--reload` genutzt; R20 macht ihn zum Standard, ohne ihn neu zu erfinden.
- Die Amortisation der Kind-Wiederverwendung ist eine Untergrenze, gemessen mit einem 4-ms-Valkey-Ping; mit echten Datenbankpools fällt sie höher aus.
- Die Phoenix-Tasks sind picklebar in Argumenten und Rückgabewerten, wie es die Broker-Serialisierung bereits verlangt.

### Outstanding Questions

**Resolve Before Planning**

Keine.

**Deferred to Planning**

- Welche Middleware-Hooks brauchen Kontext aus dem Task-Körper (etwa Contextvars) und müssen deshalb im Rechenkind statt im Loop-Prozess laufen? Zu klären an der bestehenden Middleware-Kette.
- Wie erkennt der Worker, dass ein Task „mit DI" ist: `FromDishka` in der Signatur, oder ein registrierter Container?
- Mechanik des Rückkanals für Ergebnis, Fortschritt und Cancel zwischen Kind und Loop-Prozess.
- Voreinstellungen der Recycling-Schwellen für DI-Kinder im CPU-Modus, abgeleitet aus der H-Serie.
- Verhalten von `--reload` zusammen mit `--pool cpu`.
- Voreinstellung der Lebenszeichen-Frist in Phase 2 und der Umgang mit legitim langen synchronen Abschnitten.
- Woher der Worker den `consumer_timeout` des Brokers kennt, gegen den R19 warnt.

### Sources / Research

- `docs/cpu-worker-machbarkeit.md` — Entscheidung, verworfene Alternativen, 52 Messpunkte (Serien A bis I), zwölf Auflagen, Nutzerentscheidungen vom 2026-08-31.
- `prototypes/cpu_worker_feasibility.py`, `prototypes/cpu_worker_integration.py`, `prototypes/cpu_worker_parallel.py`, `prototypes/cpu_worker_lifecycle.py` — Messprogramme, die als Referenz für die Success Criteria dienen.
- `docs/worker-architecture.md` Abschnitt 2.3 — `spawn` für Worker-Subprozesse, `forkserver` für Rechenkinder, mit Messtabelle.
- `src/swarmq/cli/__init__.py:249-253` — der Supervisor-Pfad wird heute bei `--processes` größer 1, `--autoscale` und `--reload` gewählt.
- `src/swarmq/cli/parser.py:101-105` — `--concurrency` mit Voreinstellung 10.
- `src/swarmq/worker/pipeline.py:708` — Gate-Reihenfolge Expiry, Rate-Limit, Lock vor der Ausführung; DI-Teardown und Lock-Freigabe im `finally`.
- `src/swarmq/worker/execution.py:256-273` — heutiger `hard_timeout` als asyncio-Watchdog ohne Prozess-Kill.
- `src/swarmq/process_manager/supervisor.py:98-114,170` — Restart-Fenster, Autoscale-Schwellwerte, `spawn`-Kontext; keine RSS-Überwachung, kein Lebenszeichen-Kanal.
- `src/swarmq/task.py:83-110` — Felder von `TaskInfo`; kein Feld für den Ausführungsort.
- Phoenix-Repo, `docs/solutions/infrastructure/2026-08-28-worker-cpu-oom-vollanalyse.md` — OOM-Ursache, glibc-Wachstum, `MALLOC_TRIM_THRESHOLD_`-Messung.
- `docs/plans/2026-08-27-001-feat-swarmq-lasttest-suite-plan.md` — Lasttest-Suite als Produktions-Gate, übernimmt Dauerlauf und `consumer_timeout`-Nachweis.

## Deferred / Open Questions

### From 2026-09-03 review

- **DI-Status der Phoenix-Tasks entscheidet über den Abnahmepfad** — Dependencies / Assumptions (P2, adversarial, confidence 75)
  Ob Bildpipeline und DTF-Verifier `FromDishka` nutzen, legt fest, ob der 6-ms-Pfad mit frischem Kind je Task (F1) oder der Wiederverwendungspfad mit Recycling-Schwellen (F2) der Produktionspfad ist. Der Plan nennt Picklebarkeit als Annahme über die Phoenix-Tasks, aber nicht ihren DI-Status. Nutzen sie DI, werden die in die Planung vertagten Schwellen-Voreinstellungen zur einzigen Grenze gegen das gemessene glibc-Wachstum und gehören in eine Anforderung.
