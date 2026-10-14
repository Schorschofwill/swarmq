# Fehlersuche 1: Regressionen aus 0.12 (v0.11.0..v0.12.0)

Worktree: `swarmq-fehlersuche` (Stand v0.12.0). Nur Code- und Doku-Analyse, keine Tests ausgeführt.
Gelesen: Register `docs/befunde/betrieb-ueber-zeit-2026-10.md`, Changelog 0.12.0/rc1/rc2 (DE),
`git log v0.11.0..v0.12.0`. Im Diff gezielt gelesen: `keyspace.py`, `queue_names.py`,
`broker/rabbitmq/{declarations,topology,delivery,admin,redeclare,arg_conflict}.py`,
`worker/gates/{__init__,delivery_count,lock,unknown_task,expiry,schedfire,reject,rate_limit,execution_guard}.py`,
`worker/{redelivery,lock_keeper,shutdown,terminal,finalize,retry}.py`, `lua/{lock_*,guard_*,sched_poll}.lua`,
`backend/valkey/{connection,__init__,locks,execution_guard,environment}.py`,
`scheduler/{stepdown,unowned,fire,unroutable,orphaned_queues,sync,epoch,loop}.py`,
`process_manager/{restart_budget,supervisor,entry}.py`, `shutdown_budget.py`, `backoff.py`, `identity.py`.

Geprüft und **ohne Befund** (damit niemand es doppelt prüft): Namensraum-Konsistenz (kein
`sq:`-Literal außerhalb von `keyspace.py`; alle Stores erben den KeySpace des Backends,
`reconcile_namespace` verwirft die Stores; Lua baut keine Keys selbst), Delay-Stufen-Routing
und Neu-Deklaration einer gelöschten Stufe, Header-Kopien von Retry/Rückstellung/`dlq retry`
(Broker-Zähler werden dort entfernt), Lock-Keeper/`lock_extend.lua`, Guard-Lua (Backlog-Feld,
nie verkürzen), `sched_poll.lua` (BZ-28), Stepdown-Zählung, Poll-Abbruch nach Leaderverlust,
Valkey-Fehlerklassifikation (OOM/READONLY/MISCONF gelten als Ausfall), Shutdown-Budget-Phasen.

---

## R-1 (P2) – DLQ-Kopie trägt die Broker-Zähler weiter; UI-„Move“/Shovel-Replay einer am Zustelllimit dead-gelettereten Nachricht landet sofort wieder in der DLQ (BZ-08 nur teilweise behoben)

**Fundstelle**
- `src/swarmq/worker/retry.py:307` – `RetryHandler.dead_letter` kopiert *alle* Header des Originals:
  `headers = {**(dict(delivery.headers) ...)}` – inklusive `x-delivery-count` / `x-acquired-count`.
- Gleiches Muster: `src/swarmq/worker/gates/reject.py:33` (neu in rc2, P5b),
  `src/swarmq/worker/gates/expiry.py:119`.
- `src/swarmq/broker/rabbitmq/delivery.py:143` (`_publish_to_dead_letter_queue`) entfernt nur
  `LOOP_STATE_HEADERS`, `identity.dead_letter_generation`/`copy_headers` nur `x-death`.
- Gegenstück, das es richtig macht: `worker/gates/__init__.py:170-178` (`BROKER_DELIVERY_COUNTERS`,
  `_NOT_COPIED`, für Retry-/Rückstellungskopien) und `broker/rabbitmq/admin.py:329`
  (`dlq retry`/`list` entfernen die Zähler). Der Kommentar dort hält selbst fest:
  *„RabbitMQ 4 behält einen vom Publisher gesetzten `x-delivery-count` bei“*.
- Auslöser beim Replay: `worker/gates/delivery_count.py:92` (`delivered >= threshold`).

**Szenario**
1. Ein Task bringt den Worker zum Absturz/Hängen (SIGSEGV, OOM, Endlosschleife). Nach 19
   Zustellungen schickt das DeliveryCountGate ihn mit `WorkerHungError` in die XQ. Die DLQ-Kopie
   trägt `x-delivery-count: 19` (bzw. auf 4.3 `x-acquired-count: 19` bei `RedeliveryLimitError`).
2. Bug gefixt, Operator spielt die XQ per RabbitMQ-UI „Move messages“, Shovel oder swarmv zurück –
   genau der Weg, den BZ-08 laut Changelog jetzt unterstützt, und den die Migrationsanleitung
   (`website/explanation/delivery-semantics.de.md:858-862`, „per Shovel zurückholen“) empfiehlt.
3. Die zurückgelegte Nachricht trägt den publisher-gesetzten Zähler 19. Das DeliveryCountGate
   sieht `delivered (19) >= threshold (19)` und dead-lettert sie **vor jeder Ausführung** erneut
   als `WorkerHungError` – mit Fehlerergebnis, `TASK_DLQ`, Workflow-Fail. Der Task läuft nie.
   Gerade die Klasse von Nachrichten, die man nach einem Poison-Fix am ehesten zurückspielt
   (Hänger/Abstürze), ist damit nicht replaybar; nur `swarmq dlq retry` funktioniert.
   Nebenwirkung auch unterhalb der Schwelle: `broker_redelivered()` ist für das Replay wahr
   (setzt Rückstellzähler/Lock-Wartezeit zurück, falsche Annahme „Broker hat neu zugestellt“).

**Warum die Tests es nicht finden**: `tests/integration/test_befund_bz08_replay_without_id.py`
nutzt einen Task, der beim *ersten* Zustellversuch scheitert (`max_retries=0`) – die DLQ-Kopie
hat keine Zähler. Unit-Tests von `republish_headers` prüfen nur die Retry-/Rückstellkopie, nicht
`RetryHandler.dead_letter`.

**Nachweistest**
- Unit (`tests/unit/worker/test_retry.py`): `RetryHandler.dead_letter` mit einer Delivery, deren
  Header `{"x-delivery-count": 19, "x-acquired-count": 19}` tragen; Fake-Broker zeichnet
  `dead_letter(headers=...)` auf; assert: weder `x-delivery-count` noch `x-acquired-count` in den
  publizierten Headern. Gleiches für `reject_to_dead_letter` und das Expiry-Gate.
- Integration (Variante des BZ-08-Tests): Task, der beim ersten Lauf `os._exit(1)` o. ä. – oder
  einfacher: Nachricht direkt mit `x-delivery-count: 19` in die Haupt-Queue publizieren, vom
  Worker dead-lettern lassen, per `_move_back_like_shovel` zurücklegen, erwarten `runs == [..., "run"]`
  statt erneutem `task_sent_to_dlq` mit `WorkerHungError`.

**Konfidenz**: mittel-hoch (Code eindeutig; das Beibehalten des publisher-gesetzten Zählers durch
RabbitMQ 4 steht als Annahme im eigenen Code-Kommentar, nicht in einem Spike).

---

## R-2 (P2) – BZ-11-Zurückhaltung blockiert Crons dauerhaft, wenn die einzigen Worker mit dem Task auf 0 skaliert sind (oder länger als 3 Meldetakte fehlen)

**Fundstelle**: `src/swarmq/scheduler/unowned.py:96-106` (`_is_unowned`), `:122-142`
(`_hold_unowned`), `scheduler/loop.py:540`, Lebendigkeitsfenster `scheduler/reports.py:35-37`
(`3 × scheduler_seen_interval_s` = 180 s), `scheduler/sync.py:102`.

**Szenario**: Getrennte Deployments (der Changelog nennt selbst „getrennte CPU- und IO-Worker“,
BZ-09): Der Leader läuft im IO-Pool, Cron `nightly_report` gehört zu einem Task, der nur im
CPU-Deployment registriert ist. Das CPU-Deployment skaliert per KEDA (Queue-Länge) auf 0, sobald
seine Queue leer ist – ein gängiges Muster. Nach 180 s meldet kein lebender Worker den Cron mehr;
er ist `known` (ein 0.12-Worker hat ihn je gemeldet), der Leader registriert ihn nicht → `_is_unowned`
ist wahr. Der Leader committet den Termin bei jedem Takt auf `now + 60 s`, **ohne zu publizieren**.
Es entsteht nie eine Nachricht, die Queue bleibt leer, KEDA skaliert nie hoch → der Cron feuert nie
wieder (bis der Prune nach 24 h die Definition entfernt; danach existiert er gar nicht mehr, bis ein
CPU-Worker zufällig startet). Bis 0.11 hat der Leader gefeuert (nur mit Warnung
`scheduler_task_not_in_local_registry`), die Nachricht hat das Hochskalieren ausgelöst.
Abgeschwächt auch ohne Scale-to-zero: ein CPU-Deployment, das > 3 min weg ist (Crashloop,
`Recreate`-Deploy, Wartung), verliert alle Fires dieser Zeit statt sie zu puffern.

Der Changelog („Der Scheduler-Leader hält jetzt einen statischen Cron zurück, ... den kein lebender
Worker mehr meldet“) und der Guide nennen weder Scale-to-zero noch einen Schalter, um die
Zurückhaltung abzuschalten. Die Register-Entscheidung BZ-11 zielte auf *entfernte* Crons, nicht auf
vorübergehend abwesende Worker-Pools.

**Warum die Tests es nicht finden**: Nachweis `tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py`
prüft nur „entfernter Cron wird zurückgehalten“, nicht „Worker-Pool vorübergehend 0, Cron muss
trotzdem feuern“. Kein Test kombiniert Hold mit einem Leader aus einem anderen Pool.

**Nachweistest** (Unit, `tests/unit/scheduler/`): SchedulerLoop als Leader ohne eigene
`static_crons`; Fake-Store mit `known_cron_names = {"c"}`, `cron_reports` mit Meldung von `c`,
deren `last_seen` 181 s zurückliegt; fälliger Eintrag `c`. Erwartung (fachlich, zu entscheiden):
mindestens nach einer Frist (z. B. `scheduler_unowned_hold_max_s`) oder per Konfiguration wird
publiziert. Heute: `broker.publish` nie aufgerufen, nur `commit_fire` auf `now + 60 s` – rot.

**Konfidenz**: mittel (Logik eindeutig; ob Scale-to-zero bei Nutzern vorkommt, ist eine
Einsatzfrage – KEDA auf RabbitMQ-Queues ist aber Standard).

---

## R-3 (P3) – Expiry-Gate: Ack-Fehler nach liegender DLQ-Kopie führt zu Doppelkopie, doppeltem `TASK_EXPIRED` und (bis zur zweiten Runde) nicht freigegebenem Unique-Flag

**Fundstelle**: `src/swarmq/worker/gates/expiry.py:118-132`.

**Szenario**: `broker.dead_letter` publiziert die Kopie und scheitert dann am Ack
(`SettleFailedAfterPublish`, Unterklasse von `BrokerConnectionError`) oder endet mit
`PublishIndeterminate`. Der `except BrokerConnectionError`-Zweig behandelt das wie „nichts
publiziert“: `redeliver_in_place` + `Terminal.never_release()`. Die Neuzustellung ist wieder
abgelaufen → zweite DLQ-Kopie, zweites `TASK_EXPIRED`/Metrik, zweiter Fail-Mark, erst dann
BZ-22-Freigabe. Alle anderen terminalen Pfade nutzen `dead_letter_copy`
(`worker/gates/__init__.py`), das `SettleFailedAfterPublish` als Erfolg wertet – der BZ-22-Fix
hat dieses Gate angefasst, den Unterpfad aber nicht angeglichen.

**Warum die Tests es nicht finden**: `tests/unit/worker/test_befund_bz22_unique_expiry.py` deckt
den Erfolgsfall ab; kein Test mit `SettleFailedAfterPublish` im Expiry-Gate.

**Nachweistest** (Unit): Fake-Broker, dessen `dead_letter` `SettleFailedAfterPublish` wirft;
abgelaufene Delivery mit `x-swarmq-unique-hash`; erwarten: kein `redeliver`, Outcome
`release_only`, genau ein `TASK_EXPIRED`.

**Konfidenz**: hoch (Code), Auswirkung gering.

---

## R-4 (P3) – Backoff am Deckel nicht mehr monoton; effektiver Retry-Zeitraum schrumpft

**Fundstelle**: `src/swarmq/backoff.py:31-48` (BZ-26).

**Szenario**: `base=1, max=3600`: Versuch 11 → `2048 × j`, j ∈ [1, 2]; bei j = 1,7 → 3482 s
(unter dem Deckel, ungekappt). Versuch 12 → `4096 × j > 3600` → 2160–2880 s, also *kürzer* als
der Versuch davor. Jeder gekappte Retry wartet höchstens 0,8 × `retry_max_delay`; wer
`max_retries × retry_max_delay` auf eine Ausfallzeit (Drittanbieter) bemessen hat, landet 20–40 %
früher in der DLQ. Der Changelog nennt „0,6 bis 0,8 × retry_max_delay statt genau den Cap“, aber
nicht die Nicht-Monotonie und die Verkürzung des Gesamtfensters; `SWARMQ_RETRY_MAX_DELAY` ist als
„Obergrenze“ dokumentiert, faktisch liegt die Dauer-Obergrenze bei 0,8 ×.

**Warum die Tests es nicht finden**: `tests/unit/test_befund_bz26_backoff.py` prüft Jitter am
Deckel und Overflow, nicht die Folge über die Deckelgrenze.

**Nachweistest** (Unit): `random.uniform` patchen (1,7 für Versuch 11, 1,0 für Versuch 12);
assert `calculate_backoff(12, ...) >= calculate_backoff(11, ...)` – heute rot. Fachlich: Jitter
z. B. als `max × (0,8 + 0,2 u)` oder Deckel vor dem Jitter anwenden.

**Konfidenz**: hoch (Code); ob es ein Fehler ist, ist eine Produktfrage.

---

## R-5 (P3) – Stepdown in einer Flotte ohne zweiten Kandidaten verlängert nur die Pause

**Fundstelle**: `src/swarmq/scheduler/stepdown.py:127-152`.

**Szenario**: Ein einzelner Worker (oder alle Worker hinter demselben Broker) erlebt einen
Broker-Ausfall länger als `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S`. Der Leader tritt zurück und
bewirbt sich `2 × scheduler_lock_ttl` nicht. Bei einem Kandidaten (oder wenn alle Kandidaten
nacheinander zurückgetreten sind) gibt es nach der Broker-Erholung bis zu `2 × lock_ttl` keinen
Leader; Crons und `delay/eta` feuern verspätet. Bei mehreren Kandidaten rotiert die Führung im
Ausfall reihum (Signal-/Log-Lärm `SCHEDULER_LEADER_FAILOVER`), ohne Nutzen, weil alle denselben
Broker haben. Kein Verlust (überfällige Termine feuern danach), aber vermeidbare Latenz. Der Sinn
(BZ-10) greift nur bei Apps mit *unterschiedlichen* Brokern an einem Valkey.

**Warum die Tests es nicht finden**: Stepdown-Tests prüfen die Zählung und den Rücktritt, nicht
die Zeit bis zur Wiederaufnahme bei nur einem Kandidaten.

**Nachweistest** (Unit): SchedulerLoop mit einem Kandidaten, Broker wirft 2× `BrokerConnectionError`
über die Frist, dann Erfolg; Uhr vorspulen um `lock_ttl + ε`; erwarten: Leader wieder aktiv
(oder Rücktritt nur, wenn ein anderer Kandidat lebt). Heute erst nach `2 × lock_ttl`.

**Konfidenz**: mittel (Verhalten wie dokumentiert; Bewertung als Fehler ist Ermessen).

---

## Verworfene Verdachte (geprüft, kein Fehler)

- Delay-Stufen ohne Queue-TTL und `expiration=0`: aio-pika kodiert 0 als `"0"`, keine hängenden Nachrichten.
- `x-acquired-count` zählt Prefetch-Rückgaben beim Shutdown: laut Spike S4 zählt Channel-Abbruch auf 4.2/4.3 auch `x-delivery-count`, also keine neue Verschärfung.
- Namensraum nach `reconcile_namespace`: Stores werden verworfen, Ergebnis-/Kanal-Keys lesen `self._keys` zur Laufzeit.
- OOM/READONLY/MISCONF unter `noeviction` würden das Zustelllimit verbrennen: `as_backend_error` stuft sie als Ausfall ein (Rückstellung statt Requeue).
- Supervisor-/Worker-Budget: der Worker beginnt vor dem `started` des Supervisors, die Eskalation trifft keine Aufräumphase vorzeitig.
- Schedfire-Duplikat nach Stepdown mitten im Batch: `_poll_tick` bricht bei Standby ab, Restduplikate fängt der Schedfire-Key.
