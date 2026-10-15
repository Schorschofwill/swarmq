"""AMQP message header names used across Client, Worker, and Scheduler.

Keeping the literals here avoids the silent-typo hazard: a single
misspelling at any producer or consumer site skips the corresponding
pipeline stage without raising. Consolidated constants mean any typo
surfaces at import time instead.
"""

from __future__ import annotations

# R3.5 Unique Tasks — producer-side dedup flag identity + mode carried
# on every dedup-tagged message. Producer sets both on publish; worker
# reads them at pickup (start-mode release) or at terminal paths
# (completion-mode release).
HEADER_UNIQUE_HASH = "x-swarmq-unique-hash"
HEADER_UNIQUE_UNTIL = "x-swarmq-unique-until"

# R4.4 Task Expiry — unix-epoch seconds (int) set by the client at
# publish time; worker compares (now - enqueued_at) to task_info.max_age
# on consume. Expired messages route to DLQ with TASK_EXPIRED signal
# instead of running.
HEADER_ENQUEUED_AT = "x-swarmq-enqueued-at"

# R4.1 Workflow primitives — set on every workflow-step message so the
# worker advancement seam (U5) recognises a step and can advance the
# workflow. WORKFLOW_ID is stable from apply() time (also the get()
# notification channel key); WORKFLOW_STEP is the node's path within the
# persisted topology (e.g. "/0", "/1/h/g2") used to navigate to the next
# step / chord. Absence of WORKFLOW_ID means a normal non-workflow task —
# the advancement seam is a no-op for it.
HEADER_WORKFLOW_ID = "x-swarmq-workflow-id"
HEADER_WORKFLOW_STEP = "x-swarmq-workflow-step"

# Zurueckstellung bei Backend-Ausfall: wie oft ein Gate diese Nachricht
# schon wegen eines Backend-Fehlers ueber die Delay-Queue zurueckgestellt
# hat. Bestimmt die naechste Wartezeit; kein Retry-Zaehler.
HEADER_BACKEND_DEFER_COUNT = "x-swarmq-backend-defer-count"

# Schedfire-Claim-Token: Owner des In-Flight-Claims, den die Zustellung hielt,
# aus der diese Kopie (Zurueckstellung, Retry, Requeue) hervorging. Die Kopie
# bietet es beim Claim zur Uebernahme an -- scheiterte die Freigabe am
# Backend-Ausfall, erkennt sie den Claim als ihren statt als Duplikat.
HEADER_SCHEDFIRE_CLAIM = "x-swarmq-schedfire-claim"

# Zurueckstellungen eines Schedfires mit fremdem Claim (FS-03, E-046): wie oft
# diese Nachricht schon zurueckgestellt wurde, weil ihre Broker-Neuzustellung
# (oder eine schon zurueckgestellte Kopie) einen fremden In-Flight-Claim traf.
# Bestimmt Wartezeit und Deckel; kein Retry-Zaehler. Schon sein Vorhandensein
# (auch mit 0) kennzeichnet eine Neuzustellung. Jede Kopie traegt ihn weiter;
# die Backend-Ausfall-Kopie einer Broker-Neuzustellung bekommt ihn mit 0
# (ohne ihn kaeme sie ohne Broker-Zaehler zurueck und wuerde bei fremdem
# Claim bestaetigt); ``dlq retry`` entfernt ihn.
HEADER_SCHEDFIRE_DEFER_COUNT = "x-swarmq-schedfire-defer-count"

# Wiederholungs-Kennung (Plan 2026-10-08-1541, KTD5): ``dlq retry`` und
# ``dlq retry --all`` setzen sie neu. Die Ausfuehrungs-Identitaet ist Task-ID
# plus diese Kennung -- eine bewusste Wiederholung laeuft erneut, auch wenn
# der Done-Marker der ersten Ausfuehrung noch steht.
HEADER_REPLAY_ID = "x-swarmq-replay-id"

# Zurueckstellungen durch den Ausfuehrungsschutz (Plan 2026-10-08-1541, KTD5):
# wie oft diese Nachricht schon zurueckgestellt wurde, weil dieselbe
# Ausfuehrung anderswo lief. Bestimmt die naechste Wartezeit; kein
# Retry-Zaehler.
HEADER_GUARD_DEFER_COUNT = "x-swarmq-guard-defer-count"

# Retry-Zaehler einer Zustellung: wie oft der Task schon fachlich wiederholt
# wurde (``retry_later`` mit erhoehtem Zaehler). Der Ausfuehrungsschutz
# erkennt daran eine durch einen Retry ueberholte Kopie (KTD5).
HEADER_RETRY_COUNT = "x-swarmq-retry-count"

# Zeitpunkt des Dead-Letterings in Epoch-Millisekunden (Plan 2026-10-08-1541,
# KTD6). ``dlq list``/``inspect`` berechnen daraus die Rest-TTL, mit der sie
# eine gelesene Nachricht zurücklegen -- Lesen verlängert die Frist nicht.
# Millisekunden statt Sekunden: AMQP kodiert Gleitkommazahlen in Header-
# Tabellen nur einfach genau.
HEADER_DEAD_LETTERED_AT = "x-swarmq-dead-lettered-at"

# Frist, mit der eine Nachricht in die DLQ gelegt wurde: Millisekunden oder
# ``"none"`` (unbegrenzt). ``dlq list``/``inspect`` rechnen mit ihr statt mit
# der eigenen Konfiguration -- ein Leser mit kuerzerer Frist loescht sonst
# Nachrichten, die der Worker laenger halten wollte (Review Phase 2).
HEADER_DEAD_LETTER_TTL_MS = "x-swarmq-dead-letter-ttl-ms"

# Erster Kontakt mit einem Task-Namen, den der Worker nicht kennt, in
# Epoch-Millisekunden (Plan 0.12.0, MR 11, BZ-06). Zustands-Header der
# Rueckstellschleife: jede Kopie der Gates traegt ihn weiter (Q6); die
# fachliche Retry-Kopie, der DLQ-Stempel und ``dlq retry`` entfernen ihn,
# damit eine spaetere Unbekanntheit (naechster Rollout) neu gemessen wird.
HEADER_UNKNOWN_SINCE = "x-swarmq-unknown-since"

# Ausfuehrungsidentitaet (Plan 0.12.0, MR 12, BZ-08): Summe der ``x-death``-
# Zaehler ausserhalb der Delay-Queues, die eine Kopie von swarmq mitnimmt.
# Der Broker baut ``x-death`` beim naechsten Dead-Lettern neu auf (Spike S5);
# die Base haelt den Stand fest, damit die Identitaet ``task_id|H#n`` stabil
# bleibt. Fehlt der Header (0.11, UI-Move), gilt Base 0.
HEADER_XDEATH_BASE = "x-swarmq-xdeath-base"

# Generation beim Dead-Lettern durch den Worker (MR 12): der Wert von
# ``x-swarmq-dead-lettered-at`` desselben Stempels. ``dlq retry`` behaelt die
# Replay-ID (Generations-ID) nur, wenn beide gleich sind; hat ein 0.11-Worker
# die Nachricht erneut dead-gelettert, stempelt er ``dead-lettered-at`` neu,
# und die CLI vergibt eine neue ``uuid4``.
HEADER_DLQ_GENERATION = "x-swarmq-dlq-gen"

# Lock-Konflikte warten zeitbasiert (Plan 0.12.0, MR 13, BZ-05). Zustands-
# Header der Rueckstellschleife (Q6): jede Kopie der Gates traegt sie weiter;
# die fachliche Retry-Kopie, der DLQ-Stempel und ``dlq retry`` entfernen sie.
# ``x-swarmq-lock-conflicts`` zaehlt die Konflikte (speist das Signal-kwarg
# ``lock_conflict_count``); der Legacy-Zaehler ``x-swarmq-lock-conflict-count``
# wird von 0.12 nicht mehr erhoeht, weil 0.11-Worker derselben App ab 10 in
# die DLQ schicken. ``lock-wait-since``: erster Konflikt (Epoch-ms, Worker-
# Wanduhr); ``lock-holder``: Fingerprint der Halter beim letzten Wechsel,
# ``lock-holder-since``: Zeitpunkt des Wechsels, ``lock-holder-ttl-ms``: die
# damals beobachtete PTTL des Keys (Basis fuer die Stillstandsgrenze + 60 s).
HEADER_LOCK_CONFLICTS = "x-swarmq-lock-conflicts"
# Legacy-Zaehler aus 0.11 (Deckel 10): 0.12 setzt und liest ihn nicht mehr,
# Kopien tragen ihn unveraendert weiter (Wire-Name bleibt, Golden).
HEADER_LOCK_CONFLICT_COUNT_LEGACY = "x-swarmq-lock-conflict-count"
HEADER_LOCK_WAIT_SINCE = "x-swarmq-lock-wait-since"
HEADER_LOCK_HOLDER = "x-swarmq-lock-holder"
HEADER_LOCK_HOLDER_SINCE = "x-swarmq-lock-holder-since"
HEADER_LOCK_HOLDER_TTL_MS = "x-swarmq-lock-holder-ttl-ms"
# Generation der Wartezeit (Review MR 13, P2-4): Retry-Zaehler und Replay-ID
# der Zustellung, deren Konflikt die Wartezeit begann (``"<retry>:<replay>"``).
# Eine Kopie einer spaeteren Ausfuehrung, die die Zustands-Header noch traegt
# (0.11 entfernt sie weder in der Retry-Kopie noch bei ``dlq retry``), beginnt
# eine neue Wartezeit; fehlt der Header, gilt ``"0:"``.
HEADER_LOCK_WAIT_GEN = "x-swarmq-lock-wait-gen"

# Zustellzaehler, die der Broker je Zustellung fuehrt (Quorum-Queues; Spike S4).
# Keine Kopie, die swarmq publiziert (Retry, Zurueckstellung, DLQ-Stempel,
# ``dlq retry``), traegt sie weiter: eine Kopie ist eine neue Nachricht mit
# eigenem Zustellbudget. RabbitMQ 4 behaelt einen vom Publisher gesetzten
# ``x-delivery-count`` bei der ersten Zustellung bei (3.13 zaehlt zudem
# Requeues) -- mitkopiert hielte das Delivery-Count-Gate die Kopie fuer
# haengend, und ein Replay per UI/Shovel landete sofort wieder in der DLQ
# (FS-10).
BROKER_DELIVERY_COUNTERS = frozenset({"x-delivery-count", "x-acquired-count"})

# Zustands-Header einer Lock-Wartezeit (ohne ``x-swarmq-lock-name``). Eine
# Kopie einer vom Broker neu zugestellten Nachricht traegt sie nicht weiter
# (Review MR 13, P2-4): die Wartezeit endete mit dem Acquire.
LOCK_WAIT_STATE_HEADERS: tuple[str, ...] = (
    HEADER_LOCK_CONFLICTS,
    HEADER_LOCK_WAIT_SINCE,
    HEADER_LOCK_WAIT_GEN,
    HEADER_LOCK_HOLDER,
    HEADER_LOCK_HOLDER_SINCE,
    HEADER_LOCK_HOLDER_TTL_MS,
)

# Zustands-Header aller Rueckstellschleifen (Q6): bleiben auf jeder Kopie,
# die ein Gate vor der Ausfuehrung anlegt; entfernt nur in der fachlichen
# Retry-Kopie, im DLQ-Stempel und durch ``dlq retry``.
LOOP_STATE_HEADERS: tuple[str, ...] = (
    HEADER_UNKNOWN_SINCE,
    HEADER_LOCK_CONFLICTS,
    HEADER_LOCK_WAIT_SINCE,
    HEADER_LOCK_HOLDER,
    HEADER_LOCK_HOLDER_SINCE,
    HEADER_LOCK_HOLDER_TTL_MS,
    HEADER_LOCK_WAIT_GEN,
)
