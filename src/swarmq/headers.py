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
