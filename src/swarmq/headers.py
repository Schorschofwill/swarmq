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
