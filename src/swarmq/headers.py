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
