"""Einstieg für ``swarmq worker --module workers.swarmq_cli``.

Legt die Benchmark-Tasks auf der Queue aus ``VERGLEICH_QUEUE`` an, die der
Adapter für diesen Lauf frisch gewählt hat.
"""

from __future__ import annotations

import os

from workers.swarmq_worker import create_task_classes

TASKS = create_task_classes(os.environ["VERGLEICH_QUEUE"])
