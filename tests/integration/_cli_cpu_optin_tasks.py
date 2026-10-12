"""Task-Modul mit Opt-in-Queue beim Import (Plan 2026-10-06-1811, U6, R6).

Erzeugt ``SwarmQ(log_queue=True)`` auf Modulebene und loggt danach INFO auf
``swarmq.*`` -- vor dem Fix startete das im Forkserver den Listener-Thread,
und ``swarmq worker --pool cpu`` brach mit Exit 78 ab. Die App entsteht nur
mit ``CPU_OPTIN_FIXTURE_APP=1`` (gesetzt fuer den Worker), damit der
Testprozess beim Einreihen keinen Opt-in einschaltet.
"""

from __future__ import annotations

import os
from typing import override

from swarmq.logging import get_logger
from swarmq.task import Task, TaskInfo

QUEUE = os.environ.get("CPU_FIXTURE_QUEUE", "cpu-fixture")

if os.environ.get("CPU_OPTIN_FIXTURE_APP") == "1":
    from swarmq.app import SwarmQ
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

    APP = SwarmQ(
        RabbitMQBroker(os.environ["SWARMQ_BROKER_URL"]),
        ValkeyBackend(os.environ["SWARMQ_BACKEND_URL"]),
        log_queue=True,
    )
    get_logger("swarmq.tests.optin_fixture").info("optin_fixture_imported")


class CpuOptinEcho(Task):
    task_info = TaskInfo(name="cpu_optin_echo", queues=[QUEUE], max_retries=0)

    @override
    async def run(self, x: int) -> int:
        return x * 2
