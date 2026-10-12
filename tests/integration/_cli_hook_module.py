"""Hook-Modul für ``swarmq worker --configure`` (U5, AE4).

``setup`` registriert eine Middleware auf der CLI-App. Sie protokolliert
jeden ``pre_execute``, damit der Integrationstest am Worker-Ausgabestrom
sieht, dass die Middleware wirklich je Task lief -- und nicht nur, dass der
Hook aufgerufen wurde.
"""

from __future__ import annotations

from typing import Any, override

import structlog

from swarmq.middleware import Middleware

logger = structlog.get_logger()


class CountingMiddleware(Middleware):
    @override
    async def pre_execute(self, message: Any) -> None:
        logger.info("hook_middleware_pre_execute", task_name=message["task_name"])


def setup(app: Any) -> None:
    app.add_middleware(CountingMiddleware())
    logger.info("hook_called")
