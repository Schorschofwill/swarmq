"""Fixture-Task-Module fuer CLI-Integrationstests im Testprozess registrieren.

Die autouse-Fixture ``_reset_task_registry`` leert die Task-Registry nach jedem
Test; ein zweiter ``import`` desselben Moduls ist aber ein No-op. Damit
``schedule()`` die Klasse trotzdem findet, wird das Modul bei Bedarf neu
geladen — aber nur, wenn der Task nicht mehr registriert ist, sonst meldet die
Registry ein Duplikat.
"""

from __future__ import annotations

import importlib
import sys

from swarmq.task import Task


def ensure_fixture_registered(module_name: str, task_name: str) -> None:
    module = sys.modules.get(module_name)
    if module is None:
        importlib.import_module(module_name)
        return
    registered = {cls.task_info.name for cls in Task._registered_tasks if cls.task_info is not None}  # type: ignore[redundant-expr]  # Basisklassen ohne task_info
    if task_name not in registered:
        importlib.reload(module)
