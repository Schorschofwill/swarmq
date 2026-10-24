"""Lädt Skripte außerhalb von ``sys.path`` (``scripts/``, ``benchmarks/``) als Modul."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def lade_skript(name: str, pfad: Path) -> ModuleType:
    """Führt ``pfad`` als Modul ``name`` aus.

    Das Modul steht vor der Ausführung in ``sys.modules``: dataclasses löst
    Annotationen darüber auf.
    """
    spec = importlib.util.spec_from_file_location(name, pfad)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module
