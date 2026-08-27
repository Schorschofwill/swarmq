"""Zurücksetzen der structlog-Lazy-Proxy-Caches aller geladenen
``swarmq.*``-Module (Test-Isolation, siehe ``tests/unit/conftest.py``)."""

from __future__ import annotations

import sys

from structlog._config import BoundLoggerLazyProxy


def reset_swarmq_logger_caches(package: str = "swarmq") -> list[str]:
    """Entfernt den ``bind``-Cache jedes structlog-Lazy-Proxys auf
    Modulebene in allen geladenen ``package``-Modulen. Gibt die Namen der
    zurückgesetzten ``modul.attr`` zurück."""
    reset: list[str] = []
    for name, module in list(sys.modules.items()):
        if module is None or not (name == package or name.startswith(package + ".")):
            continue
        for attr, value in list(vars(module).items()):
            if isinstance(value, BoundLoggerLazyProxy) and "bind" in value.__dict__:
                value.__dict__.pop("bind", None)
                reset.append(f"{name}.{attr}")
    return reset
