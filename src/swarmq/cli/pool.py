"""Pool-Modus der CLI: ``--pool`` aufloesen und validieren, ``--concurrency`` aufloesen (R1, R3, R4)."""

from __future__ import annotations

import argparse
import os

from swarmq.exceptions import ConfigurationError
from swarmq.slots import effective_concurrency, validate_pool_mode

_POOLS = ("io", "cpu")


def resolve_pool(args: argparse.Namespace) -> str:
    """Pool-Modus aus Flag, sonst ``SWARMQ_POOL``, sonst ``io`` — einmal fuer alle Pfade.

    ``SwarmQConfig`` wendet die Umgebung ebenfalls an; damit CLI-Pruefung,
    ``--check``, Supervisor und Worker denselben Modus sehen, entscheidet die
    CLI hier und schreibt das Ergebnis nach ``args.pool`` zurueck.
    """
    pool = getattr(args, "pool", None) or os.environ.get("SWARMQ_POOL", "").strip() or "io"
    if pool not in _POOLS:
        raise ConfigurationError(f"SWARMQ_POOL must be one of {', '.join(_POOLS)}, got {pool!r}")
    args.pool = pool
    return pool


def prepare_pool_args(args: argparse.Namespace) -> None:
    """Setzt ``args.pool`` und ``args.concurrency`` (aufgeloeste Slotzahl) und prueft den Pool-Modus.

    Laeuft vor ``--check`` und vor dem echten Start, damit beide dieselbe
    Entscheidung treffen. ``args.concurrency_source`` traegt die Herkunft
    (``explicit``, ``default``, ``affinity``, ``cgroup``) fuer das Startlog.
    """
    pool = resolve_pool(args)
    validate_pool_mode(pool=pool, autoscale=getattr(args, "autoscale", None))
    # Kontrollkanal (U4, R4): mit Spielraum teilt der die CPUs, nicht die
    # Startprozesszahl -- sonst ueberbucht ein Sollwert am Spielraum die CPUs.
    divisor = getattr(args, "max_processes", None) or int(getattr(args, "processes", 1) or 1)
    decision = effective_concurrency(getattr(args, "concurrency", None), pool, int(divisor))
    args.concurrency = decision.count
    args.concurrency_source = decision.source
