"""Pool-Modus der CLI: ``--pool`` validieren und ``--concurrency`` aufloesen (R1, R3, R4)."""

from __future__ import annotations

import argparse

from swarmq.slots import effective_concurrency, validate_pool_mode


def prepare_pool_args(args: argparse.Namespace) -> None:
    """Setzt ``args.concurrency`` auf die aufgeloeste Slotzahl und prueft den Pool-Modus.

    Laeuft vor ``--check`` und vor dem echten Start, damit beide dieselbe
    Entscheidung treffen. ``args.concurrency_source`` traegt die Herkunft
    (``explicit``, ``default``, ``affinity``, ``cgroup``) fuer das Startlog.
    """
    pool = getattr(args, "pool", "io") or "io"
    validate_pool_mode(pool=pool, autoscale=getattr(args, "autoscale", None))
    decision = effective_concurrency(
        getattr(args, "concurrency", None),
        pool,
        int(getattr(args, "processes", 1) or 1),
    )
    args.concurrency = decision.count
    args.concurrency_source = decision.source
