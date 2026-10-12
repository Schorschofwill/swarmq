"""Autoskalierung des Supervisors (R4.7) — als freie Funktionen.

Die Entscheidungen (hochskalieren bei Rueckstau, herunterskalieren nach
anhaltender Ruhe) liegen hier statt in ``supervisor.py``: sie lesen den
Supervisor-Zustand, aendern ihn aber nur ueber dessen eigene Methoden. Das
Muster folgt ``worker/recycling.py``, das denselben Schnitt am Worker macht --
inklusive ``pm: Any``: eine Typangabe waere ein Import von ``supervisor`` und
damit ein Zyklus, den der Architektur-Guard zu Recht meldet.
"""

from __future__ import annotations

import time
from typing import Any

from swarmq.logging import _error_fields, get_logger, sanitize_url
from swarmq.process_manager.slots import SlotState

logger = get_logger(__name__)


def tick(pm: Any) -> None:
    """One autoscale decision (no-op outside the sample interval).

    Sampling uses ``_queue_depth_fn`` which is synchronous — the
    caller adapts async broker APIs into a sync wrapper (see
    :func:`make_queue_depth_sampler`).
    """
    # Use explicit checks instead of asserts so
    # the invariant survives python -O deployments. Constructor
    # already enforces these, but a subtle refactor could later
    # violate them.
    if pm._autoscale is None or pm._queue_depth_fn is None:
        return

    now = time.monotonic()
    if now - pm._last_autoscale_sample < pm.AUTOSCALE_SAMPLE_INTERVAL:
        return

    # Check cooldown BEFORE advancing the sample
    # timestamp. Otherwise a cooldown early-return consumes the
    # sample slot, extending the effective dead-time by up to
    # AUTOSCALE_SAMPLE_INTERVAL beyond the documented cooldown.
    if now - pm._last_scale_decision < pm.SCALE_COOLDOWN_S:
        return

    pm._last_autoscale_sample = now

    try:
        depth = pm._queue_depth_fn()
    except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, Autoscale läuft weiter
        # sanitize_url strips embedded AMQP
        # credentials before the exception text reaches the log.
        fields = _error_fields(exc)
        logger.warning(
            "autoscale_sample_failed",
            **fields | {"error": sanitize_url(fields["error"])},
        )
        return

    min_w, max_w = pm.effective_bounds  # Kontrollkanal (KTD12): Sollwert oder Startargumente
    # R2: belegte Slots, ein Slot im Uebergang zaehlt einmal.
    active = pm._slots.occupied()

    # Scale up: enough backlog to justify another worker.
    slot = pm._slots.smallest_free()
    if active < max_w and slot is not None and depth > active * pm.SCALE_UP_THRESHOLD:
        logger.info(
            "autoscale_up",
            depth=depth,
            active=active,
            max_workers=max_w,
            slot=slot,
        )
        pm._start_worker(slot)
        pm._last_scale_decision = now
        pm._idle_since = None
        return

    # Scale down: sustained idle.
    if active > min_w and depth == 0:
        if pm._idle_since is None:
            pm._idle_since = now
            return
        if now - pm._idle_since >= pm.SCALE_DOWN_IDLE_S:
            scale_down_one(pm)
            pm._last_scale_decision = now
            pm._idle_since = None
    else:
        pm._idle_since = None


def scale_down_one(pm: Any) -> None:
    """Tell the youngest eligible worker to drain + exit (SIGTERM).

    Workers younger than
    :attr:`SCALE_MIN_AGE_S` are excluded. Without this, a
    crash → ``_maybe_restart`` → ``_autoscale_tick`` sequence
    would spawn a replacement and immediately kill it, producing
    pointless process churn.
    """
    if not pm.workers:
        return
    now = time.monotonic()
    # KTD1: Slots im Uebergang (Alt- und Ersatzprozess) sind tabu — der
    # Ersatz waere sonst als juengster Prozess der erste Kandidat.
    eligible = {
        pid: w
        for pid, w in pm.workers.items()
        if (now - w.spawned_at) >= pm.SCALE_MIN_AGE_S and pm._slots.state(w.worker_id) is not SlotState.TRANSITION
    }
    if not eligible:
        logger.debug(
            "autoscale_down_deferred_all_young",
            total=len(pm.workers),
            min_age_s=pm.SCALE_MIN_AGE_S,
        )
        return
    # Youngest by spawn time among the eligible (R4) — oldest
    # workers have warmer caches. A low slot may carry the youngest
    # process after a replacement, so the slot number is no proxy;
    # on equal age the highest slot goes (R1).
    target_pid, info = max(
        eligible.items(),
        key=lambda kv: (kv[1].spawned_at, kv[1].worker_id),
    )
    logger.info(
        "autoscale_down",
        worker_id=info.worker_id,
        pid=target_pid,
    )
    info.expected_shutdown = True
    try:
        info.process.terminate()
    except ProcessLookupError:
        # Already gone — bookkeeping will clean up in _check_dead.
        pass
