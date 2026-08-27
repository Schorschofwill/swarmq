"""Worker-Fassade (Plan U6): Importpfade bleiben, gepatchte Namen werden
nicht re-exportiert (KTD6/KTD14)."""

from __future__ import annotations

import swarmq.worker
import swarmq.worker.pipeline


def test_facade_keeps_identity_of_worker_and_state() -> None:
    assert swarmq.worker.Worker is swarmq.worker.pipeline.Worker
    assert swarmq.worker.WorkerState is swarmq.worker.pipeline.WorkerState


def test_facade_does_not_reexport_patched_names() -> None:
    for name in ("logger", "decode_json", "asyncio", "os", "SchedulerLoop", "_LIFECYCLE_DISPATCH_TIMEOUT_S"):
        assert not hasattr(swarmq.worker, name), name
