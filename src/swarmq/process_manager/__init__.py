"""ProcessManager-Fassade (Paket seit Plan U21). ``swarmq.process_manager``
bleibt der Importpfad; Patches zielen auf die Submodule
(``swarmq.process_manager.supervisor.os`` usw., KTD14)."""

from swarmq.process_manager.check import (
    WorkerConfigCheckResult,
    check_worker_config,
)
from swarmq.process_manager.config import WorkerConfig
from swarmq.process_manager.entry import worker_subprocess_entry
from swarmq.process_manager.run import run_multi_process_worker
from swarmq.process_manager.sampler import SAMPLE_TIMEOUT_S
from swarmq.process_manager.sampler import (  # Re-Export (KTD10)
    _make_queue_depth_sampler as _make_queue_depth_sampler,
)
from swarmq.process_manager.slots import WorkerInfo
from swarmq.process_manager.supervisor import (
    ProcessManager,
    QueueDepthFn,
)

__all__ = [
    "SAMPLE_TIMEOUT_S",
    "ProcessManager",
    "QueueDepthFn",
    "WorkerConfig",
    "WorkerConfigCheckResult",
    "WorkerInfo",
    "check_worker_config",
    "run_multi_process_worker",
    "worker_subprocess_entry",
]
