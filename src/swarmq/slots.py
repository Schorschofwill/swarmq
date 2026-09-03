"""Pool-Modus und Slot-Voreinstellung fuer den CPU-Modus (R3, R4, R5, KTD7).

Bewusst ein stdlib-only Modul auf Paketebene: CLI und Prozess-Manager
brauchen es vor jedem schweren Import, und ``swarmq.worker`` zieht beim
Import die ganze Pipeline.

Die Voreinstellung ist die Zahl der dem Worker *zugeteilten* CPUs, nicht
die Kernzahl des Hosts: ``os.sched_getaffinity`` liefert die Affinitaet,
``/sys/fs/cgroup/cpu.max`` die Container-Quote. Das Minimum, geteilt durch
die Zahl der Loop-Prozesse, ergibt die Slots je Prozess. Ein 4-CPU-Container
auf einem 24-Kern-Host bekommt so 4 Slots statt 24 Verbindungspools.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

from swarmq.exceptions import ConfigurationError

DEFAULT_IO_CONCURRENCY = 10
_CGROUP_CPU_MAX = Path("/sys/fs/cgroup/cpu.max")


@dataclass(frozen=True, slots=True)
class SlotDecision:
    """Aufgeloeste Slotzahl samt Herkunft fuer das Startlog."""

    count: int
    source: str


def _affinity_count() -> int:
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError, OSError:
        return os.cpu_count() or 1


def _cgroup_quota(path: Path) -> int | None:
    """CPU-Quote aus ``cpu.max`` (``"<quota> <period>"`` oder ``"max <period>"``)."""
    try:
        parts = path.read_text(encoding="ascii").split()
    except OSError, UnicodeDecodeError:
        return None
    if len(parts) != 2 or parts[0] == "max":
        return None
    try:
        quota, period = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    if quota <= 0 or period <= 0:
        return None
    return max(1, math.ceil(quota / period))


def resolve_default_slots(processes: int = 1, *, cgroup_path: Path = _CGROUP_CPU_MAX) -> SlotDecision:
    """Slots je Loop-Prozess: ``min(Affinitaet, cgroup-Quote) // processes``, mindestens 1."""
    affinity = _affinity_count()
    quota = _cgroup_quota(cgroup_path)
    if quota is not None and quota < affinity:
        available, source = quota, "cgroup"
    else:
        available, source = affinity, "affinity"
    divisor = max(1, int(processes))
    count = max(1, available // divisor)
    if divisor > 1:
        source = f"{source}/{divisor}"
    return SlotDecision(count=count, source=source)


def effective_concurrency(concurrency: int | None, pool: str, processes: int = 1) -> SlotDecision:
    """``--concurrency`` aufloesen: explizit gewinnt, sonst Pool-Voreinstellung."""
    if concurrency is not None:
        return SlotDecision(count=int(concurrency), source="explicit")
    if pool == "cpu":
        return resolve_default_slots(processes)
    return SlotDecision(count=DEFAULT_IO_CONCURRENCY, source="default")


def validate_pool_mode(*, pool: str, autoscale: tuple[int, int] | None) -> None:
    """Lehnt ``--pool cpu`` mit ``--autoscale`` und auf Nicht-POSIX ab (R4, R5).

    Der Autoskalierer ist auf IO-Worker zugeschnitten (Schwellwert 50
    wartende Nachrichten je Worker); fuer Rechenslots braucht er eigene
    Schwellwerte und bleibt eine spaetere Stufe. Der Forkserver existiert
    nur auf POSIX.
    """
    if pool != "cpu":
        return
    if autoscale is not None:
        raise ConfigurationError(
            "--pool cpu cannot be combined with --autoscale: autoscaling "
            "thresholds are tuned for IO workers and slot-level autoscaling "
            "is a later stage. Use --processes N with --pool cpu instead."
        )
    if os.name != "posix":
        raise ConfigurationError(
            "--pool cpu requires a POSIX platform with the multiprocessing "
            "'forkserver' start method; it is not available on this platform."
        )
