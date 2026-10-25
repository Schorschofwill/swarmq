"""Parallelität der Worker im Performance-Vergleich.

Ab Werk führen die Systeme auf einer Maschine mit 24 Kernen sehr verschieden
viele Tasks gleichzeitig aus: swarmq 10 (``--concurrency``), arq 10
(``max_jobs``), Celery 24 (ein Prefork-Prozess je Kern), Dramatiq 192
(24 Prozesse mit je 8 Threads), taskiq 200 (2 Prozesse mit je 100). Ein
Durchsatzvergleich misst dann zum Teil nur diese Zahl. ``gleich`` setzt sie
für alle auf ``GLEICH``, in einem Prozess, wo das System es erlaubt; Celery
kennt im Prefork-Pool keinen Prozess mit mehreren Tasks. ``ab_werk`` lässt die
Werkseinstellung stehen, so wie Nutzer die Systeme betreiben.

Der Garantie-Vergleich (``stoerungen/``) läuft immer ab Werk.
"""

from __future__ import annotations

GLEICH = 10
AB_WERK = "ab_werk"
NAMEN = ("gleich", AB_WERK)

_ARGUMENTE = {
    "swarmq": ["--processes", "1", "--concurrency", str(GLEICH)],
    "celery": ["--concurrency", str(GLEICH)],
    "dramatiq": ["--processes", "1", "--threads", str(GLEICH)],
    "arq": [],
    "taskiq": ["--workers", "1", "--max-async-tasks", str(GLEICH)],
}

# arq hat keinen CLI-Schalter dafür; ``workers/arq_worker.py`` setzt ``max_jobs`` aus der Umgebung.
_UMGEBUNG = {"arq": {"VERGLEICH_MAX_JOBS": str(GLEICH)}}


def argumente(system: str, parallelitaet: str) -> list[str]:
    """Zusätzliche CLI-Argumente des Workers von ``system``."""
    return [] if parallelitaet == AB_WERK else _ARGUMENTE[system]


def umgebung(system: str, parallelitaet: str) -> dict[str, str]:
    """Zusätzliche Umgebungsvariablen des Workers von ``system``."""
    return {} if parallelitaet == AB_WERK else _UMGEBUNG.get(system, {})
