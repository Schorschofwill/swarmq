"""Root conftest — shared fixtures for all test layers."""

from __future__ import annotations

import contextlib
import gc
import pathlib
import subprocess
from collections.abc import Iterator
from typing import Any

import pytest

from swarmq.task import Task

# ---------------------------------------------------------------------------
# Leaks dem verursachenden Test zuordnen (Plan 2026-10-13-0902, KTD2)
# ---------------------------------------------------------------------------


# Suiten mit echten Diensten und Prozessen; dort entstehen offene Pipes,
# Sockets und Verbindungen. In der Unit-Suite kostet der GC-Lauf je Test ein
# Vielfaches ihrer Laufzeit, deshalb gilt er dort nicht.
_GC_AFTER_TEST_DIRS = frozenset({"integration", "e2e", "chaos", "stress", "soak"})


def _uses_services(nodeid: str) -> bool:
    # Über die Node-ID statt ``Path``: Tests, die os.name umbiegen, machen
    # ``Path`` bis zum Abbau ihrer monkeypatches unbenutzbar.
    parts = nodeid.split("/", 2)
    return len(parts) == 3 and parts[0] == "tests" and parts[1] in _GC_AFTER_TEST_DIRS


def _collect_garbage_after(request: pytest.FixtureRequest) -> Iterator[None]:
    """Nach dem Abbau eines Tests, einer Klasse oder eines Moduls mit Diensten
    den Garbage Collector laufen lassen.

    Unter ``filterwarnings = error`` scheitert ein Test an einer
    ResourceWarning aus ``__del__`` (offene Pipe, Verbindung, Transport).
    Ohne diesen Lauf räumt der GC das Objekt irgendwann später ab, und die
    Warnung trifft einen unbeteiligten Test, je nach Reihenfolge und
    Speicherdruck. Mit ihm scheitert deterministisch der Test, der das Leck
    erzeugt hat.

    Je Ebene eine autouse-Fixture: Sie entsteht vor den Fixtures ihrer Ebene
    (etwa dem klassenweiten Chaos-Stack, an dem Verbindungen hängen) und wird
    nach ihnen abgebaut. Ein Hook ginge nicht: pytest sammelt die
    Unraisable-Warnungen in einem ``trylast``-Hook ein, und ein eigener Hook
    liefe erst danach; seine Funde träfen das Setup des nächsten Tests.
    """
    collect = _uses_services(request.node.nodeid)
    yield
    if collect:
        gc.collect()


_collect_garbage_after_test = pytest.fixture(autouse=True, name="_collect_garbage_after_test")(_collect_garbage_after)
_collect_garbage_after_class = pytest.fixture(autouse=True, scope="class", name="_collect_garbage_after_class")(
    _collect_garbage_after
)
_collect_garbage_after_module = pytest.fixture(autouse=True, scope="module", name="_collect_garbage_after_module")(
    _collect_garbage_after
)


@pytest.fixture(autouse=True)
def _close_subprocess_pipes() -> Iterator[None]:
    """Pipes jedes im Test gestarteten Unterprozesses am Testende schließen.

    Viele Tests starten ``swarmq worker`` mit ``stdout=PIPE``/``stderr=PIPE``,
    lesen die Ausgabe und beenden den Prozess, schließen die Pipe-Objekte aber
    nie. Unter ``filterwarnings = error`` scheitern sie an der
    ResourceWarning aus ``FileIO.__del__``. Die Fixture schließt die Pipes
    zentral, nachdem der Test fertig ist; Prozesse beendet sie nicht, das
    bleibt Sache des Tests.

    Bewusst ohne ``monkeypatch``: die Fixture würde dessen Abbau hinter
    ``_reset_task_registry`` ziehen, und ein Test, der die Registry per
    monkeypatch ersetzt, bekäme danach die alte Liste samt seiner Tasks
    zurück.
    """
    started: list[subprocess.Popen[Any]] = []
    original_init = subprocess.Popen.__init__

    def _tracking_init(self: subprocess.Popen[Any], *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        started.append(self)

    subprocess.Popen.__init__ = _tracking_init  # type: ignore[method-assign]  # Patch nur für diesen Test, unten zurückgesetzt
    try:
        yield
    finally:
        subprocess.Popen.__init__ = original_init  # type: ignore[method-assign]  # Rücknahme des Patches
    for proc in started:
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream is not None:
                with contextlib.suppress(OSError, ValueError):
                    stream.close()


# ---------------------------------------------------------------------------
# Task registry isolation — prevents __init_subclass__ pollution across tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_task_registry():
    """Save and restore Task._registered_tasks between every test."""
    snapshot = Task._registered_tasks.copy()
    yield
    Task._registered_tasks = snapshot


@pytest.fixture(autouse=True)
def _reset_dishka_container():
    """Save and restore the global Dishka container slot between every test.

    Same hazard as Task._registered_tasks: a setup_dishka() call in one test
    would otherwise leak into the next. swarmq.dishka imports without dishka
    installed, so this fixture is always safe to load.
    """
    from swarmq import dishka as _di

    snapshot = _di.get_registered_container()
    yield
    _di._container = snapshot


# ---------------------------------------------------------------------------
# Auto-marker: tag every test with unit / integration / e2e based on its path
# ---------------------------------------------------------------------------
_LAYER_MARKERS: dict[str, str] = {
    "unit": "unit",
    "integration": "integration",
    "e2e": "e2e",
}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Automatically apply layer markers based on test file location."""
    tests_root = pathlib.Path(__file__).resolve().parent

    for item in items:
        item_path = pathlib.Path(item.fspath).resolve()
        try:
            rel = item_path.relative_to(tests_root)
        except ValueError:
            continue

        # The first directory component under tests/ determines the layer.
        parts = rel.parts
        if parts:
            layer = parts[0]
            if layer in _LAYER_MARKERS:
                item.add_marker(getattr(pytest.mark, _LAYER_MARKERS[layer]))
