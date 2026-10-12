"""Spike S2 (Plan 2026-10-12-1105, MR 6, P6): erben Kindprozesse einen zur
Laufzeit vorangestellten ``sys.path``-Eintrag?

``swarmq worker --app-dir`` stellt das App-Verzeichnis im Supervisor an
``sys.path[0]``, bevor Supervisor-Kinder (``spawn``) und der Forkserver des
CPU-Modus entstehen. Beide bekommen ``sys.path`` aus
``multiprocessing.spawn.get_preparation_data``. Der Test prueft das im Kind
selbst, weil CPython einen ImportError im Forkserver-Preload still schluckt.

Echte Prozesse (Spawn-Kind, Forkserver samt Aufwaerm-Kind), daher deutlich
ueber 100 ms und hier statt in ``tests/unit`` (AGENTS.md). Der Treiber laeuft
in einem eigenen Interpreter: der Forkserver ist ein Singleton je Prozess
und soll den Testprozess nicht praegen.

Test-Liste:
11. ``spawn``-Kind aus einem Prozess mit vorangestelltem App-Verzeichnis
    importiert ein Modul, das nur dort liegt.
12. Forkserver des CPU-Modus (``start_forkserver`` mit dem Modul im
    Preload): das Kind meldet ``sys.path[0] == app_dir`` und hat das Modul in
    ``sys.modules``, ohne es selbst zu laden.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.timeout(60)]

_DRIVER = """
import json
import multiprocessing
import sys


def spawn_target(mod, q):
    import importlib

    module = importlib.import_module(mod)
    q.put({"path0": sys.path[0], "value": module.VALUE})


def fork_target(mod, q):
    q.put({"path0": sys.path[0], "preloaded": mod in sys.modules})


if __name__ == "__main__":
    app_dir, mod = sys.argv[1], sys.argv[2]
    sys.path.insert(0, app_dir)
    out = {}
    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=spawn_target, args=(mod, q))
    p.start()
    out["spawn"] = q.get(timeout=30)
    p.join(timeout=30)

    from swarmq.worker.compute.forkserver import start_forkserver

    handle = start_forkserver([mod])
    fq = handle.context.Queue()
    fp = handle.context.Process(target=fork_target, args=(mod, fq))
    fp.start()
    out["forkserver"] = fq.get(timeout=30)
    fp.join(timeout=30)
    print(json.dumps(out))
"""


def _run_driver(tmp_path: Path, mod: str) -> tuple[dict[str, dict[str, object]], str]:
    """Treiber starten; liefert seine Ausgabe und das App-Verzeichnis."""
    app_dir = tmp_path / "app"
    app_dir.mkdir()
    (app_dir / f"{mod}.py").write_text("VALUE = 42\n")
    driver_dir = tmp_path / "driver"
    driver_dir.mkdir()
    driver = driver_dir / "drive.py"
    driver.write_text(_DRIVER)
    proc = subprocess.run(
        [sys.executable, str(driver), str(app_dir), mod],
        capture_output=True,
        text=True,
        timeout=50,
        check=False,
        cwd=driver_dir,
    )
    assert proc.returncode == 0, proc.stderr
    result: dict[str, dict[str, object]] = json.loads(proc.stdout.strip().splitlines()[-1])
    return result, str(app_dir)


def test_spawn_child_imports_module_from_prepended_app_dir(tmp_path: Path) -> None:
    result, app_dir = _run_driver(tmp_path, "appmod_s2_spawn")

    assert result["spawn"] == {"path0": app_dir, "value": 42}


def test_forkserver_child_inherits_app_dir_and_preloaded_module(tmp_path: Path) -> None:
    result, app_dir = _run_driver(tmp_path, "appmod_s2_forkserver")

    assert result["forkserver"] == {"path0": app_dir, "preloaded": True}
