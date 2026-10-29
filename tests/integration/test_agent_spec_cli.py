"""``python -m swarmq.agent_spec`` erzeugt die Agent-Spezifikation neu.

Der Drift-Waechter in ``tests/unit/test_agent_spec.py`` verweist bei
veralteten Dateien auf genau diesen Aufruf; hier steht, dass er mit
Zielverzeichnis alle Teile und ``llms.txt`` daneben schreibt und die
Pfade ausgibt. Echter Subprozess, deshalb Integration.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from swarmq.agent_spec import _PARTS, _dump, build_llms_txt


def test_module_cli_writes_spec_and_llms_txt_into_the_given_dir(tmp_path: Path) -> None:
    out = tmp_path / "spec"
    proc = subprocess.run(
        [sys.executable, "-m", "swarmq.agent_spec", str(out)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    printed = {Path(line) for line in proc.stdout.splitlines()}
    assert printed == {out / rel for rel in _PARTS} | {out / "llms.txt"}
    for rel, builder in _PARTS.items():
        assert (out / rel).read_text(encoding="utf-8") == _dump(builder())
    assert (out / "llms.txt").read_text(encoding="utf-8") == build_llms_txt()
