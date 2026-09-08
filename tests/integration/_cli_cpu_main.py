"""Per ``python -m tests.integration._cli_cpu_main`` startbares Worker-Hauptmodul (U8, AE13).

Nachbildung eines Anwender-Programms, das seine Tasks importiert und dann
``swarmq.cli.main`` ruft — der Startweg, auf dem B13 auftrat: der Forkserver
laedt dieses Modul unter seinem Namen vor, jedes Rechenkind fuehrte es in
``multiprocessing.spawn.prepare()`` trotzdem erneut aus.
"""

from __future__ import annotations

import sys

from swarmq.cli import main
from tests.integration import _cli_cpu_fixture_tasks as fixture

fixture.MAIN_MODULE_EXECUTIONS += 1

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
