"""Enable ``python -m swarmq.cli`` for the package.

When ``swarmq.cli`` was a single module (``cli.py``), the
``if __name__ == "__main__"`` guard at the bottom of the file handled
``python -m swarmq.cli`` invocations. Packages don't receive
``__name__ == "__main__"`` on their ``__init__.py`` — Python looks for
``<package>/__main__.py`` instead.

Tests and chaos harnesses launch the CLI as a subprocess via
``python -m swarmq.cli worker --module …``:

- tests/integration/test_cli_worker.py (2 call sites)
- tests/e2e/test_dlq_cli.py
- tests/e2e/test_worker_shutdown_load.py
- tests/chaos/test_worker_crash.py
- tests/chaos/test_message_corruption.py

Without this file the invocation fails with
``'swarmq.cli' is a package and cannot be directly executed``.
"""

from swarmq.cli import main

raise SystemExit(main())
