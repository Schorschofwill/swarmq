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

The ``__name__`` guard below is still needed: multiprocessing's spawn
start method re-imports the parent's main module in every child as
``__mp_main__``. Without the guard that import ran ``main()`` again and
the child died with "An attempt has been made to start a new process
before the current process has finished its bootstrapping phase".
"""

from swarmq.cli import console_main

if __name__ == "__main__":
    raise SystemExit(console_main())
