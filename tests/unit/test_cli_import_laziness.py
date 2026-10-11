"""Import-laziness tests (Plan 2026-04-23-002).

Verifies that ``import swarmq`` and ``import swarmq.cli`` do NOT pull
``valkey`` or ``aio_pika`` into ``sys.modules``. R-1a (CLI-local lazy
imports) is already satisfied by Plan 2026-04-23-001; R-1b (global
laziness across the whole package init chain) is what these tests guard.

Each test runs in a fresh subprocess so ``sys.modules`` starts clean —
in-process tests would be polluted by earlier imports anywhere in the
test session.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap


def _modules_with_prefix_after(import_stmt: str, *prefixes: str) -> set[str]:
    """Return the set of sys.modules names matching any of the prefixes
    after running ``import_stmt`` in a fresh subprocess.

    Uses ``startswith`` matching so ``valkey`` catches ``valkey``,
    ``valkey.asyncio``, ``valkey.client``, etc. — the goal of R-1 is
    that NONE of the cascade lands in sys.modules.
    """
    code = textwrap.dedent(f"""
        import sys
        {import_stmt}
        for name in sorted(sys.modules):
            for prefix in {list(prefixes)!r}:
                if name == prefix or name.startswith(prefix + "."):
                    print(name)
                    break
    """)
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    return {line for line in result.stdout.strip().splitlines() if line}


# ---------------------------------------------------------------------------
# Unit 1 — R-1: swarmq package init must not eager-load valkey/aio_pika
# ---------------------------------------------------------------------------


class TestSwarmqPackageInitLaziness:
    """R-1: ``import swarmq`` must not pull valkey/aio_pika into sys.modules.

    Root cause without the fix: ``src/swarmq/__init__.py`` re-exports
    ``ValkeyBackend`` and ``RabbitMQBroker`` for the public API
    (`from swarmq import ValkeyBackend`). The fix uses PEP-562
    ``__getattr__`` so the symbols resolve lazily on first attribute
    access — identity is preserved.
    """

    def test_import_swarmq_does_not_load_valkey(self) -> None:
        leaked = _modules_with_prefix_after("import swarmq", "valkey")
        assert not leaked, (
            f"`import swarmq` leaked {sorted(leaked)} into sys.modules — "
            "swarmq/__init__.py must use PEP-562 __getattr__ for the "
            "ValkeyBackend re-export."
        )

    def test_import_swarmq_does_not_load_aio_pika(self) -> None:
        leaked = _modules_with_prefix_after("import swarmq", "aio_pika")
        assert not leaked, (
            f"`import swarmq` leaked {sorted(leaked)} into sys.modules — "
            "swarmq/__init__.py must use PEP-562 __getattr__ for the "
            "RabbitMQBroker re-export."
        )


# ---------------------------------------------------------------------------
# Unit 1 — R-1: swarmq.cli import must not eager-load valkey/aio_pika
# ---------------------------------------------------------------------------


class TestSwarmqCliLaziness:
    """R-1 (via cli path): ``import swarmq.cli`` must not pull heavy libs.

    This is the headline goal — CLI startup latency. The cli path goes
    through swarmq package init, so this test is satisfied transitively
    by the same swarmq/__init__.py fix.
    """

    def test_import_swarmq_cli_does_not_load_valkey(self) -> None:
        leaked = _modules_with_prefix_after("import swarmq.cli", "valkey")
        assert not leaked, (
            f"`import swarmq.cli` leaked {sorted(leaked)} into sys.modules — "
            "this defeats R-1b. The likely culprit is an eager top-level "
            "import in swarmq/__init__.py or one of its transitively "
            "imported modules (app.py, client.py, worker.py, scheduler.py)."
        )

    def test_import_swarmq_cli_does_not_load_aio_pika(self) -> None:
        leaked = _modules_with_prefix_after("import swarmq.cli", "aio_pika")
        assert not leaked, (
            f"`import swarmq.cli` leaked {sorted(leaked)} into sys.modules — "
            "this defeats R-1b. The likely culprit is an eager top-level "
            "import in swarmq/__init__.py or one of its transitively "
            "imported modules (app.py, client.py, worker.py, scheduler.py)."
        )


# ---------------------------------------------------------------------------
# Unit 1 — R-3: PEP-562 __getattr__ must preserve class identity
# ---------------------------------------------------------------------------


class TestPublicSurfaceIdentity:
    """R-3: ``from swarmq import ValkeyBackend`` must still work and yield
    the same class object as ``from swarmq.backend.valkey import ValkeyBackend``.

    Tests run in-process — the assertion needs both imports live in the
    same interpreter so ``is`` can compare them.
    """

    def test_swarmq_valkey_backend_preserves_identity(self) -> None:
        from swarmq import ValkeyBackend
        from swarmq.backend.valkey import ValkeyBackend as CanonicalVB

        assert ValkeyBackend is CanonicalVB, (
            "swarmq.ValkeyBackend is not the canonical class — the lazy "
            "__getattr__ must return importlib.import_module(...).attr, "
            "not a wrapper or proxy."
        )

    def test_swarmq_rabbitmq_broker_preserves_identity(self) -> None:
        from swarmq import RabbitMQBroker
        from swarmq.broker.rabbitmq import RabbitMQBroker as CanonicalRB

        assert RabbitMQBroker is CanonicalRB

    def test_swarmq_getattr_unknown_attribute_raises(self) -> None:
        """``__getattr__`` must still raise ``AttributeError`` for names
        not in the lazy-surface registry — otherwise typos silently
        return None and confuse callers."""
        import pytest

        import swarmq

        with pytest.raises(AttributeError, match="NotARealSymbol"):
            _ = swarmq.NotARealSymbol  # type: ignore[attr-defined]
