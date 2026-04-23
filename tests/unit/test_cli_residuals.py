"""Tests for ce-review residuals (Plan 2026-04-23-001-fix-cli-refactor-residuals).

Each class corresponds to a Unit in the plan and verifies one ce-review
finding. RED tests live here first; the matching production fix follows
in its own commit.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest


# ---------------------------------------------------------------------------
# Unit 1 — CLI-01: Lazy-Imports (R-1)
# ---------------------------------------------------------------------------


class TestLazyImports:
    """R-1: ``import swarmq.cli`` must not pull in heavy Backend/Broker deps.

    Baseline (pre-fix, measured 2026-04-23):
        swarmq.cli total import time ~152 ms; both ``valkey`` and
        ``aio_pika`` end up in ``sys.modules``.

    Target:
        Median of 10 warm runs >= 60 % below baseline; neither heavy
        dep is imported until first use (via ``cli.__getattr__`` for
        ambient re-exports or via ``_broker_from_env`` /
        ``_backend_from_env`` for internal use).
    """

    def _run_snippet(self, snippet: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-c", textwrap.dedent(snippet)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ},
        )

    def test_import_cli_does_not_import_valkey(self) -> None:
        """Fresh interpreter: ``import swarmq.cli`` must not load ``valkey``."""
        result = self._run_snippet(
            """
            import sys
            import swarmq.cli  # noqa: F401
            assert "valkey" not in sys.modules, sorted(
                m for m in sys.modules if m.startswith("valkey")
            )
            """
        )
        assert result.returncode == 0, (
            f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
        )

    def test_import_cli_does_not_import_aio_pika(self) -> None:
        """Fresh interpreter: ``import swarmq.cli`` must not load ``aio_pika``."""
        result = self._run_snippet(
            """
            import sys
            import swarmq.cli  # noqa: F401
            assert "aio_pika" not in sys.modules, sorted(
                m for m in sys.modules if m.startswith("aio_pika")
            )
            """
        )
        assert result.returncode == 0, (
            f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
        )

    def test_swarmq_help_subprocess_has_no_heavy_imports(self) -> None:
        """``swarmq --help`` (via ``python -m``) stays lightweight.

        The ``--help`` path parses args, emits text, exits. It must not
        touch broker/backend modules. Uses ``python -m swarmq.cli`` so
        this test runs without the installed console script.
        """
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """
                    import sys
                    # Simulate the --help path without actually invoking argparse
                    # help printer (which calls sys.exit). We just import the
                    # facade — the critical path for `swarmq --help` latency.
                    import swarmq.cli  # noqa: F401
                    heavy = [
                        m for m in sys.modules
                        if m.startswith(("valkey", "aio_pika", "libvalkey"))
                    ]
                    assert not heavy, f"heavy deps leaked: {sorted(heavy)}"
                    """
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
        )

    def test_cli_getattr_valkey_backend_loads_lazily(self) -> None:
        """``cli.ValkeyBackend`` stays resolvable via ``__getattr__``.

        Identity contract (R-8): ``swarmq.cli.ValkeyBackend`` must be
        the same object as ``swarmq.backend.valkey.ValkeyBackend`` — the
        surface whitelist's identity check relies on this. But access
        is what triggers the import, not ``import swarmq.cli``.
        """
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """
                    import sys
                    import swarmq.cli as cli
                    # Before first access: valkey must not be loaded.
                    assert "valkey" not in sys.modules, (
                        "valkey loaded too early: "
                        + repr(sorted(m for m in sys.modules if m.startswith('valkey')))
                    )
                    # First access triggers lazy import.
                    vb = cli.ValkeyBackend
                    # Identity check against the canonical module.
                    from swarmq.backend.valkey import ValkeyBackend as CanonicalVB
                    assert vb is CanonicalVB, (vb, CanonicalVB)
                    # And valkey is loaded now.
                    assert "valkey" in sys.modules
                    """
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
        )

    def test_cli_getattr_rabbitmq_broker_loads_lazily(self) -> None:
        """``cli.RabbitMQBroker`` behaves symmetrically to ``ValkeyBackend``."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """
                    import sys
                    import swarmq.cli as cli
                    assert "aio_pika" not in sys.modules
                    rb = cli.RabbitMQBroker
                    from swarmq.broker.rabbitmq import RabbitMQBroker as CanonicalRB
                    assert rb is CanonicalRB, (rb, CanonicalRB)
                    assert "aio_pika" in sys.modules
                    """
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
        )

    def test_cli_getattr_unknown_attribute_raises(self) -> None:
        """``__getattr__`` must still raise ``AttributeError`` for bogus names.

        Regression guard: lazy-loading mechanism must not swallow
        ``AttributeError`` for names outside ``_LAZY_SURFACE``.
        """
        import swarmq.cli as cli

        with pytest.raises(AttributeError, match="no attribute 'ThisDoesNotExist'"):
            _ = cli.ThisDoesNotExist  # type: ignore[attr-defined]
