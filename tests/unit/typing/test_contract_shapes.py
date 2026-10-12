"""Negative mypy-Fixtures für die Verträge (KTD1, AE5-Vorbereitung).

``@runtime_checkable`` prüft nur Member-Präsenz. Dass eine Implementierung
die *Signaturen* erfüllt, beweist nur mypy strict — diese Tests führen mypy
als Subprozess über kleine Fixture-Module aus und erwarten die Fehler.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def mypy_cache(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("mypy-cache")


def _run_mypy(source: str, tmp_path: Path, cache: Path) -> str:
    fixture = tmp_path / "fixture.py"
    fixture.write_text(textwrap.dedent(source), encoding="utf-8")
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--strict",
            "--no-error-summary",
            "--hide-error-context",
            "--cache-dir",
            str(cache),
            str(fixture),
        ],
        capture_output=True,
        text=True,
        cwd=_REPO,
        env={"MYPYPATH": str(_REPO / "src"), "PATH": "/usr/bin:/bin"},
        check=False,
    )
    return proc.stdout + proc.stderr


_SYNC_ACK = """
    from collections.abc import Mapping
    from typing import Any
    from swarmq.contracts import Consumer, Delivery, DeliveryCallback

    class Impl:
        async def consume(self, queue: str, on_delivery: DeliveryCallback, *, concurrency: int) -> None: ...
        async def cancel_consume(self) -> None: ...
        def ack(self, delivery: Delivery) -> None: ...   # synchron statt async
        async def redeliver(self, delivery: Delivery) -> None: ...
        async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None: ...
        async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None: ...

    _conforms: Consumer = Impl()
"""

_CONFORMING = """
    from collections.abc import Mapping
    from typing import Any
    from swarmq.contracts import Consumer, Delivery, DeliveryCallback

    class Impl:
        async def consume(self, queue: str, on_delivery: DeliveryCallback, *, concurrency: int) -> None: ...
        async def cancel_consume(self) -> None: ...
        async def ack(self, delivery: Delivery) -> None: ...
        async def redeliver(self, delivery: Delivery) -> None: ...
        async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None: ...
        async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None: ...

    _conforms: Consumer = Impl()
"""

_FORGED_RECEIPT = """
    from typing import Any

    from swarmq.workflow_orchestration import WorkflowAdvancer

    def forge(advancer: WorkflowAdvancer, state: dict[str, Any]) -> None:
        class Fake:
            queue = "q"
            message_id = None

        advancer.mark_enqueued(state, "/1", "tid", Fake())
"""


@pytest.mark.timeout(120)
def test_sync_ack_is_rejected_by_mypy_strict(tmp_path: Path, mypy_cache: Path) -> None:
    out = _run_mypy(_SYNC_ACK, tmp_path, mypy_cache)
    assert "error" in out, out
    assert "def ack(" in out, out


@pytest.mark.timeout(120)
def test_conforming_consumer_passes_mypy_strict(tmp_path: Path, mypy_cache: Path) -> None:
    out = _run_mypy(_CONFORMING, tmp_path, mypy_cache)
    assert "error" not in out, out


@pytest.mark.timeout(120)
def test_forged_receipt_is_a_type_error(tmp_path: Path, mypy_cache: Path) -> None:
    """AE5-Vorbereitung: ein selbstgebautes Objekt ist kein ``PublishReceipt``."""
    out = _run_mypy(_FORGED_RECEIPT, tmp_path, mypy_cache)
    assert "error" in out, out
    assert "PublishReceipt" in out, out
