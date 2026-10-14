"""``fail_workflow_step`` (Plan 0.12.0, MR 3, BZ-13).

Seit MR 3 markieren auch Ablauf, Lock-Pfade und der DLX-Fallback den
Workflow als fehlgeschlagen. Ein spaetes Duplikat eines Schritts, der schon
erfolgreich lief (at-least-once, Replay), darf einen weiterlaufenden
Workflow dabei nicht kippen.

Test-Liste:
- [x] Schritt schon erfolgreich erledigt (Erfolgsergebnis gespeichert) ->
      keine Markierung, INFO ``workflow_fail_skipped_step_done``
- [x] gespeichertes Fehlerergebnis -> Markierung
- [x] kein Ergebnis -> Markierung
- [x] Lesen des Ergebnisses scheitert -> Markierung (wie bisher)
- [x] P9 (Plan 2026-10-12-1105, MR 3, T13): ``error_class`` geht nur mit Wert
      als Keyword an ``fail``; ohne Wert bleibt der Aufruf wie bisher
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.cancellation import build_result_dict
from swarmq.exceptions import BackendConnectionError
from swarmq.serialization import encode_result
from swarmq.worker.gates import GateContext
from swarmq.worker.terminal import fail_workflow_step
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = pytest.mark.timeout(5)

_WF_ID = "01890a5d-ac96-774b-bcce-b302099a8057"


def _deps(stored: object) -> MagicMock:
    deps = MagicMock()
    deps.extract_valid_workflow_id = MagicMock(return_value=_WF_ID)
    deps.config.backend_op_timeout = 5.0
    if isinstance(stored, Exception):
        deps.backend.get_result = AsyncMock(side_effect=stored)
    else:
        deps.backend.get_result = AsyncMock(return_value=stored)
    return deps


def _result(*, is_error: bool) -> bytes:
    return encode_result(
        build_result_dict(
            task_id="step-1",
            queue="default",
            task_name="t",
            is_error=is_error,
            is_cancelled=False,
            error_message="boom" if is_error else "",
            result=None if is_error else 42,
        )
    )


async def _fail(deps: MagicMock, advancer: MagicMock) -> bool:
    from swarmq.logging import get_logger

    ctx = GateContext(task_id="step-1", task_name="t", queue="default")
    return await fail_workflow_step(
        deps,
        advancer,
        MagicMock(headers={}),
        ctx,
        "task expired",
        redeliver=True,
        log=get_logger("swarmq.worker.retry"),
    )


async def test_step_that_already_succeeded_does_not_fail_its_workflow() -> None:
    advancer = MagicMock(fail=AsyncMock())

    with capture_swarmq_logs() as logs:
        assert await _fail(_deps(_result(is_error=False)), advancer) is True

    advancer.fail.assert_not_awaited()
    [entry] = [e for e in logs if e["event"] == "workflow_fail_skipped_step_done"]
    assert entry["log_level"] == "info"
    assert entry["workflow_id"] == _WF_ID
    assert entry["task_id"] == "step-1"


@pytest.mark.parametrize(
    "stored", [_result(is_error=True), None, BackendConnectionError("valkey down")], ids=["error", "none", "outage"]
)
@pytest.mark.entscheidung("E-040")
async def test_step_without_a_success_result_fails_its_workflow(stored: object) -> None:
    advancer = MagicMock(fail=AsyncMock())

    assert await _fail(_deps(stored), advancer) is True

    advancer.fail.assert_awaited_once_with(_WF_ID, "TaskError", "task expired")


async def test_error_class_reaches_fail_as_keyword_only_with_a_value() -> None:
    from swarmq.logging import get_logger

    advancer = MagicMock(fail=AsyncMock())
    ctx = GateContext(task_id="step-1", task_name="t", queue="default")

    await fail_workflow_step(
        _deps(None),
        advancer,
        MagicMock(headers={}),
        ctx,
        "",
        redeliver=True,
        log=get_logger("swarmq.worker.retry"),
        error_class="builtins.ValueError",
    )

    advancer.fail.assert_awaited_once_with(_WF_ID, "TaskError", "", error_class="builtins.ValueError")
