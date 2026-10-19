"""``fail_workflow_step`` (Plan 0.12.0, MR 3, BZ-13).

Seit MR 3 markieren auch Ablauf, Lock-Pfade und der DLX-Fallback den
Workflow als fehlgeschlagen. Ein spaetes Duplikat eines Schritts, der schon
erfolgreich lief (at-least-once, Replay), darf einen weiterlaufenden
Workflow dabei nicht kippen.

Test-Liste:
- [x] Schritt schon erfolgreich erledigt (Erfolgsergebnis gespeichert) ->
      keine Markierung, INFO ``workflow_fail_skipped_step_done``; seit F4-01
      (U14, KTD8) holt der Schutzfall den Advance mit dem gespeicherten
      Ergebnis nach -> ``StepDone.ACK``, INFO ``workflow_advance_caught_up``
- [x] nachgeholter Advance scheitert voruebergehend unter dem Limit -> neu
      zugestellt (``False``), auch ueber *redeliver_with*
- [x] am Zustelllimit scheitert der letzte Advance voruebergehend ->
      ``StepDone.DEAD_LETTER`` (E-040, Restfall ``running``)
- [x] ``UnroutableMessage`` bzw. unerwarteter Fehler im nachgeholten Advance ->
      Fehlmarkierung mit der Ursache, ``StepDone.ACK``
- [x] Fehlmarkierung danach scheitert voruebergehend -> neu zugestellt bzw.
      am Limit ``StepDone.DEAD_LETTER``
- [x] gespeichertes Fehlerergebnis -> Markierung
- [x] kein Ergebnis -> Markierung
- [x] Lesen des Ergebnisses scheitert -> Markierung (wie bisher)
- [x] P9 (Plan 2026-10-12-1105, MR 3, T13): ``error_class`` geht nur mit Wert
      als Keyword an ``fail``; ohne Wert bleibt der Aufruf wie bisher
- [x] Markierung scheitert voruebergehend und das Neuzustellen scheitert auch ->
      kein Absturz des Aufrufers, ``False``, genau eine ERROR-Zeile
      ``workflow_fail_mark_failed`` (Redeliver best effort)
- [x] 0.13.1 (E-079, Nachtrag E-040): am Zustelllimit scheitert der letzte
      Advance mit einer Fehlerantwort (``BackendResponseError``) ->
      Fehlmarkierung mit dem Fehlertext, ``StepDone.ACK`` (ohne DLQ-Kopie)
- [x] dabei scheitert die Fehlmarkierung -> ``StepDone.DEAD_LETTER`` wie bisher
- [x] Fehlerantwort unter dem Limit -> weiter neu zugestellt (E-079)
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.cancellation import build_result_dict
from swarmq.exceptions import (
    BackendConnectionError,
    BackendResponseError,
    BrokerConnectionError,
    UnroutableMessage,
    WorkflowStateConflict,
)
from swarmq.headers import HEADER_WORKFLOW_STEP
from swarmq.serialization import encode_result
from swarmq.worker.gates import GateContext, StepDone
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


async def _fail(
    deps: MagicMock,
    advancer: MagicMock,
    *,
    redeliver: bool = True,
    redeliver_with: Callable[[BaseException], Awaitable[None]] | None = None,
) -> bool | StepDone:
    from swarmq.logging import get_logger

    ctx = GateContext(task_id="step-1", task_name="t", queue="default")
    return await fail_workflow_step(
        deps,
        advancer,
        MagicMock(headers={HEADER_WORKFLOW_STEP: "/0"}),
        ctx,
        "task expired",
        redeliver=redeliver,
        log=get_logger("swarmq.worker.retry"),
        redeliver_with=redeliver_with,
    )


async def test_step_that_already_succeeded_advances_with_the_stored_result_instead_of_failing() -> None:
    advancer = MagicMock(fail=AsyncMock(), advance=AsyncMock())

    with capture_swarmq_logs() as logs:
        assert await _fail(_deps(_result(is_error=False)), advancer) is StepDone.ACK

    advancer.fail.assert_not_awaited()
    advancer.advance.assert_awaited_once_with(_WF_ID, "/0", "step-1", 42)
    [entry] = [e for e in logs if e["event"] == "workflow_fail_skipped_step_done"]
    assert entry["log_level"] == "info"
    assert entry["workflow_id"] == _WF_ID
    assert entry["task_id"] == "step-1"
    [caught_up] = [e for e in logs if e["event"] == "workflow_advance_caught_up"]
    assert caught_up["log_level"] == "info"
    assert (caught_up["workflow_id"], caught_up["task_id"]) == (_WF_ID, "step-1")


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


@pytest.mark.parametrize(
    "error",
    [BackendConnectionError("valkey down"), WorkflowStateConflict("cas"), BrokerConnectionError("nack")],
    ids=["backend", "cas", "broker"],
)
async def test_transient_catch_up_failure_below_the_limit_redelivers(error: Exception) -> None:
    deps = _deps(_result(is_error=False))
    deps.redeliver_pause = None
    deps.broker.redeliver = AsyncMock()
    advancer = MagicMock(fail=AsyncMock(), advance=AsyncMock(side_effect=error))

    assert await _fail(deps, advancer) is False

    deps.broker.redeliver.assert_awaited_once()
    advancer.fail.assert_not_awaited()


async def test_transient_catch_up_failure_goes_through_redeliver_with() -> None:
    deps = _deps(_result(is_error=False))
    deps.broker.redeliver = AsyncMock()
    error = BackendConnectionError("valkey down")
    advancer = MagicMock(fail=AsyncMock(), advance=AsyncMock(side_effect=error))
    redeliver_with = AsyncMock()

    assert await _fail(deps, advancer, redeliver_with=redeliver_with) is False

    redeliver_with.assert_awaited_once_with(error)
    deps.broker.redeliver.assert_not_awaited()


@pytest.mark.entscheidung("E-040")
async def test_transient_last_advance_at_the_limit_dead_letters_without_failing_the_workflow() -> None:
    """Nachtrag E-040 (F4-01): am Zustelllimit folgt ein letzter Advance;
    scheitert er vorübergehend, bleibt der Workflow ``running`` (Restfall),
    die Zustellung geht als DLQ-Kopie ohne Fehlerergebnis."""
    deps = _deps(_result(is_error=False))
    deps.broker.redeliver = AsyncMock()
    advancer = MagicMock(fail=AsyncMock(), advance=AsyncMock(side_effect=BackendConnectionError("valkey down")))

    assert await _fail(deps, advancer, redeliver=False) is StepDone.DEAD_LETTER

    advancer.advance.assert_awaited_once()
    advancer.fail.assert_not_awaited()
    deps.broker.redeliver.assert_not_awaited()


@pytest.mark.parametrize(
    ("error", "error_type"),
    [(UnroutableMessage("no queue for step /1"), "UnroutableMessage"), (KeyError("topology"), "KeyError")],
    ids=["unroutable", "unexpected"],
)
async def test_permanent_catch_up_failure_fails_the_workflow_with_its_cause(error: Exception, error_type: str) -> None:
    deps = _deps(_result(is_error=False))
    deps.broker.redeliver = AsyncMock()
    advancer = MagicMock(fail=AsyncMock(), advance=AsyncMock(side_effect=error))

    assert await _fail(deps, advancer) is StepDone.ACK

    advancer.fail.assert_awaited_once_with(_WF_ID, error_type, str(error))
    deps.broker.redeliver.assert_not_awaited()


@pytest.mark.parametrize(
    ("redeliver", "expected"), [(True, False), (False, StepDone.DEAD_LETTER)], ids=["below", "limit"]
)
async def test_transient_fail_mark_after_an_unroutable_catch_up(redeliver: bool, expected: object) -> None:
    deps = _deps(_result(is_error=False))
    deps.redeliver_pause = None
    deps.broker.redeliver = AsyncMock()
    advancer = MagicMock(
        fail=AsyncMock(side_effect=BackendConnectionError("valkey down")),
        advance=AsyncMock(side_effect=UnroutableMessage("no queue")),
    )

    assert await _fail(deps, advancer, redeliver=redeliver) is expected

    assert deps.broker.redeliver.await_count == int(redeliver)


async def test_failing_redelivery_after_a_transient_fail_mark_does_not_crash_or_log_twice() -> None:
    """Das Neuzustellen ist best effort: scheitert es auch, endet der Aufrufer
    nicht-terminal (``False``) statt mit der Ausnahme, und die schon geloggte
    Markierungs-Zeile bleibt die einzige."""
    advancer = MagicMock(fail=AsyncMock(side_effect=BackendConnectionError("valkey down")))
    redeliver_with = AsyncMock(side_effect=BrokerConnectionError("publish nacked"))

    with capture_swarmq_logs() as logs:
        assert await _fail(_deps(None), advancer, redeliver_with=redeliver_with) is False

    redeliver_with.assert_awaited_once()
    errors = [e for e in logs if e["log_level"] == "error"]
    assert [e["event"] for e in errors] == ["workflow_fail_mark_failed"]
    assert errors[0]["redelivered"] is True


_WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"


@pytest.mark.entscheidung("E-079")
async def test_error_reply_of_the_last_advance_at_the_limit_fails_the_workflow_and_acks() -> None:
    """0.13.1 (Nachtrag E-079, E-040): eine Fehlerantwort heilt keine
    Wiederholung; am Zustelllimit wird der Workflow mit dem Fehlertext
    fehlmarkiert, die Zustellung ohne DLQ-Kopie bestaetigt."""
    deps = _deps(_result(is_error=False))
    deps.broker.redeliver = AsyncMock()
    advancer = MagicMock(fail=AsyncMock(), advance=AsyncMock(side_effect=BackendResponseError(_WRONGTYPE)))

    assert await _fail(deps, advancer, redeliver=False) is StepDone.ACK

    advancer.fail.assert_awaited_once_with(_WF_ID, "BackendResponseError", _WRONGTYPE)
    deps.broker.redeliver.assert_not_awaited()


@pytest.mark.entscheidung("E-079")
async def test_failing_fail_mark_after_an_error_reply_at_the_limit_dead_letters() -> None:
    """Ist ``sq:wf:{wf}`` selbst kaputt, scheitert auch die Fehlmarkierung:
    dann bleibt es beim Restfall (DLQ-Kopie ohne Fehlerergebnis)."""
    deps = _deps(_result(is_error=False))
    deps.broker.redeliver = AsyncMock()
    advancer = MagicMock(
        fail=AsyncMock(side_effect=BackendResponseError(_WRONGTYPE)),
        advance=AsyncMock(side_effect=BackendResponseError(_WRONGTYPE)),
    )

    assert await _fail(deps, advancer, redeliver=False) is StepDone.DEAD_LETTER

    advancer.fail.assert_awaited_once()
    deps.broker.redeliver.assert_not_awaited()


@pytest.mark.entscheidung("E-079")
async def test_error_reply_of_the_catch_up_below_the_limit_still_redelivers() -> None:
    deps = _deps(_result(is_error=False))
    deps.redeliver_pause = None
    deps.broker.redeliver = AsyncMock()
    advancer = MagicMock(fail=AsyncMock(), advance=AsyncMock(side_effect=BackendResponseError(_WRONGTYPE)))

    assert await _fail(deps, advancer) is False

    deps.broker.redeliver.assert_awaited_once()
    advancer.fail.assert_not_awaited()
