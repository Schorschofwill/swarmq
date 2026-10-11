"""U1: Signature + chain/group/chord topology, operator syntax, validation.

Pure-logic unit tests — no broker/backend. Tasks self-register via
``__init_subclass__``; the autouse ``_reset_task_registry`` fixture in
``tests/conftest.py`` isolates registrations per test.
"""

from __future__ import annotations

import pytest

from swarmq.exceptions import WorkflowValidationError
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.workflow import (
    DEFAULT_MAX_GROUP_SIZE,
    DEFAULT_MAX_WORKFLOW_DEPTH,
    Chain,
    Chord,
    Group,
    Signature,
    chain,
    chord,
    group,
    sig,
)


class Fetch(Task):
    task_info = TaskInfo(name="fetch", queues=["default"])

    async def run(self, url: str) -> str:
        return f"fetched {url}"


class Parse(Task):
    task_info = TaskInfo(name="parse", queues=["default"])

    async def run(self, data: str) -> str:
        return f"parsed {data}"


class Store(Task):
    task_info = TaskInfo(name="store", queues=["default"])

    async def run(self, data: str) -> str:
        return f"stored {data}"


class Notify(Task):
    task_info = TaskInfo(name="notify", queues=["default"])

    async def run(self, msg: str) -> str:
        return f"notified {msg}"


class FafTask(Task):
    task_info = TaskInfo(name="faf", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET)

    async def run(self, x: int) -> int:
        return x


# --------------------------------------------------------------------------
# Signature creation — R1, R2, R3
# --------------------------------------------------------------------------


def test_task_s_builds_signature_with_bound_args():
    s = Fetch.s("http://x")
    assert isinstance(s, Signature)
    assert s.task_name == "fetch"
    assert s.args == ("http://x",)
    assert s.kwargs == {}
    assert s.immutable is False


def test_sig_builds_equivalent_signature_as_task_s():
    # AE9: Task.s(url) and sig("fetch", url) produce equivalent signatures.
    from_class = Fetch.s("http://x")
    from_name = sig("fetch", "http://x")
    assert from_name.task_name == from_class.task_name
    assert from_name.args == from_class.args
    assert from_name.kwargs == from_class.kwargs
    assert from_name.immutable == from_class.immutable
    assert from_name.to_dict() == from_class.to_dict()


def test_task_si_marks_immutable():
    # R3: immutable signature.
    s = Notify.si("done")
    assert s.immutable is True
    assert s.args == ("done",)


def test_sig_immutable_kwarg():
    s = sig("notify", "done", immutable=True)
    assert s.immutable is True


def test_signature_kwargs_preserved():
    s = sig("fetch", "http://x", retries=3)
    assert s.args == ("http://x",)
    assert s.kwargs == {"retries": 3}


# --------------------------------------------------------------------------
# Primitives — R4, R5, R6
# --------------------------------------------------------------------------


def test_chain_holds_ordered_steps():
    c = chain(Fetch.s("u"), Parse.s(), Store.s())
    assert isinstance(c, Chain)
    assert [step.task_name for step in c.steps] == ["fetch", "parse", "store"]


def test_group_holds_members():
    g = group(Fetch.s("a"), Fetch.s("b"))
    assert isinstance(g, Group)
    assert len(g.members) == 2


def test_group_accepts_generator():
    g = group(Fetch.s(u) for u in ["a", "b", "c"])
    assert len(g.members) == 3


def test_chord_holds_header_and_body():
    ch = chord(group(Fetch.s("a"), Fetch.s("b")), Store.s())
    assert isinstance(ch, Chord)
    assert isinstance(ch.header, Group)
    assert ch.body.task_name == "store"


# --------------------------------------------------------------------------
# Operator syntax — R7, AE10
# --------------------------------------------------------------------------


def test_pipe_operator_builds_chain():
    piped = Fetch.s("u") | Parse.s() | Store.s()
    explicit = chain(Fetch.s("u"), Parse.s(), Store.s())
    assert piped.to_dict() == explicit.to_dict()


def test_group_pipe_body_builds_chord():
    piped = group(Fetch.s("a"), Fetch.s("b")) | Store.s()
    explicit = chord(group(Fetch.s("a"), Fetch.s("b")), Store.s())
    assert piped.to_dict() == explicit.to_dict()


# --------------------------------------------------------------------------
# Flat composition — R8, AE11
# --------------------------------------------------------------------------


def test_chain_may_embed_group():
    c = chain(Fetch.s("u"), group(Parse.s(), Parse.s()), Store.s())
    assert isinstance(c.steps[1], Group)


def test_chain_may_embed_chord():
    c = chain(
        Fetch.s("u"),
        chord(group(Parse.s(), Parse.s()), Store.s()),
        Notify.s("done"),
    )
    assert isinstance(c.steps[1], Chord)


def test_excessive_nesting_rejected_at_construction():
    # Building nesting deeper than DEFAULT_MAX_WORKFLOW_DEPTH must raise —
    # group() validates each level, so the rejection fires as soon as the
    # ceiling is crossed (here, inside the loop), not only at the end.
    def nest_too_deep() -> None:
        node: object = Fetch.s("u")
        for _ in range(DEFAULT_MAX_WORKFLOW_DEPTH + 2):
            node = group(node)  # type: ignore[arg-type]

    with pytest.raises(WorkflowValidationError):
        nest_too_deep()


# --------------------------------------------------------------------------
# Safety limits — R9, AE7
# --------------------------------------------------------------------------


def test_group_over_max_size_rejected():
    members = [Fetch.s(str(i)) for i in range(DEFAULT_MAX_GROUP_SIZE + 1)]
    with pytest.raises(WorkflowValidationError):
        group(*members)


def test_group_at_max_size_allowed():
    members = [Fetch.s(str(i)) for i in range(DEFAULT_MAX_GROUP_SIZE)]
    g = group(*members)
    assert len(g.members) == DEFAULT_MAX_GROUP_SIZE


# --------------------------------------------------------------------------
# FAF rejection — construction-time
# --------------------------------------------------------------------------


def test_faf_signature_rejected_in_chain():
    with pytest.raises(WorkflowValidationError):
        chain(Fetch.s("u"), FafTask.s(1))


def test_faf_signature_rejected_in_group():
    with pytest.raises(WorkflowValidationError):
        group(FafTask.s(1), Fetch.s("u"))


def test_faf_signature_rejected_as_chord_body():
    with pytest.raises(WorkflowValidationError):
        chord(group(Fetch.s("a")), FafTask.s(1))


def test_sig_by_name_not_faf_checked_at_construction():
    # sig() by name cannot resolve delivery mode — no construction-time FAF
    # rejection; the runtime guard (U5) handles it.
    c = chain(sig("fetch", "u"), sig("faf", 1))
    assert len(c.steps) == 2


# --------------------------------------------------------------------------
# to_dict / from_dict roundtrip — R1
# --------------------------------------------------------------------------


def test_chain_roundtrip():
    c = chain(Fetch.s("u"), Parse.s(), Store.s())
    restored = Chain.from_dict(c.to_dict())
    assert restored.to_dict() == c.to_dict()


def test_chord_roundtrip():
    ch = chord(group(Fetch.s("a"), Fetch.s("b")), Store.s())
    restored = Chord.from_dict(ch.to_dict())
    assert restored.to_dict() == ch.to_dict()


def test_immutable_flag_survives_roundtrip():
    c = chain(Fetch.s("u"), Notify.si("done"))
    restored = Chain.from_dict(c.to_dict())
    assert restored.steps[1].immutable is True
