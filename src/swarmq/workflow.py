"""Workflow primitives: Signature, chain, group, chord (R1-R9).

This module is the infrastructure-free topology layer. A ``Signature``
captures a bound task call; ``chain`` / ``group`` / ``chord`` compose
signatures (and one level of embedded workflows) into a DAG. Everything
here is pure data + validation — no broker, no backend. The topology
serializes to plain nested dicts (``to_dict`` / ``from_dict``) so it can
be persisted in Valkey and reconstructed on reattach (U4).

Composition is flat-DAG: a chain may embed a group/chord as a step and a
chord body may be a chain/group, but arbitrarily deep recursion is
rejected at construction via ``max_workflow_depth`` (the Celery-canvas
bug class is designed out, not inherited). ``max_group_size`` caps fan-out.
FIRE_AND_FORGET tasks are rejected as steps because they ack before
execution and would never reach the worker advancement seam (U5).
"""

from __future__ import annotations

from typing import Any, Union

from swarmq.exceptions import WorkflowValidationError

DEFAULT_MAX_WORKFLOW_DEPTH = 10
DEFAULT_MAX_GROUP_SIZE = 1000

# A workflow node is any composable element of a topology.
WorkflowNode = Union["Signature", "Chain", "Group", "Chord"]


def _coerce_node(obj: Any) -> WorkflowNode:
    if isinstance(obj, (Signature, Chain, Group, Chord)):
        return obj
    raise WorkflowValidationError(f"expected a Signature/chain/group/chord, got {type(obj).__name__}")


def _node_depth(node: WorkflowNode) -> int:
    """Composition depth: a Signature is 1; containers add one level over
    their deepest child. Used to enforce the flat-DAG ceiling."""
    if isinstance(node, Signature):
        return 1
    if isinstance(node, Chain):
        return 1 + max((_node_depth(s) for s in node.steps), default=0)
    if isinstance(node, Group):
        return 1 + max((_node_depth(m) for m in node.members), default=0)
    if isinstance(node, Chord):
        return 1 + max(_node_depth(node.header), _node_depth(node.body))
    raise WorkflowValidationError(f"unknown node type {type(node).__name__}")


def _reject_faf(node: WorkflowNode) -> None:
    """Reject any signature known to be FIRE_AND_FORGET. ``sig()`` by name
    cannot resolve delivery mode, so its hint is None and it is checked at
    runtime (U5) instead."""
    if isinstance(node, Signature):
        if node._is_fire_and_forget:
            raise WorkflowValidationError(
                f"task {node.task_name!r} is FIRE_AND_FORGET and cannot be a "
                "workflow step: it acks before execution and would never "
                "advance the workflow"
            )
    elif isinstance(node, Chain):
        for step in node.steps:
            _reject_faf(step)
    elif isinstance(node, Group):
        for member in node.members:
            _reject_faf(member)
    elif isinstance(node, Chord):
        _reject_faf(node.header)
        _reject_faf(node.body)


def _validate(node: WorkflowNode) -> None:
    depth = _node_depth(node)
    if depth > DEFAULT_MAX_WORKFLOW_DEPTH:
        raise WorkflowValidationError(
            f"workflow nesting depth {depth} exceeds max_workflow_depth "
            f"{DEFAULT_MAX_WORKFLOW_DEPTH}; flatten the topology"
        )
    _reject_faf(node)


class Signature:
    """A bound task call: task name + frozen args/kwargs.

    ``immutable`` (set via ``Task.si()`` / ``sig(..., immutable=True)``)
    suppresses chain result-injection — the predecessor's result is NOT
    prepended to this signature's args (R3).
    """

    __slots__ = ("_is_fire_and_forget", "args", "immutable", "kwargs", "task_name")

    def __init__(
        self,
        task_name: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        immutable: bool = False,
        _is_fire_and_forget: bool | None = None,
    ) -> None:
        self.task_name = task_name
        self.args = tuple(args)
        self.kwargs = dict(kwargs) if kwargs else {}
        self.immutable = immutable
        # None => unknown (built via sig() by name); checked at runtime in U5.
        self._is_fire_and_forget = _is_fire_and_forget

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "signature",
            "task": self.task_name,
            "args": list(self.args),
            "kwargs": self.kwargs,
            "immutable": self.immutable,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Signature:
        return cls(
            data["task"],
            tuple(data.get("args", [])),
            data.get("kwargs", {}),
            immutable=data.get("immutable", False),
        )

    def __or__(self, other: WorkflowNode) -> Chain:
        return chain(self, other)

    def __repr__(self) -> str:
        flag = "si" if self.immutable else "s"
        return f"Signature({self.task_name!r}.{flag}{self.args!r})"


class Chain:
    """Sequential composition; each step's result feeds the next (R4)."""

    __slots__ = ("steps",)

    def __init__(self, steps: list[WorkflowNode]) -> None:
        self.steps = steps

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "chain", "steps": [s.to_dict() for s in self.steps]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Chain:
        node = cls([_node_from_dict(s) for s in data["steps"]])
        _validate(node)
        return node

    def __or__(self, other: WorkflowNode) -> Chain:
        return chain(*self.steps, other)

    def __repr__(self) -> str:
        return f"Chain({self.steps!r})"


class Group:
    """Parallel composition; result is the ordered list of member results
    in definition order, not completion order (R5).

    A member may itself be a ``Chain``, ``Group`` or ``Chord``. Its entry in
    the result list is then whatever that member produces as a whole — a
    chain's last step, a nested group's own list, a chord's body. The list
    always has exactly ``len(members)`` entries; a shorter one is an error,
    never a result. (In 0.2.0 and 0.3.0 a composite member was silently
    dropped from the list — see the changelog.)
    """

    __slots__ = ("members",)

    def __init__(self, members: list[WorkflowNode]) -> None:
        self.members = members

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "group", "members": [m.to_dict() for m in self.members]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Group:
        node = cls([_node_from_dict(m) for m in data["members"]])
        _validate(node)
        return node

    def __or__(self, body: WorkflowNode) -> Chord:
        return chord(self, body)

    def __repr__(self) -> str:
        return f"Group({self.members!r})"


class Chord:
    """Fan-out + fan-in: the body runs exactly once after every header
    member succeeds, receiving the ordered list of header results (R6)."""

    __slots__ = ("body", "header")

    def __init__(self, header: Group, body: WorkflowNode) -> None:
        self.header = header
        self.body = body

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "chord",
            "header": self.header.to_dict(),
            "body": self.body.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Chord:
        header = Group.from_dict(data["header"])
        node = cls(header, _node_from_dict(data["body"]))
        _validate(node)
        return node

    def __repr__(self) -> str:
        return f"Chord(header={self.header!r}, body={self.body!r})"


def _node_from_dict(data: dict[str, Any]) -> WorkflowNode:
    kind = data.get("kind")
    if kind == "signature":
        return Signature.from_dict(data)
    if kind == "chain":
        return Chain.from_dict(data)
    if kind == "group":
        return Group.from_dict(data)
    if kind == "chord":
        return Chord.from_dict(data)
    raise WorkflowValidationError(f"unknown workflow node kind {kind!r}")


# --------------------------------------------------------------------------
# Public constructors
# --------------------------------------------------------------------------


def sig(
    task_name: str,
    *args: Any,
    immutable: bool = False,
    **kwargs: Any,
) -> Signature:
    """Build a Signature by task name (import-free, cross-service). The
    delivery mode is unknown here, so FAF rejection is deferred to runtime
    (U5). For type-safe construction use ``Task.s()`` / ``Task.si()``."""
    return Signature(task_name, args, kwargs, immutable=immutable)


def chain(*nodes: Any) -> Chain:
    if not nodes:
        raise WorkflowValidationError("chain requires at least one step")
    steps = [_coerce_node(n) for n in nodes]
    c = Chain(steps)
    _validate(c)
    return c


def group(*members: Any) -> Group:
    # Accept a single iterable/generator as well as varargs.
    if len(members) == 1 and not isinstance(members[0], (Signature, Chain, Group, Chord)):
        candidate = members[0]
        if hasattr(candidate, "__iter__"):
            members = tuple(candidate)
    if not members:
        raise WorkflowValidationError("group requires at least one member")
    if len(members) > DEFAULT_MAX_GROUP_SIZE:
        raise WorkflowValidationError(f"group size {len(members)} exceeds max_group_size {DEFAULT_MAX_GROUP_SIZE}")
    nodes = [_coerce_node(m) for m in members]
    g = Group(nodes)
    _validate(g)
    return g


def chord(header: Any, body: Any) -> Chord:
    if not isinstance(header, Group):
        raise WorkflowValidationError("chord header must be a group(...) of header tasks")
    ch = Chord(header, _coerce_node(body))
    _validate(ch)
    return ch
