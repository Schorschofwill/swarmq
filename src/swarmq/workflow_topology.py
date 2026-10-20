"""Reine Adressierung und Topologie-Helfer der Workflows (U12, KTD13).

Abgetrennt von ``workflow_orchestration.py``: hier steht, WIE ein Schritt
adressiert und benannt wird (Pfade, deterministische Kennungen, Frontier,
Ergebnis-Injektion) — dort, wie ein Workflow fortgeschaltet wird. Keine
Infrastruktur, keine Zustandsschreibung; alles hier ist rein und synchron.

``workflow_orchestration`` re-exportiert die oeffentlichen Namen, damit
bestehende Importe unveraendert bleiben.
"""

from __future__ import annotations

from typing import Any

from swarmq.exceptions import WorkflowValidationError
from swarmq.workflow import Chain, Chord, Group, Signature, WorkflowNode


def node_at_path(root: WorkflowNode, path: str) -> WorkflowNode:
    """Resolve the node addressed by *path* within *root*. Empty string is
    the root itself. Raises WorkflowValidationError on an invalid path."""
    node: WorkflowNode = root
    for seg in [s for s in path.split("/") if s]:
        if seg.startswith("g") and seg[1:].isdigit():
            idx = int(seg[1:])
            if not isinstance(node, Group):
                raise WorkflowValidationError(f"path segment {seg!r} expects a group")
            node = node.members[idx]
        elif seg == "h":
            if not isinstance(node, Chord):
                raise WorkflowValidationError("path segment 'h' expects a chord")
            node = node.header
        elif seg == "b":
            if not isinstance(node, Chord):
                raise WorkflowValidationError("path segment 'b' expects a chord")
            node = node.body
        elif seg.isdigit():
            if not isinstance(node, Chain):
                raise WorkflowValidationError(f"path segment {seg!r} expects a chain")
            node = node.steps[int(seg)]
        else:
            raise WorkflowValidationError(f"unknown path segment {seg!r}")
    return node


def initial_frontier(root: WorkflowNode) -> list[tuple[str, Signature]]:
    """Leaf signatures to enqueue first, each with its topology path.

    The frontier is the set of steps that can run immediately at apply()
    time: the first step of a chain (recursing into an embedded group/chord),
    every member of a group, and the header of a chord. The chord body and
    later chain steps are enqueued later by the worker advancement seam (U5).
    """
    return _frontier(root, "")


def _frontier(node: WorkflowNode, prefix: str) -> list[tuple[str, Signature]]:
    if isinstance(node, Signature):
        return [(prefix or "", node)]
    if isinstance(node, Chain):
        return _frontier(node.steps[0], f"{prefix}/0")
    if isinstance(node, Group):
        out: list[tuple[str, Signature]] = []
        for i, member in enumerate(node.members):
            out.extend(_frontier(member, f"{prefix}/g{i}"))
        return out
    if isinstance(node, Chord):  # pyright: ignore[reportUnnecessaryIsInstance]  # defensiv fuer ungetypte Aufrufer
        return _frontier(node.header, f"{prefix}/h")
    raise WorkflowValidationError(f"unknown node type {type(node).__name__}")


# ``ChordManager`` lebt seit Plan U18 in ``swarmq.backend.valkey.chords``
# (U15 hat die KTD10-Re-Exports entfernt).


def workflow_step_task_id(wf_id: str, step_path: str) -> str:
    """Deterministic task id for a workflow step, derived from
    ``(workflow_id, step_path)``.

    Determinism is load-bearing for idempotency: if a step is published but
    its ``step_tasks`` marker is lost before persist (crash in the
    publish→mark window), a redelivery re-enqueues the SAME id rather than a
    fresh one. The successor's result key is shared (a double-enqueue is
    absorbed), and crucially a re-enqueued chord/group MEMBER lands the same
    id in the ``chord_complete.lua`` SADD set, so the distinct-member count
    cannot be inflated past N (which would fire the chord body early on
    incomplete results). ``apply()`` and the worker advancer MUST mint step
    ids through this one function so the schemes never diverge.
    """
    return f"{wf_id}#{step_path}"


def aggregate_result_id(wf_id: str, node_path: str) -> str:
    """Deterministische Kennung des Ergebnisses, das ein zusammengesetzter
    Knoten an *node_path* als Ganzes produziert, wenn kein einzelner Schritt
    es besitzt (eine Gruppe: ihr Ergebnis ist die Liste).

    Die eine Definition fuer Schreiber und Leser --
    :meth:`WorkflowAdvancer._materialise` legt darunter ab,
    :meth:`WorkflowAdvancer._terminal_result_id` findet darueber wieder.
    Zwei Formatstrings waeren der stille Weg zurueck in B10.
    """
    return f"{wf_id}:agg:{node_path}"


def split_path(path: str) -> tuple[str, str]:
    """Return ``(parent_path, last_segment)`` for a topology path. The root
    (``""``) has no parent — returns ``("", "")``."""
    if not path:
        return "", ""
    idx = path.rfind("/")
    return path[:idx], path[idx + 1 :]


def inject_result(signature: Signature, result: Any) -> Signature:
    """Return a copy of *signature* with *result* prepended as the first
    positional arg, unless the signature is immutable (R3/R4)."""
    if signature.immutable:
        return signature
    return Signature(
        signature.task_name,
        (result, *signature.args),
        signature.kwargs,
        immutable=signature.immutable,
        _is_fire_and_forget=signature._is_fire_and_forget,
    )


__all__ = [
    "aggregate_result_id",
    "initial_frontier",
    "inject_result",
    "node_at_path",
    "split_path",
    "workflow_step_task_id",
]
