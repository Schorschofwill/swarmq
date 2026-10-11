"""Property tests: WorkflowAdvancer state machine under adversarial order.

Härtung R2 B5: the stateful, race-prone advancement logic (chord fan-in,
result injection, redelivery idempotency) was example-based only. These
properties drive random topologies through the in-memory Driver harness
(tests/unit/test_workflow_advancer.py) with RANDOM completion order and
random at-least-once redeliveries, and pin the invariants:

* the workflow always reaches COMPLETED;
* every step is published exactly once (chord body included — it fires
  exactly once, never zero times, regardless of member order/redelivery);
* the chord body receives the member results in DEFINITION order;
* a chain threads each predecessor result into its successor.

Opt-in under ``-m property``.
"""

from __future__ import annotations

import asyncio

from hypothesis import given
from hypothesis import strategies as st

from swarmq.serialization import decode_result, encode_json
from swarmq.workflow import chain, chord, group
from swarmq.workflow_orchestration import (
    STATUS_COMPLETED,
    build_initial_workflow_state,
    initial_frontier,
)
from tests.unit.test_workflow_advancer import Driver, Echo


async def _seed_enqueued(driver: Driver) -> None:
    """Seed like Driver.seed(), but ENQUEUE the initial frontier as
    published messages instead of advancing it inline — only then does
    ``_drain_random`` control the COMPLETION ORDER of group/chord
    members (Driver.seed() advances them sequentially, which would fix
    the order and gut the property)."""
    state = build_initial_workflow_state(driver.wf_id, driver.workflow.to_dict())
    step_tasks = {}
    frontier = list(initial_frontier(driver.workflow))
    for step_path, _sig in frontier:
        step_tasks[step_path] = f"seed-{step_path}"
    state["step_tasks"] = step_tasks
    await driver.backend.store_workflow_state(driver.wf_id, encode_json(state), 60_000)
    for step_path, sig in frontier:
        tid = step_tasks[step_path]
        body = encode_json(
            {
                "task": {"id": tid, "queue": "default", "name": sig.task_name},
                "args": list(sig.args),
                "kwargs": {},
            }
        )
        driver.published.append(
            ("default", body, {"x-swarmq-workflow-step": step_path}),
        )


async def _drain_random(driver: Driver, data: st.DataObject) -> list[str]:
    """Drain published steps in data-driven random order with random
    redeliveries. Returns the execution order of step paths."""
    order: list[str] = []
    # Hard bound: |published| can only shrink unless the advancer is
    # broken; 10_000 iterations means runaway re-publishing.
    for _ in range(10_000):
        if not driver.published:
            return order
        idx = data.draw(
            st.integers(min_value=0, max_value=len(driver.published) - 1),
            label="next_step",
        )
        _queue, body, headers = driver.published.pop(idx)
        step_path = headers["x-swarmq-workflow-step"]
        order.append(step_path)
        task_id = await driver._run_step(body)
        raw = await driver.backend.get_result(task_id)
        assert raw is not None
        result_value = decode_result(raw)["result"]
        await driver.advancer.advance(
            driver.wf_id,
            step_path,
            task_id,
            result_value,
        )
        if data.draw(st.booleans(), label="redeliver"):
            # At-least-once redelivery of the SAME completion — must be
            # a no-op (publish-then-mark idempotency).
            await driver.advancer.advance(
                driver.wf_id,
                step_path,
                task_id,
                result_value,
            )
    raise AssertionError("advancer kept re-publishing steps (runaway)")


@given(
    n_members=st.integers(min_value=2, max_value=5),
    data=st.data(),
)
def test_chord_body_fires_exactly_once_with_ordered_results(n_members, data):
    async def _run() -> None:
        wf = chord(
            group(*(Echo.s(i) for i in range(n_members))),
            Echo.s(),
        )
        driver = Driver(wf)
        await _seed_enqueued(driver)
        order = await _drain_random(driver, data)

        st_dict = await driver.state()
        assert st_dict["status"] == STATUS_COMPLETED

        # Every published step ran exactly once — the chord body never
        # double-fires and never gets lost.
        assert len(order) == len(set(order)), f"duplicate publish: {order}"

        # Body received member results in DEFINITION order, regardless
        # of the (random) completion order: member i echoed [i].
        final = await driver.final_value()
        assert final == [[[i] for i in range(n_members)]]

    asyncio.run(_run())


@given(
    depth=st.integers(min_value=2, max_value=5),
    data=st.data(),
)
def test_chain_threads_results_under_redelivery(depth, data):
    async def _run() -> None:
        wf = chain(*(Echo.s() for _ in range(depth)))
        driver = Driver(wf)
        await _seed_enqueued(driver)
        order = await _drain_random(driver, data)

        st_dict = await driver.state()
        assert st_dict["status"] == STATUS_COMPLETED
        assert len(order) == len(set(order)), f"duplicate publish: {order}"

        # Echo returns list(args); each chain hop wraps the previous
        # result in one more list level. Seed step yields [] → final
        # nesting depth == number of hops after the seed.
        expected: list = []
        for _ in range(depth - 1):
            expected = [expected]
        assert await driver.final_value() == expected

    asyncio.run(_run())


@given(
    n_members=st.integers(min_value=2, max_value=5),
    data=st.data(),
)
def test_standalone_group_aggregates_in_definition_order(n_members, data):
    async def _run() -> None:
        wf = group(*(Echo.s(i) for i in range(n_members)))
        driver = Driver(wf)
        await _seed_enqueued(driver)
        order = await _drain_random(driver, data)

        st_dict = await driver.state()
        assert st_dict["status"] == STATUS_COMPLETED
        assert len(order) == len(set(order))
        assert await driver.final_value() == [[i] for i in range(n_members)]

    asyncio.run(_run())


@given(
    shapes=st.lists(
        st.sampled_from(["leaf", "chain", "group", "chord", "chain-group", "chord-group"]),
        min_size=2,
        max_size=4,
    ),
    data=st.data(),
)
def test_group_with_composite_members_aggregates_in_definition_order(shapes, data):
    """B10 (Plan 2026-08-30-002, U4): Gruppen mit ZUSAMMENGESETZTEN Gliedern.

    Bis 0.3.0 erzeugte diese Stufe ausschliesslich flache Topologien —
    genau deshalb ist der Defekt zwei Releases lang unentdeckt geblieben.
    Die Generierung mischt jetzt alle vier Knotenarten als Glieder und
    faehrt sie in zufaelliger Abschlussreihenfolge mit zufaelligen
    Wiederholzustellungen.
    """

    async def _run() -> None:
        members = []
        expected = []
        for i, shape in enumerate(shapes):
            if shape == "leaf":
                members.append(Echo.s(i))
                expected.append([i])
            elif shape == "chain":
                members.append(chain(Echo.s(i), Echo.s()))
                expected.append([[i]])
            elif shape == "group":
                members.append(group(Echo.s(i), Echo.s(-i)))
                expected.append([[i], [-i]])
            elif shape == "chord":
                members.append(chord(group(Echo.s(i)), Echo.s()))
                expected.append([[[i]]])
            elif shape == "chain-group":
                # Glied, dessen ABSCHLUSSKNOTEN eine Gruppe hinter einer
                # Kettenebene ist — das Aggregat liegt am Gliedpfad.
                members.append(chain(Echo.s(i), group(Echo.si(i), Echo.si(-i))))
                expected.append([[i], [-i]])
            else:
                # dasselbe hinter einer Chord-Ebene (Body ist eine Gruppe).
                members.append(chord(group(Echo.si(i)), group(Echo.si(-i), Echo.si(i))))
                expected.append([[-i], [i]])

        driver = Driver(group(*members))
        await _seed_enqueued(driver)
        order = await _drain_random(driver, data)

        st_dict = await driver.state()
        assert st_dict["status"] == STATUS_COMPLETED
        # Jeder Schritt genau einmal ausgefuehrt (Wiederholzustellung ist no-op).
        assert len(order) == len(set(order))
        # Definitionsreihenfolge, nicht Abschlussreihenfolge — und vollstaendig.
        assert await driver.final_value() == expected

    asyncio.run(_run())


# --------------------------------------------------------------------------
# Compare-and-Set unter nebenlaeufigen Advancern (Plan 2026-09-08-001, U9)
# --------------------------------------------------------------------------


async def _drain_in_concurrent_batches(driver: Driver, data: st.DataObject) -> list[str]:
    """Wie ``_drain_random``, aber die naechsten Schritte laufen in zufaelligen
    NEBENLAEUFIGEN Buendeln: alle Advancer eines Buendels lesen beim Eintritt
    denselben Schnappschuss (die Verschraenkung „beide lesen, dann schreiben
    beide"), erst dann schreibt einer nach dem anderen -- jeder ausser dem
    ersten verliert sein Compare-and-Set und muss mergen. Nachlesungen
    (Fan-in, Konflikt) sehen das Backend frisch. Wiederholzustellungen laufen
    ausserhalb der Buendel mit frischem Zustand."""
    order: list[str] = []
    real_get = driver.backend.get_workflow_state
    for _ in range(10_000):
        if not driver.published:
            return order
        size = data.draw(st.integers(min_value=1, max_value=len(driver.published)), label="batch")
        batch = []
        for _i in range(size):
            idx = data.draw(st.integers(min_value=0, max_value=len(driver.published) - 1), label="pick")
            batch.append(driver.published.pop(idx))
        snapshot = await real_get(driver.wf_id)
        for _queue, body, headers in batch:
            step_path = headers["x-swarmq-workflow-step"]
            order.append(step_path)
            task_id = await driver._run_step(body)
            raw = await driver.backend.get_result(task_id)
            assert raw is not None
            result_value = decode_result(raw)["result"]
            driver.backend.get_workflow_state = _entry_snapshot(real_get, snapshot)  # type: ignore[method-assign]
            try:
                await driver.advancer.advance(driver.wf_id, step_path, task_id, result_value)
            finally:
                driver.backend.get_workflow_state = real_get  # type: ignore[method-assign]
            if data.draw(st.booleans(), label="redeliver"):
                await driver.advancer.advance(driver.wf_id, step_path, task_id, result_value)
    raise AssertionError("advancer kept re-publishing steps (runaway)")


def _entry_snapshot(real_get, snapshot: bytes | None):
    """``get_workflow_state``-Ersatz: die ERSTE Lesung (Eintritt in ``advance``)
    liefert den veralteten Buendel-Schnappschuss, jede weitere das Backend."""
    pending = True

    async def snapshot_first(wf_id, *args, **kwargs):
        nonlocal pending
        if pending:
            pending = False
            return snapshot
        return await real_get(wf_id, *args, **kwargs)

    return snapshot_first


def _count_cas(driver: Driver) -> dict[str, int]:
    counts = {"calls": 0, "conflicts": 0}
    real_cas = driver.backend.compare_and_store_workflow_state

    async def counting(*args, **kwargs):
        counts["calls"] += 1
        ok = await real_cas(*args, **kwargs)
        if not ok:
            counts["conflicts"] += 1
        return ok

    driver.backend.compare_and_store_workflow_state = counting  # type: ignore[method-assign]
    return counts


@given(
    n_chains=st.integers(min_value=10, max_value=14),
    depth=st.integers(min_value=2, max_value=3),
    as_chord=st.booleans(),
    data=st.data(),
)
def test_concurrent_advancers_never_truncate_group_of_parallel_chains(n_chains, depth, as_chord, data):
    """R22-R24 / AE10: Gruppen (und Chords) aus mindestens zehn parallelen
    Ketten, deren Zweige gleichzeitig enden. Ohne CAS verlor der letzte
    Schreiber eines Buendels die Marker der anderen -- der Guard machte daraus
    einen gescheiterten Workflow. Mit CAS endet jeder Lauf vollstaendig, jeder
    Schritt genau einmal, und kein Advance ueberschreitet die Konfliktgrenze
    (die Ausnahme ``WorkflowStateConflict`` wuerde den Lauf abbrechen)."""

    async def _run() -> None:
        chains = [chain(Echo.s(i), *(Echo.s() for _ in range(depth - 1))) for i in range(n_chains)]
        wf = chord(group(*chains), Echo.s()) if as_chord else group(*chains)
        driver = Driver(wf)
        counts = _count_cas(driver)
        await _seed_enqueued(driver)
        order = await _drain_in_concurrent_batches(driver, data)

        st_dict = await driver.state()
        assert st_dict["status"] == STATUS_COMPLETED
        assert len(order) == len(set(order)), f"duplicate publish: {order}"
        # Jeder Zweig ist n_chains-mal in einer Kettentiefe eingewickelt.
        expected_members = []
        for i in range(n_chains):
            value: list = [i]
            for _ in range(depth - 1):
                value = [value]
            expected_members.append(value)
        expected = [expected_members] if as_chord else expected_members
        assert await driver.final_value() == expected
        # Alle Marker persistiert, keiner verloren.
        assert len(st_dict["step_tasks"]) == n_chains * depth + (1 if as_chord else 0)
        assert counts["conflicts"] < counts["calls"]

    asyncio.run(_run())


@given(
    shapes=st.lists(st.sampled_from(["leaf", "chain", "group", "chord"]), min_size=2, max_size=5),
    data=st.data(),
)
def test_concurrent_advancers_on_mixed_group_members_complete(shapes, data):
    """Zufaellige Verschraenkung auf Gruppen mit gemischten Gliedern (Blatt,
    Kette, Gruppe, Chord): vollstaendig oder terminal, nie gekuerzt."""

    async def _run() -> None:
        members = []
        expected = []
        for i, shape in enumerate(shapes):
            if shape == "leaf":
                members.append(Echo.s(i))
                expected.append([i])
            elif shape == "chain":
                members.append(chain(Echo.s(i), Echo.s()))
                expected.append([[i]])
            elif shape == "group":
                members.append(group(Echo.s(i), Echo.s(-i)))
                expected.append([[i], [-i]])
            else:
                members.append(chord(group(Echo.s(i)), Echo.s()))
                expected.append([[[i]]])
        driver = Driver(group(*members))
        await _seed_enqueued(driver)
        order = await _drain_in_concurrent_batches(driver, data)

        st_dict = await driver.state()
        assert st_dict["status"] == STATUS_COMPLETED
        assert len(order) == len(set(order))
        assert await driver.final_value() == expected

    asyncio.run(_run())
