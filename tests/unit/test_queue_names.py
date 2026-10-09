"""``swarmq.queue_names`` (Plan 0.12.0, MR 0, Q16): reine Queue-Namen.

Test-Liste:
- [x] ``queue_family(q)``: Haupt-, Delay- und Dead-Letter-Queue
- [x] ``is_delay_queue``: nur die Delay-Queue, nicht Haupt- oder DLQ
- [x] ``is_delay_queue``: ein Queue-Name, der ``DQ`` nur enthaelt, ist keine
- [x] ``delay_queue_for(q, delay_ms)``: heute immer ``{q}.DQ``
"""

from __future__ import annotations

import pytest

from swarmq.queue_names import delay_queue_for, is_delay_queue, queue_family


def test_queue_family_lists_main_delay_and_dead_letter_queue() -> None:
    assert queue_family("orders") == ("orders", "orders.DQ", "orders.XQ")


@pytest.mark.parametrize(
    ("name", "expected"),
    [("orders.DQ", True), ("orders", False), ("orders.XQ", False), ("DQ", False), ("orders.DQ.backup", False)],
)
def test_is_delay_queue(name: str, expected: bool) -> None:
    assert is_delay_queue(name) is expected


@pytest.mark.parametrize("delay_ms", [0, 1, 60_000, 86_400_000])
def test_delay_queue_for_is_the_family_delay_queue(delay_ms: int) -> None:
    assert delay_queue_for("orders", delay_ms) == "orders.DQ"
