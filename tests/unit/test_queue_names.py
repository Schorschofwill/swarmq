"""``swarmq.queue_names`` (Plan 0.12.0, MR 0, Q16): reine Queue-Namen.

Test-Liste:
- [x] ``queue_family(q)``: Haupt-, Delay- und Dead-Letter-Queue
- [x] ``is_delay_queue``: nur die Delay-Queue, nicht Haupt- oder DLQ
- [x] ``is_delay_queue``: ein Queue-Name, der ``DQ`` nur enthaelt, ist keine
- [x] ``delay_queue_for(q, delay_ms)``: heute immer ``{q}.DQ``
- [x] ``delay_tier_for``: kleinste Stufe >= delay, ueber 1 h ``{q}.DQ`` (BZ-01, MR 10)
- [x] ``delay_tiers``: sieben Stufen von 1 s bis 1 h, benachbarte hoechstens Faktor 4
- [x] ``is_delay_queue``: auch jede Delay-Stufe (Voraussetzung MR 12)
- [x] ``main_queue_of``: Haupt-Queue zu Haupt-, Delay-, Stufen- und Dead-Letter-Queue
- [x] ``is_delay_tier``: nur eine Delay-Stufe, nicht ``{q}.DQ``, Haupt- oder Dead-Letter-Queue (Review MR 10, P2-3)
"""

from __future__ import annotations

import itertools

import pytest

from swarmq.queue_names import (
    DELAY_TIERS,
    delay_queue_for,
    delay_tier_for,
    delay_tiers,
    is_delay_queue,
    is_delay_tier,
    main_queue_of,
    queue_family,
)


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


@pytest.mark.parametrize(
    ("delay_ms", "expected"),
    [
        (0, "orders.DQ.1s"),
        (1_000, "orders.DQ.1s"),
        (1_001, "orders.DQ.4s"),
        (10_000, "orders.DQ.15s"),
        (60_000, "orders.DQ.1m"),
        (900_001, "orders.DQ.1h"),
        (3_600_000, "orders.DQ.1h"),
        (3_600_001, "orders.DQ"),
    ],
)
def test_delay_tier_for_picks_the_smallest_tier_reaching_the_delay(delay_ms: int, expected: str) -> None:
    assert delay_tier_for("orders", delay_ms) == expected


def test_seven_delay_tiers_from_one_second_to_one_hour_at_most_factor_four_apart() -> None:
    assert delay_tiers("orders") == tuple(
        f"orders.DQ.{label}" for label in ("1s", "4s", "15s", "1m", "4m", "15m", "1h")
    )
    limits = [limit for _, limit in DELAY_TIERS]
    assert limits[0] == 1_000 and limits[-1] == 3_600_000
    assert all(upper <= 4 * lower for lower, upper in itertools.pairwise(limits))


@pytest.mark.parametrize("tier", delay_tiers("orders"))
def test_every_delay_tier_is_a_delay_queue(tier: str) -> None:
    assert is_delay_queue(tier) is True


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("orders", "orders"),
        ("orders.DQ", "orders"),
        ("orders.XQ", "orders"),
        ("orders.DQ.15s", "orders"),
        ("orders.high.DQ.1h", "orders.high"),
        ("orders.DQ.backup", "orders.DQ.backup"),
    ],
)
def test_main_queue_of(name: str, expected: str) -> None:
    assert main_queue_of(name) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("orders.DQ.15s", True),
        ("orders.high.DQ.1h", True),
        ("orders.DQ", False),
        ("orders", False),
        ("orders.XQ", False),
        ("orders.DQ.backup", False),
        ("DQ.1s", False),
    ],
)
def test_is_delay_tier(name: str, expected: bool) -> None:
    assert is_delay_tier(name) is expected
