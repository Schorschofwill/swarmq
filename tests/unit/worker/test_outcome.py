"""``DeliveryOutcome`` (Plan U6, KTD5): das typisierte Zustellungs-Ergebnis,
das die beiden Terminal-Flags des Runners ersetzt."""

from __future__ import annotations

from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal


def test_terminal_variants_are_distinct_and_comparable() -> None:
    assert Terminal(True, True) == Terminal(release_unique=True, finalize_schedfire=True)
    assert Terminal(True, True) != Terminal(False, True)
    assert Terminal(False, True) != Terminal(False, False)
    assert len({Terminal(True, True), Terminal(False, True), Terminal(False, False)}) == 3


def test_named_terminal_constructors_match_the_three_variants() -> None:
    assert Terminal.release_in_finally() == Terminal(release_unique=True, finalize_schedfire=True)
    assert Terminal.already_released() == Terminal(release_unique=False, finalize_schedfire=True)
    assert Terminal.never_release() == Terminal(release_unique=False, finalize_schedfire=False)


def test_continue_and_redelivered_are_singletons() -> None:
    assert Continue is Continue
    assert Redelivered is Redelivered
    assert Continue != Redelivered
    assert isinstance(Continue, DeliveryOutcome) and isinstance(Redelivered, DeliveryOutcome)
    assert isinstance(Terminal(True, True), DeliveryOutcome)


def test_outcomes_are_immutable() -> None:
    import pytest

    with pytest.raises((AttributeError, TypeError)):
        Terminal(True, True).release_unique = False  # type: ignore[misc]


def test_release_only_frees_the_flag_without_schedfire_marker() -> None:
    """BZ-22 (Plan 0.12.0, MR 3): Expiry nach erfolgreichem Dead-Letter."""
    assert Terminal.release_only() == Terminal(release_unique=True, finalize_schedfire=False)
