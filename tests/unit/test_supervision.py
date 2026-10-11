"""Tests fuer swarmq.supervision — der Worker↔Supervisor-Kanal (R21/R22, KTD10)."""

from __future__ import annotations

import multiprocessing

from swarmq import supervision as sv


def _channel() -> sv.SupervisorChannel:
    return sv.SupervisorChannel.create(multiprocessing.get_context("spawn"))


def test_beat_writes_monotonic_time_and_starts_at_zero():
    ch = _channel()
    assert ch.last_beat() == 0.0
    ch.beat()
    assert ch.last_beat() > 0.0


def test_intent_transitions_and_ack_have_single_writers():
    ch = _channel()
    assert ch.intent_value() == sv.INTENT_NONE
    ch.mark_draining()
    assert ch.intent_value() == sv.INTENT_DRAINING
    ch.request_recycle()
    assert ch.intent_value() == sv.INTENT_RECYCLE
    ch.mark_draining()  # Recycling-Absicht bleibt stehen
    assert ch.intent_value() == sv.INTENT_RECYCLE
    assert not ch.replacement_acknowledged()
    ch.acknowledge_replacement()
    assert ch.replacement_acknowledged()


def test_current_channel_roundtrip():
    ch = _channel()
    sv.set_current_channel(ch)
    try:
        assert sv.current_channel() is ch
    finally:
        sv.set_current_channel(None)
    assert sv.current_channel() is None


def test_locked_slots_are_shared_by_all_channels_of_a_supervisor():
    """BZ-27 (Plan 0.12.0, MR 17): ein Wert je Supervisor, Schreiber ist nur er."""
    ctx = multiprocessing.get_context("spawn")
    shared = ctx.Value("i", 0)
    first = sv.SupervisorChannel.create(ctx, locked_slots=shared)
    second = sv.SupervisorChannel.create(ctx, locked_slots=shared)
    shared.value = 2
    assert first.locked_slots_value() == 2
    assert second.locked_slots_value() == 2


def test_channel_without_shared_locked_slots_reports_none():
    assert _channel().locked_slots_value() is None
