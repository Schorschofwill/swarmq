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
