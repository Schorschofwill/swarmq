"""Tests fuer swarmq.worker.compute.envelope (U4, KTD5)."""

from __future__ import annotations

import pickle

from swarmq.exceptions import NoRetry, Retry
from swarmq.worker.compute import envelope as env


def test_success_envelope_keeps_picklable_value():
    e = env.success_envelope("t1", {"a": 1})
    assert e.ok and e.value == {"a": 1} and e.error() is None
    assert pickle.loads(pickle.dumps(e)).value == {"a": 1}


def test_success_envelope_replaces_unpicklable_value_with_placeholder():
    e = env.success_envelope("t1", lambda: None)
    assert isinstance(e.value, env.UnpicklableResult)
    assert "function" in e.value.reason
    pickle.dumps(e)  # der Platzhalter selbst ueberquert die Grenze


def test_swarmq_retry_exceptions_survive_the_boundary_typed():
    for exc in (Retry(delay=5.0), NoRetry("nope")):
        e = pickle.loads(pickle.dumps(env.error_envelope("t1", exc)))
        rebuilt = e.error()
        assert type(rebuilt) is type(exc)
    assert pickle.loads(pickle.dumps(env.error_envelope("t1", Retry(delay=5.0)))).error().delay == 5.0


def test_unpicklable_exception_is_described_and_reconstructed_by_import():
    class Local(ValueError):
        pass

    # lokale Klasse: nicht importierbar -> RemoteTaskError mit remote_type
    e = pickle.loads(pickle.dumps(env.error_envelope("t1", Local("boom"))))
    rebuilt = e.error()
    assert isinstance(rebuilt, env.RemoteTaskError)
    assert rebuilt.remote_type.endswith("Local")
    assert "boom" in str(rebuilt)


def test_module_level_exception_with_unpicklable_state_is_rebuilt_by_type():
    info = env.RemoteExceptionInfo(module="swarmq.exceptions", qualname="NoRetry", message="x", traceback_text="tb")
    rebuilt = env.reconstruct_exception(info)
    assert isinstance(rebuilt, NoRetry)
    assert "remote traceback" in rebuilt.__notes__[0]


def test_reconstruct_non_exception_target_falls_back():
    info = env.RemoteExceptionInfo(module="swarmq.exceptions", qualname="__doc__", message="x", traceback_text="")
    assert isinstance(env.reconstruct_exception(info), env.RemoteTaskError)


def test_compute_job_carries_the_child_logging_settings_across_pickle():
    """Plan 2026-10-06 U5 (R13/KTD7): Rechenkinder richten ihr Logging nach den
    aufgeloesten Einstellungen des Loop-Prozesses ein; die Defaults entsprechen
    dem bisherigen Verhalten (eingebaute Einrichtung)."""
    plain = env.ComputeJob("t1", "task", "q", (), {})
    assert (
        plain.log_config_json,
        plain.configure_logging,
        plain.logger_levels,
        plain.log_queue_size,
    ) == (None, True, (), 10_000)

    job = env.ComputeJob(
        "t1",
        "task",
        "q",
        (),
        {},
        log_level="CRITICAL",
        log_config_json='{"version": 1}',
        configure_logging=False,
        logger_levels=(("aio_pika", "WARNING"),),
        log_queue_size=500,
    )
    assert pickle.loads(pickle.dumps(job)) == job
