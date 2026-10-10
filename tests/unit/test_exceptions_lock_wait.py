"""``LockWaitExceeded`` (Plan 0.12.0, MR 13, BZ-05, Q11): Fehlertyp der
DLQ-Kopie, wenn die Wartezeit auf einen Lock endet (Obergrenze
``SWARMQ_LOCK_WAIT_MAX_S`` oder Stillstand des Halters). Wird nie geworfen;
eine eigene Klasse, damit der Fehlerkatalog ihn fuehrt.

Test-Liste:
- [x] Klasse, Export, Name des ``error_type`` im Lock-Gate
"""

from __future__ import annotations


def test_lock_wait_exceeded_names_the_dead_letter_reason_of_the_lock_gate() -> None:
    import swarmq
    from swarmq import LockWaitExceeded, exceptions
    from swarmq.worker.gates.lock import LOCK_WAIT_EXCEEDED_ERROR_TYPE

    assert LockWaitExceeded is exceptions.LockWaitExceeded
    assert issubclass(LockWaitExceeded, exceptions.SwarmQError)
    assert LockWaitExceeded.__name__ == LOCK_WAIT_EXCEEDED_ERROR_TYPE
    assert "LockWaitExceeded" in swarmq.__all__
