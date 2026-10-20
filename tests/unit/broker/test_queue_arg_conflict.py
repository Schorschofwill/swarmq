"""Übernahme vorhandener Queue-Argumente nach einem 406 (Patch 0.11.1).

Ab 0.12 legt swarmq Queues mit ``x-dead-letter-strategy``/``x-overflow`` und
eventuell einem anderen ``x-delivery-limit`` an. Ein Rollback auf 0.11.x
scheiterte daran mit ``PRECONDITION_FAILED - inequivalent arg``. 0.11.1
übernimmt für genau diese drei Argumente den vorhandenen Wert; alle anderen
Konflikte bleiben ein Fehler wie in 0.11.0.

Die 406-Texte stammen aus Spike S1 (RabbitMQ 4.2.9 und 4.3.6, identisch):
- ``received the value '<v>' of type '<t>' but current is none``
- ``received none but current is the value '<v>' of type '<t>'``
- ``received '<v>' but current is '<v>'``

Test-Liste:
1. Strategy fehlt in der Deklaration, Queue hat ``at-least-once`` → übernehmen.
2. Overflow fehlt, Queue hat ``reject-publish`` → übernehmen.
3. Delivery-Limit abweichend (``received '20' but current is '40'``) → 40 als int.
4. Delivery-Limit fehlt auf der Queue (``current is none``) → ab 0.12 übernommen
   (Argument entfällt; Plan MR 8: tolerant, Broker-Default 20).
5. (Typ ``short`` → int: abgedeckt in ``test_declare_adoption.py``.)
6. Anderes Argument (``x-queue-type``) → nicht übernehmbar (None).
7. 406 ohne "inequivalent arg" (Queue-Limit) → None.
8. Delivery-Limit mit nicht-ganzzahligem Wert → None.
9. Die übergebenen Argumente werden nicht verändert.

Testlücken Runde 4 (Plan 2026-10-20-1500, U3, Lücke E-17):
10. ``recognized_mismatch`` ohne lesbaren ``received``-Teil -> ``declared`` ist ``None``
    (Mutant ``None if received is None`` -> ``None if received is None and False``).
11. ohne lesbaren ``current``-Teil -> ``existing`` ist ``None``
    (Mutant ``None if current is None`` -> ``None if current is None and False``).
"""

from __future__ import annotations

from swarmq.broker.rabbitmq.arg_conflict import adopt_existing_argument, recognized_mismatch

_Q = "for queue 'tasks' in vhost '/': "
_PREFIX = "PRECONDITION_FAILED - inequivalent arg"

BASE = {
    "x-queue-type": "quorum",
    "x-dead-letter-exchange": "",
    "x-dead-letter-routing-key": "tasks.XQ",
    "x-delivery-limit": 20,
}


def _text(arg: str, rest: str) -> str:
    return f"{_PREFIX} '{arg}' {_Q}{rest}"


def test_adopts_existing_dead_letter_strategy() -> None:
    text = _text("x-dead-letter-strategy", "received none but current is the value 'at-least-once' of type 'longstr'")

    adopted = adopt_existing_argument(text, BASE)

    assert adopted == ("x-dead-letter-strategy", {**BASE, "x-dead-letter-strategy": "at-least-once"})


def test_adopts_existing_overflow() -> None:
    text = _text("x-overflow", "received none but current is the value 'reject-publish' of type 'longstr'")

    adopted = adopt_existing_argument(text, BASE)

    assert adopted == ("x-overflow", {**BASE, "x-overflow": "reject-publish"})


def test_adopts_differing_delivery_limit_as_int() -> None:
    text = _text("x-delivery-limit", "received '20' but current is '40'")

    adopted = adopt_existing_argument(text, BASE)

    assert adopted == ("x-delivery-limit", {**BASE, "x-delivery-limit": 40})


def test_delivery_limit_missing_on_the_queue_is_adopted() -> None:
    """0.12 (Plan MR 8, BZ-15): ``x-delivery-limit`` wird tolerant behandelt,
    auch wenn es auf der Queue fehlt (dann gilt der Broker-Default 20)."""
    text = _text("x-delivery-limit", "received the value '20' of type 'byte' but current is none")

    without_limit = {key: value for key, value in BASE.items() if key != "x-delivery-limit"}
    assert adopt_existing_argument(text, BASE) == ("x-delivery-limit", without_limit)


def test_other_arguments_are_not_adopted() -> None:
    text = _text("x-queue-type", "received 'quorum' but current is 'classic'")

    assert adopt_existing_argument(text, BASE) is None


def test_non_integer_delivery_limit_is_not_adopted() -> None:
    text = _text("x-delivery-limit", "received '20' but current is 'many'")

    assert adopt_existing_argument(text, BASE) is None


def test_precondition_failed_without_argument_conflict_is_not_adopted() -> None:
    text = "PRECONDITION_FAILED - cannot declare queue 'tasks': queue limit in vhost '/' (1) is reached"

    assert adopt_existing_argument(text, BASE) is None


def test_given_arguments_stay_unchanged() -> None:
    arguments = dict(BASE)
    adopt_existing_argument(_text("x-delivery-limit", "received '20' but current is '40'"), arguments)
    adopt_existing_argument(
        _text("x-delivery-limit", "received the value '20' of type 'byte' but current is none"), arguments
    )

    assert arguments == BASE


def test_recognized_mismatch_without_a_readable_received_part_has_no_declared_value() -> None:
    """E-17: Gegenprobe ``arg_conflict.py:79`` ``None if received is None`` ->
    ``None if received is None and False``: rot."""
    mismatch = recognized_mismatch("tasks", _text("x-queue-type", "declared 'quorum' but current is 'classic'"))

    assert mismatch is not None
    assert (mismatch.queue, mismatch.argument, mismatch.declared, mismatch.existing) == (
        "tasks",
        "x-queue-type",
        None,
        "classic",
    )


def test_recognized_mismatch_without_a_readable_current_part_has_no_existing_value() -> None:
    """E-17: Gegenprobe ``arg_conflict.py:80`` ``None if current is None`` ->
    ``None if current is None and False``: rot."""
    mismatch = recognized_mismatch("tasks", _text("x-queue-type", "received 'quorum' but current is unknown"))

    assert mismatch is not None
    assert (mismatch.declared, mismatch.existing) == ("quorum", None)
