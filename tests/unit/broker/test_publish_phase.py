"""Phase eines gescheiterten Publish aus aiormqs ``basic_publish`` (B24, KTD3).

Das Verhalten ueber den echten aiormq-Code pruefen
``test_publish_connection_loss.py`` und ``test_publish_channel_closing.py``;
hier stehen die Rahmen von ``basic_publish`` als kleine Nachbauten, auch die
Faelle, die der echte Code selten oder gar nicht erzeugt (ein geaendertes
aiormq).

Seit aiormq 7.2 (Plan 2026-10-29-1945, KTD3) sagt das Local ``sent``, ob die
Frames in der ``write_queue`` liegen; ``drain_future`` ist schon vorher
gebunden (``None``). ``drain_future`` sagt weiter, ob der Writer sie
geschrieben hat.

Test-Liste:
- [x] ``basic_publish`` ohne ``sent`` (aiormq geaendert) -> gilt als gesendet
      (unbestimmt) und als eingereiht
- [x] ``sent`` bekannt, aber nicht gebunden (Abbruch in der Warteschlange der
      Kanal-Sperre) -> nicht gesendet, nicht eingereiht (FS-08)
- [x] ``sent = False``, ``drain_future = None`` schon gebunden (Fehler zwischen
      Sperre und ``write_queue.put``) -> nicht gesendet, nicht eingereiht
- [x] ``sent = True``, ``drain_future`` ohne Fehler erfuellt -> gesendet
- [x] ``sent = True``, ``drain_future`` abgebrochen (aiormq bricht ein offenes
      ``drain_future`` im ``except`` ab) -> nicht gesendet, aber eingereiht
- [x] ``sent = True``, ``drain_future`` gescheitert -> nicht gesendet, eingereiht
- [x] ``sent = True`` ohne lesbares ``drain_future`` -> gilt als gesendet
- [x] Ausnahme ausserhalb von ``basic_publish`` -> nicht gesendet, nicht eingereiht
"""

from __future__ import annotations

import asyncio

import pytest

from swarmq.broker.rabbitmq.publish_phase import enqueued_before_failure, sent_before_failure

pytestmark = [pytest.mark.timeout(5)]

# Ein kuenftiges aiormq ohne ``sent``, das die Frames anders schreibt.
_CHANGED_AIORMQ = """
def basic_publish(drain):
    written = object()
    raise ConnectionError("Server connection unexpectedly closed")
"""

# Abbruch in der Warteschlange der Kanal-Sperre: ``sent`` und ``drain_future``
# stehen in ``co_varnames``, sind aber nicht gebunden.
_WAITING_FOR_THE_LOCK = """
def basic_publish(drain):
    raise ConnectionError("cancelled while waiting for the channel lock")
    drain_future = None
    sent = False
"""

# Unter der Sperre, vor ``write_queue.put`` (etwa ein Fehler beim Marshalling).
_BEFORE_THE_PUT = """
def basic_publish(drain):
    drain_future = None
    sent = False
    raise TypeError("marshalling failed")
"""

# Eingereiht; ``drain`` ist der Zustand von ``drain_future`` beim Scheitern.
_AFTER_THE_PUT = """
def basic_publish(drain):
    drain_future = None
    sent = False
    drain_future = drain
    sent = True
    raise ConnectionError("Server connection unexpectedly closed")
"""


def _raised_in(source: str, drain: object = None, module_name: str = "aiormq.channel") -> BaseException:
    namespace: dict[str, object] = {"__name__": module_name}
    exec(source, namespace)
    publish = namespace["basic_publish"]
    assert callable(publish)
    try:
        publish(drain)
    except Exception as exc:
        return exc
    raise AssertionError("basic_publish did not raise")


def test_a_frame_without_sent_counts_as_sent_and_enqueued() -> None:
    """Unbestimmt ist nie falsch; ein erneutes Senden koennte doppelt ausfuehren."""
    exc = _raised_in(_CHANGED_AIORMQ)

    assert sent_before_failure(exc) is True
    assert enqueued_before_failure(exc) is True


def test_an_unbound_sent_counts_as_neither_sent_nor_enqueued() -> None:
    """FS-08: In der Warteschlange der Sperre legt aiormq nichts in die ``write_queue``."""
    exc = _raised_in(_WAITING_FOR_THE_LOCK)

    assert sent_before_failure(exc) is False
    assert enqueued_before_failure(exc) is False


def test_a_failure_between_the_lock_and_the_put_is_neither_sent_nor_enqueued() -> None:
    """KTD3: ``drain_future`` ist schon ``None``, ``sent`` noch falsch."""
    exc = _raised_in(_BEFORE_THE_PUT)

    assert sent_before_failure(exc) is False
    assert enqueued_before_failure(exc) is False


async def test_an_enqueued_frame_whose_drain_completed_counts_as_sent() -> None:
    drain: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    drain.set_result(None)

    exc = _raised_in(_AFTER_THE_PUT, drain)

    assert sent_before_failure(exc) is True
    assert enqueued_before_failure(exc) is True


async def test_an_enqueued_frame_whose_drain_was_cancelled_is_not_sent_but_enqueued() -> None:
    """aiormq 7.2 bricht ein offenes ``drain_future`` im ``except`` ab: Der
    Writer hat den Frame nicht geschrieben, bei offener Verbindung vielleicht
    noch (E-042)."""
    drain: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    drain.cancel()

    exc = _raised_in(_AFTER_THE_PUT, drain)

    assert sent_before_failure(exc) is False
    assert enqueued_before_failure(exc) is True


async def test_an_enqueued_frame_whose_drain_failed_is_not_sent_but_enqueued() -> None:
    drain: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    drain.set_exception(ConnectionError("lost"))
    drain.exception()

    exc = _raised_in(_AFTER_THE_PUT, drain)

    assert sent_before_failure(exc) is False
    assert enqueued_before_failure(exc) is True


def test_an_enqueued_frame_without_a_readable_drain_counts_as_sent() -> None:
    exc = _raised_in(_AFTER_THE_PUT, object())

    assert sent_before_failure(exc) is True


def test_an_exception_outside_basic_publish_is_neither_sent_nor_enqueued() -> None:
    exc = _raised_in(_AFTER_THE_PUT, object(), module_name="aio_pika.exchange")

    assert sent_before_failure(exc) is False
    assert enqueued_before_failure(exc) is False
