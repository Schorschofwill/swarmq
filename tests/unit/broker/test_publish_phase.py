"""Phase eines gescheiterten Publish aus aiormqs ``basic_publish`` (B24).

Das Verhalten ueber den echten aiormq-Code pruefen
``test_publish_connection_loss.py``; hier steht nur der Fall, den der echte
Code nicht erzeugt: ein geaendertes aiormq, dessen ``basic_publish`` kein
lesbares ``drain_future`` hat.

Test-Liste:
- [x] ``basic_publish`` ohne ``drain_future`` -> gilt als gesendet (unbestimmt)
"""

from __future__ import annotations

import pytest

from swarmq.broker.rabbitmq.publish_phase import sent_before_failure

pytestmark = [pytest.mark.timeout(5)]

# Ein kuenftiges aiormq, das die Frames anders schreibt.
_CHANGED_AIORMQ = """
def basic_publish():
    written = object()
    raise ConnectionError("Server connection unexpectedly closed")
"""


def _raised_in(module_name: str, source: str) -> BaseException:
    namespace: dict[str, object] = {"__name__": module_name}
    exec(source, namespace)
    publish = namespace["basic_publish"]
    assert callable(publish)
    try:
        publish()
    except Exception as exc:
        return exc
    raise AssertionError("basic_publish did not raise")


def test_an_unreadable_drain_future_counts_as_sent() -> None:
    """Unbestimmt ist nie falsch; ein erneutes Senden koennte doppelt ausfuehren."""
    exc = _raised_in("aiormq.channel", _CHANGED_AIORMQ)

    assert sent_before_failure(exc) is True
