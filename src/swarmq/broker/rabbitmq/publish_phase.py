"""In welcher Phase ein Publish gescheitert ist (B24, Loadsuite v0.12.0rc1).

``aiormq.Channel.basic_publish`` legt die Frames in die Schreib-Queue der
Verbindung und wartet dann auf zwei Futures: erst auf ``drain_future`` (der
Writer hat die Frames auf den Socket geschrieben), dann auf das
Publisher-Confirm. Reisst die Verbindung, scheitern beide mit derselben
Ausnahme (``FutureStore.reject_all``); am Typ ist die Phase nicht abzulesen.

* ``drain_future`` gescheitert: Die Frames sind nicht vollstaendig gesendet.
  Der Writer schreibt Frame fuer Frame und setzt das Future direkt nach dem
  Schreiben; wartet er auf einen vollen Sendepuffer, steht das Ende dieser
  Frames noch darin. Der Broker kann die Nachricht nicht haben, Wiederholen
  ist sicher.
* ``drain_future`` erfuellt: Die Nachricht ist gesendet, nur das Confirm fehlt.
  Der Broker hat sie vielleicht; der Ausgang ist unbestimmt.

aiormq bietet dafuer keine API. Gelesen wird darum ``drain_future`` aus dem
``basic_publish``-Rahmen im Traceback der Ausnahme (wie ``blocked.py`` liest
das nur, nichts wird veraendert). Ist der Rahmen da, das Future aber nicht
lesbar (aiormq geaendert), gilt der Publish als gesendet: Unbestimmt ist nie
falsch, ein erneutes Senden koennte doppelt ausfuehren.
"""

from __future__ import annotations

import asyncio

_PUBLISH_FUNCTION = "basic_publish"
_DRAIN_LOCAL = "drain_future"


def sent_before_failure(exc: BaseException) -> bool:
    """``True``, wenn aiormq die Frames des gescheiterten Publish schon
    geschrieben hatte; ``False``, wenn die Ausnahme vor dem Schreiben kam
    (auch ausserhalb von ``basic_publish``, etwa ``ChannelInvalidStateError``
    aus aio-pika)."""
    tb = exc.__traceback__
    while tb is not None:
        frame = tb.tb_frame
        if frame.f_code.co_name == _PUBLISH_FUNCTION and str(frame.f_globals.get("__name__", "")).startswith("aiormq"):
            drain = frame.f_locals.get(_DRAIN_LOCAL)
            if not isinstance(drain, asyncio.Future):
                return True
            return drain.done() and not drain.cancelled() and drain.exception() is None
        tb = tb.tb_next
    return False
