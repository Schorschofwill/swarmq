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

aiormq bietet dafuer keine API. Gelesen werden darum die Locals des
``basic_publish``-Rahmens im Traceback der Ausnahme (wie ``blocked.py`` liest
das nur, nichts wird veraendert). Seit aiormq 7.2 (Plan 2026-10-29-1945,
KTD3) setzt ``basic_publish`` das Local ``sent``, sobald die Frames in der
``write_queue`` liegen; ``drain_future`` ist schon davor mit ``None``
gebunden. Ist ``sent`` nicht wahr, endete der Publish vor dem Einreihen, etwa
in der Warteschlange der Kanal-Sperre (FS-08, ``sent`` dann noch ungebunden):
"nicht gesendet". Ist ``sent`` wahr, entscheidet ``drain_future`` wie oben;
aiormq bricht ein dabei noch offenes ``drain_future`` ab, ein schon erfuelltes
bleibt erfuellt. Fehlt ``sent`` im Rahmen ganz oder ist ``drain_future`` nicht
lesbar (aiormq geaendert), gilt der Publish als gesendet: Unbestimmt ist nie
falsch, ein erneutes Senden koennte doppelt ausfuehren.

``enqueued_before_failure`` ist die strengere Frage fuer eine noch offene
Verbindung (Publish-Frist, eigener Abbruch): Dort schreibt der Writer
eingereihte Frames womoeglich noch, ein offenes ``drain_future`` heisst also
nicht "nicht gesendet". Nur vor dem Einreihen ist das sicher.
"""

from __future__ import annotations

import asyncio
from types import FrameType

_PUBLISH_FUNCTION = "basic_publish"
_DRAIN_LOCAL = "drain_future"
_SENT_LOCAL = "sent"


def _publish_frame(exc: BaseException) -> FrameType | None:
    """Der Rahmen von aiormqs ``basic_publish`` im Traceback von *exc*."""
    tb = exc.__traceback__
    while tb is not None:
        frame = tb.tb_frame
        if frame.f_code.co_name == _PUBLISH_FUNCTION and str(frame.f_globals.get("__name__", "")).startswith("aiormq"):
            return frame
        tb = tb.tb_next
    return None


def sent_before_failure(exc: BaseException) -> bool:
    """``True``, wenn aiormq die Frames des gescheiterten Publish schon
    geschrieben hatte; ``False``, wenn die Ausnahme vor dem Schreiben kam
    (auch ausserhalb von ``basic_publish``, etwa ``ChannelInvalidStateError``
    aus aio-pika)."""
    frame = _publish_frame(exc)
    if frame is None or not _enqueued(frame):
        return False
    drain = frame.f_locals.get(_DRAIN_LOCAL)
    if not isinstance(drain, asyncio.Future):
        return True
    return drain.done() and not drain.cancelled() and drain.exception() is None


def enqueued_before_failure(exc: BaseException) -> bool:
    """``True``, wenn aiormq die Frames des gescheiterten Publish schon in die
    ``write_queue`` gelegt haben kann; ``False`` nur, wenn *exc* vor dem
    Einreihen kam (Warteschlange der Kanal-Sperre, ausserhalb von
    ``basic_publish``)."""
    frame = _publish_frame(exc)
    return frame is not None and _enqueued(frame)


def _enqueued(frame: FrameType) -> bool:
    """KTD3: ``sent`` wahr heisst "in der ``write_queue``". Ungebunden (Abbruch
    in der Warteschlange der Kanal-Sperre, FS-08) oder falsch heisst "nicht
    eingereiht"; ein Rahmen ganz ohne ``sent`` ist unlesbar und zaehlt als
    eingereiht."""
    if _SENT_LOCAL not in frame.f_code.co_varnames:
        return True
    return bool(frame.f_locals.get(_SENT_LOCAL, False))
