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
falsch, ein erneutes Senden koennte doppelt ausfuehren. Ausnahme (FS-08): Kennt
der Rahmen ``drain_future``, hat es aber noch nicht gebunden, endete der
Publish vor dem Einreihen der Frames, etwa in der Warteschlange der
Kanal-Sperre; das heisst "nicht gesendet".
"""

from __future__ import annotations

import asyncio
from types import FrameType
from typing import cast

_PUBLISH_FUNCTION = "basic_publish"
_DRAIN_LOCAL = "drain_future"
_CONFIRM_LOCAL = "confirmation"


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
    if frame is None:
        return False
    if _DRAIN_LOCAL in frame.f_code.co_varnames and _DRAIN_LOCAL not in frame.f_locals:
        # FS-08: Abbruch vor ``drain_future = self.create_future()``, etwa in
        # der Warteschlange der Kanal-Sperre. Erst danach legt aiormq die
        # Frames in die ``write_queue``.
        return False
    drain = frame.f_locals.get(_DRAIN_LOCAL)
    if not isinstance(drain, asyncio.Future):
        return True
    return drain.done() and not drain.cancelled() and drain.exception() is None


def collect_orphaned_confirm(exc: BaseException) -> None:
    """Holt die Ausnahme des Confirm-Futures ab, auf das nach *exc* niemand
    mehr wartet.

    aiormq legt das Future vor dem Schreiben an. Endet ``basic_publish``
    schon davor (gescheiterter Drain), lehnt der Verbindungsabriss es spaeter
    oder gleichzeitig ab, und asyncio meldet beim Einsammeln "Future
    exception was never retrieved". Die Ausnahme hat der Publish bereits
    selbst gemeldet; abgeholt wird nur dieses eine Future."""
    frame = _publish_frame(exc)
    if frame is None:
        return
    confirmation = frame.f_locals.get(_CONFIRM_LOCAL)
    if isinstance(confirmation, asyncio.Future):
        future = cast("asyncio.Future[object]", confirmation)
        if future.done():
            _collect(future)
        else:
            future.add_done_callback(_collect)


def _collect(future: asyncio.Future[object]) -> None:
    if not future.cancelled():
        future.exception()
