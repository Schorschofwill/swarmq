"""Cron-Termine in der Zeitzone des Crons (Plan 2026-10-08-1541, KTD2).

Reine Rechenfunktion ohne I/O. Alle Aufrufer, die einen naechsten Termin
brauchen (Cron-Sync, Score nach dem Fire, Zaehlung uebersprungener Fires,
CLI ``schedule add``), rechnen hierueber."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, tzinfo
from itertools import islice
from zoneinfo import ZoneInfo

from croniter import croniter

# ``@``-Aliase in Fuenf-Feld-Form, damit das Stundenfeld eingestuft werden
# kann (KTD2).
_ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}


def _zone(timezone: str | None) -> tzinfo:
    """IANA-Zone des Crons; ``None`` heisst UTC."""
    return UTC if timezone is None else ZoneInfo(timezone)


def next_fire_times(
    cron: str,
    start: datetime,
    *,
    timezone: str | None = None,
    count: int = 1,
) -> list[datetime]:
    """Die naechsten *count* Termine von *cron* nach *start*.

    *timezone* ist eine IANA-Zone (``None`` = UTC); die Termine kommen
    zeitzonenbehaftet in dieser Zone zurueck. Gedacht auch fuer Projekte,
    die vor dem Umstellen pruefen wollen, dass ein Ausdruck mit Zeitzone
    dieselben Termine liefert wie der alte UTC-Ausdruck (R20).

    *start* muss zeitzonenbehaftet sein (z. B. ``datetime.now(UTC)``); ein
    naiver Zeitpunkt wirft ``ValueError``, statt stillschweigend als
    Ortszeit des Hosts gelesen zu werden.

    DST-Regel (KTD2): In der Rueckstellungsnacht feuert ein Cron, dessen
    Stundenfeld kein ``*`` enthaelt, nur im ersten Durchlauf der doppelten
    Wanduhrzeit; liefert croniter einen Termin im zweiten Durchlauf, wird
    ab dem Ende der doppelten Phase weitergesucht. Crons mit ``*`` im
    Stundenfeld (auch ``*/n``; ``@``-Aliase nach Uebersetzung) folgen der
    echten Zeit. In der Fruehjahrsluecke gilt das Verhalten von croniter.
    Ungueltige Ausdruecke werfen croniters ``CroniterBadCronError``.
    """
    if start.utcoffset() is None:
        raise ValueError(
            "next_fire_times() needs a timezone-aware start, "
            "e.g. datetime.now(UTC); a naive datetime would be read as "
            "the host's local time"
        )
    return list(islice(_iter_fires(cron, start.astimezone(_zone(timezone))), count))


def iter_fire_ms(cron: str, timezone: str | None, ref_ms: int) -> Iterator[int]:
    """Termine nach *ref_ms* als Unix-ms (Score im Schedule-Index), endlos."""
    start = datetime.fromtimestamp(ref_ms / 1000, tz=_zone(timezone))
    for fire in _iter_fires(cron, start):
        yield int(fire.timestamp() * 1000)


def next_fire_ms(cron: str, timezone: str | None, ref_ms: int) -> int:
    """Naechster Termin nach *ref_ms* als Unix-ms."""
    return next(iter_fire_ms(cron, timezone, ref_ms))


def _iter_fires(cron: str, start: datetime) -> Iterator[datetime]:
    """Termine nach *start* in dessen Zone, mit DST-Regel (KTD2).

    Ein croniter-Iterator traegt die Folge; nur nach einem Termin im
    zweiten Durchlauf wird ab dem Ende der doppelten Phase neu aufgesetzt.
    Der Ausdruck wird von croniter geprueft, bevor das Stundenfeld
    eingestuft wird.
    """
    it = croniter(cron, start)
    fixed_hour = "*" not in _ALIASES.get(cron.lower(), cron).split()[1]
    while True:
        fire = it.get_next(datetime)
        while fixed_hour and _in_second_pass(fire):
            it = croniter(cron, _last_second_of_doubled_phase(fire))
            fire = it.get_next(datetime)
        yield fire


def _in_second_pass(moment: datetime) -> bool:
    """Liegt *moment* im zweiten Durchlauf einer doppelten Wanduhrzeit?"""
    local = datetime.fromtimestamp(moment.timestamp(), moment.tzinfo)
    ambiguous = local.replace(fold=0).utcoffset() != local.replace(fold=1).utcoffset()
    return ambiguous and local.fold == 1


def _last_second_of_doubled_phase(moment: datetime) -> datetime:
    """Letzte Sekunde des zweiten Durchlaufs, in dem *moment* liegt.

    Der Wechsel T liegt in ``(moment - Rueckstellung, moment]``; er wird per
    Bisektion ueber ganze Sekunden gesucht. Der zweite Durchlauf endet bei
    T + Rueckstellung (dort erreicht die Wanduhr wieder den Stand vor T).
    """
    zone = moment.tzinfo
    local = datetime.fromtimestamp(moment.timestamp(), zone)
    later = local.utcoffset()
    earlier = local.replace(fold=0).utcoffset()
    assert later is not None
    assert earlier is not None
    shift = int((earlier - later).total_seconds())
    lo = int(local.timestamp()) - shift  # noch alter Offset
    hi = int(local.timestamp())  # schon neuer Offset
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if datetime.fromtimestamp(mid, zone).utcoffset() == later:
            hi = mid
        else:
            lo = mid
    return datetime.fromtimestamp(hi + shift - 1, zone)
