"""Fuzz: Cron-Prüfung und Cron-Termine (Fehlersuche Runde 4, U16, R20, KTD8).

Ziele: die Cron-Prüfung in ``TaskInfo.__post_init__`` (``swarmq.task``,
E-065) und ``scheduler.cron_time.next_fire_times`` (Leader, Sync, CLI).
croniter ist auf 6.2.4 gepinnt (``uv.lock``).

Zusagen:

* E-065: Ein Cron ist ein Standard-Ausdruck mit 5 Feldern nach crontab(5)
  (Positivliste, Nachtrag 2026-10-17); ``L``, ``W``, ``#``, ``R``, ``H``,
  ``?`` und 6/7 Felder werden abgelehnt; ein Ausdruck ohne nächsten Termin
  (``0 0 30 2 *``) ist ein ``ValueError`` ("never fires"). Jeder falsche
  Wert ist ein ``ValueError`` beim Klassenbau.
* Standard-Ausdrücke nach crontab(5) (Zahlen, ``*``, Bereiche, Schritte,
  Listen, Namen für Monat und Wochentag, "case does not matter") werden
  angenommen (website/reference/configuration.md:471 "Standard 5-field cron
  expression").
* ``next_fire_times``: für einen angenommenen Ausdruck streng monoton
  steigende Termine nach dem Start, gleich bei jedem Aufruf (der Leader
  rechnet den nächsten Termin nach jedem Fire neu, ``iter_fire_ms``);
  KTD2 (Docstring): in der Rückstellungsnacht feuert ein Cron mit festem
  Stundenfeld nur im ersten Durchlauf der doppelten Wanduhrzeit.

Allow-List: ``ValueError`` (samt croniter-Fehlern, die davon erben) für
``TaskInfo``; für ``next_fire_times`` mit angenommenem Ausdruck keine.

Treffer (Bericht docs/befunde/fehlersuche-runde-4-recherche/5-fuzz.md; IDs aus
dem Register ``docs/befunde/fehlersuche-runde-4-2026-10.md``), behoben mit
der Positivliste ``swarmq.cron_syntax`` (Fix-Plan 0.13, U15); die Klammern
sind aufgelöst:

* PF-12 / R4F-05: croniters Zufallsfeld ``R`` wurde angenommen; ``R`` ist
  wieder im Alphabet.
* F4-09 / R4F-06: Namen in Großschreibung mit ``L``/``W`` (``JUL``, ``WED``,
  ``@HOURLY``) galten als erweiterte Syntax; Namen kommen in jeder
  Schreibweise. Nachweis ``tests/unit/test_befund_f4_09_cron_uppercase_names.py``.
* PF-13 / R4F-07 (F3-35): 6- und 7-feldrige Ausdrücke wurden angenommen; der
  Test auf "genau 5 Felder" erzeugt sie wieder.

Testliste:
# [x] TaskInfo(cron=beliebig) -> ok oder ValueError
# [x] angenommener Ausdruck -> next_fire_times ohne Ausnahme, streng monoton, wiederholbar
# [x] Standard-Ausdruck nach crontab(5) -> angenommen oder "never fires"
# [x] angenommener Ausdruck hat 5 Felder (oder ist ein @-Alias)
# [x] DST: fester Stundenwert -> keine doppelte Wanduhrzeit in der Rückstellung
# [x] 0 0 30 2 * -> ValueError "never fires"
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from swarmq.scheduler.cron_time import next_fire_times
from swarmq.task import TaskInfo

pytestmark = [pytest.mark.timeout(60)]

# Zonen mit Besonderheiten: 30-min-DST (Lord Howe), negative DST (Dublin),
# Südhalbkugel (Santiago), Halbstundenversatz (St. John's), DST-Pausen im
# Ramadan (Casablanca), 45-min-Versatz (Chatham).
_ZONES = [
    None,
    "UTC",
    "Europe/Berlin",
    "America/New_York",
    "Australia/Lord_Howe",
    "Europe/Dublin",
    "America/Santiago",
    "America/St_Johns",
    "Africa/Casablanca",
    "Pacific/Chatham",
]
_zones = st.sampled_from(_ZONES)

_MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
_DAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
_ALIASES = ["@hourly", "@daily", "@midnight", "@weekly", "@monthly", "@yearly", "@annually"]


@st.composite
def _name(draw: st.DrawFn, names: list[str]) -> str:
    """Name in beliebiger Schreibweise ("case does not matter", crontab(5))."""
    name = draw(st.sampled_from(names))
    return draw(st.sampled_from([name, name.upper(), name.capitalize()]))


@st.composite
def _standard_field(draw: st.DrawFn, low: int, high: int, names: list[str] | None = None) -> str:
    """Ein Feld nach crontab(5): ``*``, Zahl, Bereich, Schritt, Liste, Name."""

    def atom() -> str:
        if names and draw(st.booleans()):
            return draw(_name(names))
        a = draw(st.integers(low, high))
        if draw(st.booleans()):
            return str(a)
        b = draw(st.integers(a, high))
        return f"{a}-{b}"

    kind = draw(st.sampled_from(["star", "atom", "list", "step"]))
    if kind == "star":
        return "*"
    if kind == "atom":
        return atom()
    if kind == "list":
        return ",".join(atom() for _ in range(draw(st.integers(2, 3))))
    base = draw(st.sampled_from(["*", f"{low}-{high}"]))
    return f"{base}/{draw(st.integers(1, high - low + 1))}"


@st.composite
def _standard_cron(draw: st.DrawFn) -> str:
    return " ".join(
        [
            draw(_standard_field(0, 59)),
            draw(_standard_field(0, 23)),
            draw(_standard_field(1, 31)),
            draw(_standard_field(1, 12, _MONTHS)),
            draw(_standard_field(0, 6, _DAYS)),
        ]
    )


_junk_token = st.one_of(
    st.sampled_from(["?", "L", "W", "#", "H", "R", "r"]),
    st.sampled_from(["*", "1W", "5L", "mon#2", "H(0-5)", "%s", "-", "/", ",", "@reboot"]),
    st.integers(-2, 100).map(str),
    st.tuples(st.integers(-1, 70), st.integers(-1, 70)).map(lambda ab: f"{ab[0]}-{ab[1]}"),
    st.tuples(st.sampled_from(["*", "0-59", "1"]), st.integers(-1, 70)).map(lambda ab: f"{ab[0]}/{ab[1]}"),
    st.sampled_from(_MONTHS + _DAYS),
    st.text(alphabet="0123456789*,-/?abcdefghijklmnopqstuvxyz", min_size=1, max_size=5),
)
_separator = st.sampled_from([" ", "  ", "\t", "\u00a0"])


@st.composite
def _junk_cron(draw: st.DrawFn, field_counts: list[int]) -> str:
    count = draw(st.sampled_from(field_counts))
    tokens = [draw(_junk_token) for _ in range(count)]
    return draw(_separator).join(tokens)


@st.composite
def _mutated_cron(draw: st.DrawFn) -> str:
    """Standard-Ausdruck mit einem ersetzten Feld: nah an gültig, damit
    Sonderzeichen die Prüfung erreichen statt an anderen Feldern zu scheitern."""
    fields = draw(_standard_cron()).split()
    fields[draw(st.integers(0, 4))] = draw(_junk_token)
    return " ".join(fields)


_any_cron = st.one_of(
    _standard_cron(),
    _mutated_cron(),
    _mutated_cron(),
    _junk_cron([1, 4, 5, 6, 7, 8]),
    st.sampled_from(_ALIASES).flatmap(lambda alias: st.sampled_from([alias, f" {alias}", f"{alias} ", alias.title()])),
    st.text(max_size=20),
)
# st.datetimes verlangt naive Grenzen; die Zone setzt ``timezones``.
_starts = st.datetimes(
    min_value=datetime(2025, 1, 1),  # noqa: DTZ001 - naive Grenze fuer st.datetimes
    max_value=datetime(2035, 12, 31),  # noqa: DTZ001 - naive Grenze fuer st.datetimes
    timezones=st.just(UTC),
)


def _accepted(cron: str, timezone: str | None = None) -> bool:
    try:
        TaskInfo(name="t", queues=["q"], cron=cron, timezone=timezone)
    except ValueError:
        return False
    return True


@given(cron=_any_cron, timezone=_zones)
@example(cron="0 0 30 2 *", timezone=None)
@example(cron="* * * * * * 2020", timezone=None)
@example(cron="*/0 * * * *", timezone=None)
@example(cron="R * * * *", timezone=None)
@example(cron="0 0 1 R 0", timezone=None)
@settings(max_examples=500, deadline=None)
def test_taskinfo_cron_is_accepted_or_a_value_error(cron: str, timezone: str | None) -> None:
    _accepted(cron, timezone)


@given(cron=_any_cron, timezone=_zones, start=_starts)
@example(cron="@hourly", timezone="Europe/Berlin", start=datetime(2026, 10, 25, tzinfo=UTC))
@settings(max_examples=500, deadline=None)
def test_accepted_cron_has_increasing_repeatable_fire_times(cron: str, timezone: str | None, start: datetime) -> None:
    if not _accepted(cron, timezone):
        return

    fires = next_fire_times(cron, start, timezone=timezone, count=6)

    assert len(fires) == 6
    assert fires[0] > start
    # Vergleich über Zeitstempel: zwei aware datetimes mit demselben ZoneInfo
    # vergleicht Python nach Wanduhrzeit ohne ``fold``; 01:30 (+11) und 01:30
    # (+10:30) in der Lord-Howe-Rückstellung gälten sonst als gleich.
    stamps = [fire.timestamp() for fire in fires]
    assert all(a < b for a, b in itertools.pairwise(stamps))
    assert next_fire_times(cron, start, timezone=timezone, count=6) == fires


@given(cron=_standard_cron())
@example(cron="0 0 * jul *")
@example(cron="0 0 * * MON-FRI")
@example(cron="* * * * WED")
@example(cron="0 0 * JUL *")
@settings(max_examples=500, deadline=None)
def test_standard_cron_is_accepted_or_never_fires(cron: str) -> None:
    error = ""
    try:
        TaskInfo(name="t", queues=["q"], cron=cron)
    except ValueError as exc:
        error = str(exc)
    assert not error or "never fires" in error, f"{cron!r}: {error}"


@given(cron=st.one_of(_junk_cron([1, 2, 3, 4, 5, 6, 7, 8, 9]), _standard_cron()))
@example(cron="0 0 * * * *")
@example(cron="0 0 9 * * *")
@example(cron="* * * * * * 2020")
@settings(max_examples=500, deadline=None)
def test_accepted_cron_has_five_fields(cron: str) -> None:
    if _accepted(cron):
        assert len(cron.split()) == 5 or cron.lower() in _ALIASES


@st.composite
def _fixed_hour_cron(draw: st.DrawFn) -> str:
    minute = draw(st.sampled_from(["0", "30", "*/15", "0,45", "*"]))
    hour = draw(st.sampled_from(["0", "1", "2", "3", "1-3", "2,14", "0-23/2"]))
    return f"{minute} {hour} * * *"


@given(
    cron=_fixed_hour_cron(),
    timezone=st.sampled_from([zone for zone in _ZONES if zone not in {None, "UTC"}]),
    year=st.integers(2025, 2035),
    month=st.sampled_from([3, 4, 9, 10, 11]),
    day=st.integers(1, 28),
)
@example(cron="30 2 * * *", timezone="Europe/Berlin", year=2026, month=10, day=24)
@example(cron="0 1 * * *", timezone="Australia/Lord_Howe", year=2026, month=4, day=4)
@settings(max_examples=500, deadline=None)
def test_fixed_hour_cron_has_no_doubled_wall_clock_time(
    cron: str, timezone: str, year: int, month: int, day: int
) -> None:
    zone = ZoneInfo(timezone)
    start = datetime(year, month, day, tzinfo=zone) - timedelta(hours=1)

    fires = next_fire_times(cron, start, timezone=timezone, count=40)

    walls = [fire.astimezone(zone).replace(tzinfo=None, fold=0) for fire in fires]
    assert len(set(walls)) == len(walls), f"doppelte Wanduhrzeit: {fires}"
    stamps = [fire.timestamp() for fire in fires]
    assert all(a < b for a, b in itertools.pairwise(stamps))
