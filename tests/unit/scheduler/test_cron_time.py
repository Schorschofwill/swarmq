"""Cron-Termine mit Zeitzone und DST-Regel (Plan 2026-10-08-1541, U2, KTD2).

Tabellentests ohne echte Uhr: jeder Fall uebergibt seinen Referenzzeitpunkt.
Die erwarteten Termine stammen aus der Belastungspruefung vom 08.10.2026
(minutengenauer Referenzrechner gegen croniter 6.2.4)."""

# Test list (U2):
# 1. Ohne Zeitzone identisch zur bisherigen UTC-Rechnung (Regression).
# 2. ``Asia/Tokyo`` liefert UTC+9.
# 3. AE1: ``30 2 * * *`` Berlin feuert am 25.10.2026 nur um 02:30+02:00,
#    am 26.10. um 02:30+01:00.
# 4. ``* 2 * * * *`` (Sechs-Feld) in der Rueckstellungsnacht terminiert
#    und feuert nur im ersten Durchlauf.
# 5. AE2: ``*/30 * * * *`` Berlin feuert 02:00/02:30 in beiden Durchlaeufen.
# 6. AE7: ``0 */2`` beide Durchlaeufe, ``0 1-3`` nur 02:00+02:00,
#    ``@hourly`` beide Durchlaeufe.
# 7. ``@daily`` wird vor der Einstufung uebersetzt; ebenso ``@midnight``,
#    ``@weekly``, ``@monthly``, ``@yearly``, ``@annually`` und Grossschreibung.
# 8. ``0 3`` Berlin feuert einmal um 03:00+01:00, ``0 2`` einmal um
#    02:00+02:00.
# 9. AE3: ``30 2 * * *`` Berlin feuert am 28.03.2027 einmal um 03:00+02:00.
# 10. Fruehjahrsluecke mit ``*`` im Stundenfeld: ``15 * * * *`` (02:15
#     entfaellt), ``45 */2 * * *`` (03:00) festgeschrieben.
# 11. Referenz im zweiten Durchlauf (02:10+01:00) fuer ``30 2 * * *`` →
#     naechster Termin am Folgetag.
# 12. ``America/New_York`` am 01.11.2026, ``Australia/Lord_Howe``
#     (30-Minuten-DST) je ein Fall; dazu Lord Howes Luecke mit ``*``.
# 12a. ``30 2,3`` Berlin: Weitersuchen verschluckt 03:30+01:00 nicht.
# 13. R20-Rezept: ``0 1 * * *`` UTC und ``0 3 * * *`` Berlin bis 24.10.2026
#     identisch, ab 25.10. verschieden.
# 14. Interne Unix-ms-Variante entspricht der oeffentlichen Funktion.
# 15. Ungueltiger Ausdruck wirft croniters Fehler (nicht IndexError).
# 16. Naiver ``start`` (ohne tzinfo) wirft ValueError statt als Hostzeit
#     gelesen zu werden.

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from croniter import CroniterBadCronError

from swarmq.scheduler import cron_time, next_fire_times

BERLIN = "Europe/Berlin"


def _iso(cron: str, start: datetime, *, timezone: str | None = None, count: int) -> list[str]:
    return [d.isoformat() for d in next_fire_times(cron, start, timezone=timezone, count=count)]


class TestNextFireTimes:
    def test_without_timezone_matches_utc(self) -> None:
        start = datetime(2026, 10, 8, 12, 1, tzinfo=UTC)

        assert _iso("*/5 * * * *", start, count=2) == [
            "2026-10-08T12:05:00+00:00",
            "2026-10-08T12:10:00+00:00",
        ]

    def test_tokyo_is_utc_plus_nine(self) -> None:
        start = datetime(2026, 10, 7, 23, 0, tzinfo=UTC)

        assert _iso("0 9 * * *", start, timezone="Asia/Tokyo", count=1) == [
            "2026-10-08T09:00:00+09:00",
        ]

    def test_ae1_fixed_hour_fires_only_in_first_pass(self) -> None:
        start = datetime(2026, 10, 24, 12, 0, tzinfo=ZoneInfo(BERLIN))

        assert _iso("30 2 * * *", start, timezone=BERLIN, count=3) == [
            "2026-10-25T02:30:00+02:00",
            "2026-10-26T02:30:00+01:00",
            "2026-10-27T02:30:00+01:00",
        ]

    def test_ae2_star_hour_follows_real_time_in_both_passes(self) -> None:
        start = datetime(2026, 10, 25, 1, 40, tzinfo=ZoneInfo(BERLIN))

        assert _iso("*/30 * * * *", start, timezone=BERLIN, count=5) == [
            "2026-10-25T02:00:00+02:00",
            "2026-10-25T02:30:00+02:00",
            "2026-10-25T02:00:00+01:00",
            "2026-10-25T02:30:00+01:00",
            "2026-10-25T03:00:00+01:00",
        ]

    def test_ae7_hour_field_classification(self) -> None:
        start = datetime(2026, 10, 25, 0, 10, tzinfo=ZoneInfo(BERLIN))

        assert _iso("0 */2 * * *", start, timezone=BERLIN, count=3) == [
            "2026-10-25T02:00:00+02:00",
            "2026-10-25T02:00:00+01:00",
            "2026-10-25T04:00:00+01:00",
        ]
        assert _iso("0 1-3 * * *", start, timezone=BERLIN, count=4) == [
            "2026-10-25T01:00:00+02:00",
            "2026-10-25T02:00:00+02:00",
            "2026-10-25T03:00:00+01:00",
            "2026-10-26T01:00:00+01:00",
        ]
        assert _iso("@hourly", start, timezone=BERLIN, count=4) == [
            "2026-10-25T01:00:00+02:00",
            "2026-10-25T02:00:00+02:00",
            "2026-10-25T02:00:00+01:00",
            "2026-10-25T03:00:00+01:00",
        ]

    def test_daily_alias_is_classified_as_fixed_hour(self) -> None:
        start = datetime(2026, 10, 24, 12, 0, tzinfo=ZoneInfo(BERLIN))

        assert _iso("@daily", start, timezone=BERLIN, count=2) == [
            "2026-10-25T00:00:00+02:00",
            "2026-10-26T00:00:00+01:00",
        ]

    @pytest.mark.parametrize(
        ("alias", "expected"),
        [
            ("@midnight", "2026-10-26T00:00:00+01:00"),
            ("@weekly", "2026-11-01T00:00:00+01:00"),
            ("@monthly", "2026-11-01T00:00:00+01:00"),
            ("@yearly", "2027-01-01T00:00:00+01:00"),
            ("@annually", "2027-01-01T00:00:00+01:00"),
        ],
    )
    def test_other_aliases_are_translated(self, alias: str, expected: str) -> None:
        start = datetime(2026, 10, 25, 12, 0, tzinfo=ZoneInfo(BERLIN))

        assert _iso(alias, start, timezone=BERLIN, count=1) == [expected]

    def test_alias_is_case_insensitive_like_croniter(self) -> None:
        start = datetime(2026, 10, 24, 12, 0, tzinfo=ZoneInfo(BERLIN))

        assert _iso("@DAILY", start, timezone=BERLIN, count=2) == [
            "2026-10-25T00:00:00+02:00",
            "2026-10-26T00:00:00+01:00",
        ]

    def test_six_field_second_pass_skips_to_end_of_doubled_phase(self) -> None:
        """KTD2: im zweiten Durchlauf wird ab dem Ende der doppelten Phase
        weitergesucht, nicht Termin fuer Termin -- ein Sekunden-Cron braeuchte
        sonst 3600 croniter-Schritte (Laufzeit, keine Iterationsgrenze)."""
        start = datetime(2026, 10, 25, 2, 59, 58, tzinfo=ZoneInfo(BERLIN))

        with patch.object(cron_time, "croniter", wraps=cron_time.croniter) as spy:
            result = _iso("* 2 * * * *", start, timezone=BERLIN, count=3)

        assert result == [
            "2026-10-25T02:59:59+02:00",
            "2026-10-26T02:00:00+01:00",
            "2026-10-26T02:00:01+01:00",
        ]
        assert spy.call_count < 10

    def test_fixed_hour_after_doubled_phase_is_not_skipped(self) -> None:
        """Weitersuchen ab dem Ende der doppelten Phase darf den ersten
        Termin danach (03:30+01:00) nicht verschlucken."""
        start = datetime(2026, 10, 25, 0, 0, tzinfo=ZoneInfo(BERLIN))

        assert _iso("30 2,3 * * *", start, timezone=BERLIN, count=3) == [
            "2026-10-25T02:30:00+02:00",
            "2026-10-25T03:30:00+01:00",
            "2026-10-26T02:30:00+01:00",
        ]

    def test_hours_around_doubled_phase_fire_once(self) -> None:
        start = datetime(2026, 10, 24, 12, 0, tzinfo=ZoneInfo(BERLIN))

        assert _iso("0 3 * * *", start, timezone=BERLIN, count=2) == [
            "2026-10-25T03:00:00+01:00",
            "2026-10-26T03:00:00+01:00",
        ]
        assert _iso("0 2 * * *", start, timezone=BERLIN, count=2) == [
            "2026-10-25T02:00:00+02:00",
            "2026-10-26T02:00:00+01:00",
        ]

    def test_ae3_fixed_hour_in_spring_gap_fires_once_at_next_time(self) -> None:
        start = datetime(2027, 3, 27, 12, 0, tzinfo=ZoneInfo(BERLIN))

        assert _iso("30 2 * * *", start, timezone=BERLIN, count=2) == [
            "2027-03-28T03:00:00+02:00",
            "2027-03-29T02:30:00+02:00",
        ]

    def test_spring_gap_with_star_hour_keeps_croniter_behaviour(self) -> None:
        """R8: festgeschriebenes croniter-Verhalten in der Luecke."""
        start = datetime(2027, 3, 28, 1, 10, tzinfo=ZoneInfo(BERLIN))

        # 02:15 existiert nicht und entfaellt.
        assert _iso("15 * * * *", start, timezone=BERLIN, count=3) == [
            "2027-03-28T01:15:00+01:00",
            "2027-03-28T03:15:00+02:00",
            "2027-03-28T04:15:00+02:00",
        ]
        # 02:45 existiert nicht; croniter verschiebt auf 03:00.
        assert _iso("45 */2 * * *", start, timezone=BERLIN, count=2) == [
            "2027-03-28T03:00:00+02:00",
            "2027-03-28T04:45:00+02:00",
        ]

    def test_reference_in_second_pass_continues_next_day(self) -> None:
        """Dokumentiertes Verhalten: steht die Referenz schon im zweiten
        Durchlauf (02:10+01:00), ist 02:30+01:00 kein Termin."""
        start = datetime(2026, 10, 25, 2, 10, fold=1, tzinfo=ZoneInfo(BERLIN))

        assert _iso("30 2 * * *", start, timezone=BERLIN, count=1) == [
            "2026-10-26T02:30:00+01:00",
        ]

    def test_new_york_fall_back(self) -> None:
        zone = "America/New_York"
        start = datetime(2026, 11, 1, 0, 30, tzinfo=ZoneInfo(zone))

        assert _iso("30 1 * * *", start, timezone=zone, count=2) == [
            "2026-11-01T01:30:00-04:00",
            "2026-11-02T01:30:00-05:00",
        ]

    def test_lord_howe_half_hour_fall_back(self) -> None:
        zone = "Australia/Lord_Howe"
        start = datetime(2027, 4, 4, 1, 0, tzinfo=ZoneInfo(zone))

        assert _iso("45 1 * * *", start, timezone=zone, count=2) == [
            "2027-04-04T01:45:00+11:00",
            "2027-04-05T01:45:00+10:30",
        ]
        assert _iso("*/15 * * * *", start, timezone=zone, count=5) == [
            "2027-04-04T01:15:00+11:00",
            "2027-04-04T01:30:00+11:00",
            "2027-04-04T01:45:00+11:00",
            "2027-04-04T01:30:00+10:30",
            "2027-04-04T01:45:00+10:30",
        ]

    def test_r20_recipe_compares_utc_and_local_expression(self) -> None:
        """R20: ``0 1 * * *`` UTC und ``0 3 * * *`` Berlin liefern bis zum
        24.10.2026 dieselben Termine, ab dem 25.10. verschiedene."""
        start = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)

        old = next_fire_times("0 1 * * *", start, count=6)
        new = next_fire_times("0 3 * * *", start, timezone=BERLIN, count=6)

        assert old[:4] == new[:4]
        assert new[3].isoformat() == "2026-10-24T03:00:00+02:00"
        assert old[4] != new[4]
        assert new[4].isoformat() == "2026-10-25T03:00:00+01:00"

    def test_invalid_expression_raises_croniter_error(self) -> None:
        start = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)

        with pytest.raises(CroniterBadCronError):
            next_fire_times("not-a-cron", start)

    def test_naive_start_raises_value_error(self) -> None:
        """Ein naiver ``start`` haengt nicht von der Zeitzone des Hosts ab."""
        start = datetime(2026, 10, 8, 12, 0)  # noqa: DTZ001 - naive Zeit ist hier der geprüfte Fehlerfall

        with pytest.raises(ValueError, match="timezone-aware"):
            next_fire_times("0 3 * * *", start, timezone=BERLIN)


class TestNextFireMs:
    def test_matches_public_function_in_unix_ms(self) -> None:
        start = datetime(2026, 10, 25, 2, 10, tzinfo=ZoneInfo(BERLIN))
        ref_ms = int(start.timestamp() * 1000)

        expected = next_fire_times("30 2 * * *", start, timezone=BERLIN)[0]
        assert cron_time.next_fire_ms("30 2 * * *", BERLIN, ref_ms) == int(expected.timestamp() * 1000)

    def test_lord_howe_half_hour_spring_gap_with_star_hour(self) -> None:
        """R8: croniter-Verhalten in der 30-Minuten-Luecke festgeschrieben --
        02:15 existiert nicht und wird auf 02:30+11:00 verschoben (anders
        als in Berlin, wo 02:15 entfaellt)."""
        zone = "Australia/Lord_Howe"
        start = datetime(2026, 10, 4, 1, 10, tzinfo=ZoneInfo(zone))

        assert _iso("15 * * * *", start, timezone=zone, count=3) == [
            "2026-10-04T01:15:00+10:30",
            "2026-10-04T02:30:00+11:00",
            "2026-10-04T03:15:00+11:00",
        ]
