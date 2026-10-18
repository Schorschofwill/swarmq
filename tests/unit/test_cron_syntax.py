"""Cron-Positivliste nach crontab(5) (Fix-Plan 0.13, U15, KTD9, R13; E-065).

``validate_cron_syntax`` prüft nur die Form; Wertebereiche und den nächsten
Termin prüft weiter croniter.

Testliste:
# [x] angenommen: 0 0 * * WED, @HOURLY, @midnight, */5 1-5 * jan,JUL mon-fri, 0 0 * * 7
# [x] abgelehnt mit eigener Meldung: R, 6 Felder, 7 Felder, L, H, ?, @reboot, 4 Felder
# [x] W und # als Operator, Name im falschen Feld, leerer Ausdruck
# [x] nur ASCII: Unicode-Ziffern und Fall-Faltung (langes s) gelten nicht
"""

from __future__ import annotations

import pytest

from swarmq.cron_syntax import validate_cron_syntax

pytestmark = pytest.mark.timeout(5)


@pytest.mark.entscheidung("E-065")
@pytest.mark.parametrize("expr", ["0 0 * * WED", "@HOURLY", "@midnight", "*/5 1-5 * jan,JUL mon-fri", "0 0 * * 7"])
def test_crontab5_forms_are_accepted(expr: str) -> None:
    validate_cron_syntax(expr)


@pytest.mark.entscheidung("E-065")
@pytest.mark.parametrize(
    ("expr", "reason"),
    [
        ("0 0 9 * * *", "got 6 fields"),
        ("* * * * * * 2020", "got 7 fields"),
        ("0 0 * *", "got 4 fields"),
        ("@reboot", "alias '@reboot' is not supported"),
        ("R * * * *", "random field 'R'"),
        ("H * * * *", "hash field 'H'"),
        ("? * * * *", r"'\?' is not supported"),
        ("0 3 L * *", r"extended syntax \(L/W/#\) in the day-of-month field"),
        ("0 3 15W * *", r"extended syntax \(L/W/#\) in the day-of-month field"),
        ("0 3 * * 1#2", r"extended syntax \(L/W/#\) in the day-of-week field"),
        ("mon * * * *", "invalid minute field 'mon'"),
        ("", "got 0 fields"),
        ("\u0661 * * * *", "invalid minute field"),  # arabisch-indische Ziffer
        ("0 0 * * \u017fun", "invalid day-of-week field"),  # langes s faltet sonst zu "sun"
    ],
)
def test_forms_outside_crontab5_are_rejected_with_their_own_message(expr: str, reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        validate_cron_syntax(expr)
