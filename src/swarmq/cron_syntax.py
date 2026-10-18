"""Cron-Syntax nach crontab(5) (Fix-Plan 0.13, KTD9; E-065, Positivliste).

Erlaubt sind genau 5 Felder aus Zahlen, ``*``, ``-``, ``,`` und ``/``, im
Monats- und Wochentagsfeld auch Namen (``jan``, ``mon``) ohne Rücksicht auf
die Schreibweise, sowie die ``@``-Aliase. Wertebereiche, Schrittweiten und den
nächsten Termin prüft weiter croniter; hier geht es nur um die Form.
"""

from __future__ import annotations

import re

_ALIASES = frozenset({"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"})
_MONTHS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
_DAYS = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
# (Feldname, erlaubte Namen) in crontab(5)-Reihenfolge.
_FIELDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("minute", ()),
    ("hour", ()),
    ("day-of-month", ()),
    ("month", _MONTHS),
    ("day-of-week", _DAYS),
)


def _item_pattern(names: tuple[str, ...]) -> re.Pattern[str]:
    value = r"\d+" if not names else rf"(?:\d+|{'|'.join(names)})"
    return re.compile(rf"(?:\*|{value}(?:-{value})?)(?:/\d+)?", re.IGNORECASE | re.ASCII)


_ITEM_PATTERNS = tuple(_item_pattern(names) for _, names in _FIELDS)
# Quartz-Operatoren: L (letzter), 15W (nächster Werktag), LW, 5L (letzter Freitag).
_EXTENDED_TOKEN = re.compile(r"\d*L|\d+W|LW")


def _reject_item(item: str, field: str, expr: str) -> ValueError:
    """Eigene Meldung je bekannter Fremdsyntax, sonst eine allgemeine."""
    tokens = re.split(r"[-/(),]", item.upper())
    if "R" in tokens:
        return ValueError(f"cron uses croniter's random field 'R' in the {field} field: {expr!r}")
    if "H" in tokens:
        return ValueError(f"cron uses the hash field 'H' in the {field} field: {expr!r}")
    if "#" in item or any(_EXTENDED_TOKEN.fullmatch(token) for token in tokens):
        return ValueError(
            f"cron uses unsupported extended syntax (L/W/#) in the {field} field: {expr!r}; "
            "use a standard 5-field expression"
        )
    if "?" in item:
        return ValueError(f"cron uses '?' in the {field} field; '?' is not supported, use '*': {expr!r}")
    return ValueError(f"cron has an invalid {field} field {item!r}: {expr!r}")


def validate_cron_syntax(expr: str) -> None:
    """Wirft ``ValueError``, wenn *expr* kein Ausdruck nach crontab(5) ist."""
    if expr.startswith("@"):
        if expr.lower() not in _ALIASES:
            raise ValueError(f"cron alias {expr!r} is not supported; use one of {', '.join(sorted(_ALIASES))}")
        return
    fields = expr.split()
    if len(fields) != len(_FIELDS):
        raise ValueError(f"cron must have exactly 5 fields, got {len(fields)} fields: {expr!r}")
    for text, (field, _), pattern in zip(fields, _FIELDS, _ITEM_PATTERNS, strict=True):
        for item in text.split(","):
            if not pattern.fullmatch(item):
                raise _reject_item(item, field, expr)
