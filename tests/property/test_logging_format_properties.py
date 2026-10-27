"""Property: Der Textmodus ohne Farben rendert wie structlogs ``ConsoleRenderer``.

``build_formatter(json=False, colors=False)`` darf die Spalten schneller
bauen als structlog, aber keine Zeile anders ausgeben. Der Vergleich laeuft
gegen ``structlog.dev.ConsoleRenderer(colors=False)`` der installierten
Version und schlaegt deshalb auch an, wenn ein structlog-Update das Format
aendert.
"""

from __future__ import annotations

import logging

import structlog
from hypothesis import example, given
from hypothesis import strategies as st

from swarmq.logging import build_formatter
from swarmq.logging_format import iso_utc

# Feldnamen mit Praefix: kollidieren nie mit LogRecord-Attributen oder den
# Schluesseln, die der Formatter selbst setzt (event, level, logger, ...).
_FIELD_NAMES = st.from_regex(r"f_[a-z0-9_]{0,8}", fullmatch=True)
# Kurzer Text aus Trenn- und Anfuehrungszeichen: entscheidet, ob ein Wert roh
# oder als ``repr`` erscheint, und kommt in beliebigem Text zu selten vor.
_TEXT = st.one_of(st.text(), st.text(alphabet=" \t=\r\n\"'ab", max_size=4))
_SCALARS = st.one_of(_TEXT, st.integers(), st.floats(), st.none(), st.booleans())
_VALUES = st.one_of(
    _SCALARS, st.lists(_SCALARS, max_size=3), st.dictionaries(st.text(max_size=5), _SCALARS, max_size=2)
)
_LEVELS = st.sampled_from([logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL])


# Jedes Zeichen, das einen Wert in Anfuehrungszeichen setzt, einzeln.
_QUOTING_EXAMPLE = {f"f_{i}": char for i, char in enumerate(" \t=\r\n\"'")} | {"f_plain": "roh"}


@example(event="e", logger_name="a", level=logging.INFO, fields=_QUOTING_EXAMPLE, created=0.0)
@given(
    event=_TEXT,
    logger_name=st.from_regex(r"[a-z][a-z0-9_.]{0,15}", fullmatch=True),
    level=_LEVELS,
    fields=st.dictionaries(_FIELD_NAMES, _VALUES, max_size=6),
    created=st.floats(min_value=0, max_value=4_000_000_000),
)
def test_plain_text_matches_structlog_console_renderer(
    event: str, logger_name: str, level: int, fields: dict[str, object], created: float
) -> None:
    record = logging.makeLogRecord(
        {
            "name": logger_name,
            "levelno": level,
            "levelname": logging.getLevelName(level),
            "msg": event,
            "created": created,
            **fields,
        }
    )
    method = logging.getLevelName(level).lower()
    expected = structlog.dev.ConsoleRenderer(colors=False)(
        None,
        method,
        {"timestamp": iso_utc(created), "level": method, "logger": logger_name, "event": event, **fields},
    )

    assert build_formatter(json=False, colors=False).format(record) == expected
