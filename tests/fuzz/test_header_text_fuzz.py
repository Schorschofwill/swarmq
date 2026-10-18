"""Fuzz: begrenzte Freitext-Header (Fehlersuche Runde 4, U16, R20, KTD8).

Ziel ist ``swarmq.header_text.bounded_header_text``. Zusagen (Quelle:
Docstring und Modulkopf ``src/swarmq/header_text.py``, BZ-17; Unit-Tests
``tests/unit/test_header_text.py``):

* das Ergebnis passt in ``HEADER_TEXT_LIMIT_BYTES`` (8 KiB) UTF-8 und ist
  streng kodierbar (der Publish scheitert nicht an einem Surrogate);
* Text bis zur Grenze bleibt bis auf das Escapen einzelner Surrogates
  unverändert, leerer Text bleibt leer;
* längerer Text behält Anfang und Ende (der Kopf ist ein Präfix, das Ende
  ein Suffix des escapten Texts) und trägt den Kürzungsvermerk;
* der Helfer ist idempotent (ein zweiter Durchlauf ändert nichts).

Allow-List: leer. ``bounded_header_text`` sagt keine Ausnahme zu; jede
Ausnahme ist ein Treffer.

Testliste:
# [x] beliebiger Text (auch Surrogates) -> kein Fehler, <= 8 KiB, streng kodierbar
# [x] kurzer Text -> unverändert bis auf escapte Surrogates; leer bleibt leer
# [x] langer Text um die Grenze -> Präfix/Suffix des escapten Texts, Vermerk
# [x] Idempotenz
"""

from __future__ import annotations

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from swarmq.header_text import HEADER_TEXT_LIMIT_BYTES, bounded_header_text

pytestmark = [pytest.mark.timeout(60)]

# Alle Codepoints einschließlich einzelner Surrogates (Kategorie Cs), wie sie
# ``os.fsdecode`` mit ``surrogateescape`` in Fehlertexte bringt.
_chars = st.characters(exclude_categories=())
_short = st.text(alphabet=_chars, max_size=64)


@st.composite
def _around_the_limit(draw: st.DrawFn) -> str:
    """Text um die 8-KiB-Grenze: ein Baustein (1-4 Byte je Zeichen, auch
    Surrogates mit 6 Byte als ``\\udcff``) so oft wiederholt, dass die
    escapte Länge knapp unter, auf oder über der Grenze liegt."""
    chunk = draw(st.text(alphabet=_chars, min_size=1, max_size=8))
    tail = draw(_short)
    chunk_bytes = len(chunk.encode(errors="backslashreplace"))
    target = HEADER_TEXT_LIMIT_BYTES + draw(st.integers(-64, 3 * HEADER_TEXT_LIMIT_BYTES))
    return chunk * max(target // chunk_bytes, 0) + tail


_texts = st.one_of(_short, _around_the_limit())


def _escaped(text: str) -> bytes:
    return text.encode(errors="backslashreplace")


@given(text=_texts)
@example(text="")
@example(text="cannot open \udcff.txt")
@example(text="x" * HEADER_TEXT_LIMIT_BYTES)
@example(text="x" * (HEADER_TEXT_LIMIT_BYTES + 1))
@settings(max_examples=500, deadline=None)
def test_result_fits_the_limit_and_is_strictly_encodable(text: str) -> None:
    bounded = bounded_header_text(text)

    encoded = bounded.encode()  # streng: ein Surrogate im Ergebnis wirft hier
    assert len(encoded) <= HEADER_TEXT_LIMIT_BYTES


@given(text=_short)
@example(text="")
@settings(max_examples=500, deadline=None)
def test_short_text_only_escapes_surrogates(text: str) -> None:
    assert bounded_header_text(text).encode() == _escaped(text)


@given(text=_around_the_limit())
@settings(max_examples=500, deadline=None)
def test_long_text_keeps_a_prefix_and_a_suffix(text: str) -> None:
    raw = _escaped(text)
    bounded = bounded_header_text(text)

    if len(raw) <= HEADER_TEXT_LIMIT_BYTES:
        assert bounded.encode() == raw
        return
    marker = f"\n[... {len(raw) - HEADER_TEXT_LIMIT_BYTES} bytes truncated ...]\n"
    head, found, tail = bounded.partition(marker)
    assert found
    assert raw.startswith(head.encode())
    assert raw.endswith(tail.encode())
    # Der Kopf füllt das Budget bis auf ein zerschnittenes Zeichen (<= 3 Byte).
    assert len(bounded.encode()) >= HEADER_TEXT_LIMIT_BYTES - 6


@given(text=_texts)
@settings(max_examples=500, deadline=None)
def test_is_idempotent(text: str) -> None:
    once = bounded_header_text(text)

    assert bounded_header_text(once) == once
