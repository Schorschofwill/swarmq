"""Tests fuer swarmq.header_text -- begrenzte Freitext-Header (BZ-17).

Testliste:
# [x] kurzer Text bleibt unveraendert
# [x] langer Text: Anfang (6 KiB) und Ende (2 KiB) bleiben, insgesamt <= 8 KiB
# [x] Multibyte-UTF-8: Grenze in Bytes, kein zerschnittenes Zeichen
# [x] einzelne Surrogates werden escaped statt den Publish zu sprengen
"""

from swarmq.header_text import HEADER_TEXT_LIMIT_BYTES, bounded_header_text


def test_short_text_is_unchanged() -> None:
    assert bounded_header_text("RuntimeError: boom") == "RuntimeError: boom"


def test_long_text_keeps_head_and_tail_within_the_limit() -> None:
    head = "H" * 7000
    tail = "T" * 3000
    text = head + "M" * 100_000 + tail

    bounded = bounded_header_text(text)

    assert len(bounded.encode()) <= HEADER_TEXT_LIMIT_BYTES
    assert bounded.startswith("H" * 6000)
    assert bounded.endswith("T" * 2048)
    assert "truncated" in bounded


def test_multibyte_text_is_cut_on_character_boundaries_within_the_byte_limit() -> None:
    # 3 Bytes je Zeichen; 1 Byte Versatz, damit kein Schnitt zufaellig passt.
    text = "x" + "€" * 50_000

    bounded = bounded_header_text(text)

    assert len(bounded.encode()) <= HEADER_TEXT_LIMIT_BYTES
    assert bounded.startswith("x€€€")
    assert bounded.endswith("€" * 600)


def test_text_with_lone_surrogates_becomes_encodable() -> None:
    # z. B. ein Dateiname aus os.fsdecode im Fehlertext (surrogateescape)
    bounded = bounded_header_text("cannot open \udcff.txt")

    assert bounded.encode() == b"cannot open \\udcff.txt"
