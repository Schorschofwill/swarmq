"""Ausfuehrungsidentitaet (Plan 0.12.0, MR 12, BZ-08/BZ-12): ``swarmq.identity``.

Identitaet = ``task_id`` | H | #n mit H = ``x-swarmq-replay-id`` und
n = ``x-swarmq-xdeath-base`` + Summe der ``count`` aller ``x-death``-Eintraege,
deren Queue keine Delay-Queue ist. Jede Kopie, die swarmq selbst publiziert,
setzt die Base und entfernt ``x-death`` (Spike S5: ein Broker-DLX baut
``x-death`` neu auf, Spike S7: ein wiederholter Broker-DLX erhoeht nur count).

Test-Liste:
1. [x] Ohne Header ist n = 0.
2. [x] n zaehlt ``count`` der ``x-death``-Eintraege ausserhalb der Delay-Queues
       (``{q}.DQ`` und jede Stufe ``{q}.DQ.<stufe>`` zaehlen nicht).
3. [x] ``x-swarmq-xdeath-base`` wird addiert.
4. [x] Kaputte Werte (kein int, negativ, kein Eintrag-Dict, zu grosse Base)
       zaehlen wie keine.
5. [x] Identitaet bei n = 0: byte-gleich zu 0.11 (``task_id`` bzw. ``task_id|H``).
6. [x] Identitaet bei n > 0: ``#n`` dazu; H nur, wenn gueltig (<= 64 Zeichen).
7. [x] ``copy_headers``: entfernt ``x-death``, setzt die Base nur bei n > 0,
       entfernt eine Base bei n = 0, laesst andere Header unberuehrt.
8. [x] ``generation_id``: ``"g"`` + sha1(Identitaet)[:39], deterministisch,
       40 Zeichen, verschieden je Identitaet.
9. [x] ``task_id_of(body)``: Task-ID aus dem Body, None bei kaputtem Body.
10. [x] ``is_replay``: True, sobald H gesetzt oder n > 0.
"""

from __future__ import annotations

import hashlib

from swarmq.identity import copy_headers, execution_identity, generation_id, is_replay, task_id_of, xdeath_count


def test_without_headers_n_is_zero() -> None:
    assert xdeath_count({}) == 0


def _death(queue: str, count: int, reason: str = "rejected") -> dict[str, object]:
    return {"queue": queue, "count": count, "reason": reason, "exchange": "", "routing-keys": [queue]}


def test_n_counts_x_death_outside_the_delay_queues() -> None:
    headers = {
        "x-death": [
            _death("tasks", 2),
            _death("tasks.DQ", 5, "expired"),
            _death("tasks.DQ.15s", 7, "expired"),
            _death("tasks.XQ", 1, "expired"),
        ],
    }
    assert xdeath_count(headers) == 3


def test_base_header_is_added_to_n() -> None:
    assert xdeath_count({"x-swarmq-xdeath-base": 4}) == 4
    assert xdeath_count({"x-swarmq-xdeath-base": 4, "x-death": [_death("tasks", 1)]}) == 5


def test_broken_values_count_as_none_and_n_is_capped() -> None:
    assert xdeath_count({"x-swarmq-xdeath-base": "4"}) == 0
    assert xdeath_count({"x-swarmq-xdeath-base": -1}) == 0
    assert xdeath_count({"x-swarmq-xdeath-base": True}) == 0
    assert xdeath_count({"x-death": "garbage"}) == 0
    assert xdeath_count({"x-death": ["garbage", {"queue": 1, "count": 1}, {"queue": "q", "count": "1"}]}) == 0
    # Deckel: ein gefaelschter Header macht den Valkey-Key nicht beliebig lang.
    assert xdeath_count({"x-swarmq-xdeath-base": 10**15, "x-death": [_death("q", 10**15)]}) == 999_999_999_999


def test_identity_without_deaths_is_byte_identical_to_0_11() -> None:
    assert execution_identity({}, "t-1") == "t-1"
    assert execution_identity({"x-swarmq-replay-id": "r1"}, "t-1") == "t-1|r1"
    assert execution_identity({"x-death": [_death("tasks.DQ.1s", 3, "expired")]}, "t-1") == "t-1"


def test_identity_with_deaths_appends_n() -> None:
    deaths = {"x-death": [_death("tasks", 2)], "x-swarmq-xdeath-base": 1}
    assert execution_identity(deaths, "t-1") == "t-1#3"
    assert execution_identity({**deaths, "x-swarmq-replay-id": "r1"}, "t-1") == "t-1|r1#3"
    assert execution_identity({**deaths, "x-swarmq-replay-id": "x" * 65}, "t-1") == "t-1#3"


def test_copy_headers_moves_x_death_into_the_base_and_keeps_the_rest() -> None:
    original = {"x-death": [_death("tasks", 2), _death("tasks.DQ", 9, "expired")], "x-keep": "k"}
    copied = copy_headers(original)
    assert copied == {"x-swarmq-xdeath-base": 2, "x-keep": "k"}
    assert xdeath_count(copied) == xdeath_count(original)
    assert original["x-death"], "das Original bleibt unberuehrt"
    # Base plus neue Tode addieren sich; bei n = 0 traegt die Kopie keine Base
    # (byte-gleich zu 0.11).
    assert copy_headers({"x-swarmq-xdeath-base": 1, "x-death": [_death("tasks", 1)]}) == {"x-swarmq-xdeath-base": 2}
    assert copy_headers({"x-swarmq-xdeath-base": 0, "x-death": [_death("tasks.DQ", 1, "expired")]}) == {}
    assert copy_headers(None) == {}


def test_generation_id_is_a_deterministic_sha1_of_the_identity() -> None:
    gen = generation_id("t-1|r1#3")
    assert gen == "g" + hashlib.sha1(b"t-1|r1#3").hexdigest()[:39]
    assert len(gen) == 40 and gen == generation_id("t-1|r1#3")
    assert generation_id("t-1|r1#4") != gen


def test_task_id_of_reads_the_body_and_tolerates_garbage() -> None:
    assert task_id_of(b'{"task": {"id": "t-1", "name": "n"}}') == "t-1"
    assert task_id_of(b"not json") is None
    assert task_id_of(b"[]") is None
    assert task_id_of(b'{"task": {"id": 5}}') is None
    assert task_id_of(b'{"task": "x"}') is None


def test_is_replay_once_a_replay_id_or_deaths_are_present() -> None:
    assert is_replay({}) is False
    assert is_replay({"x-death": [_death("tasks.DQ", 1, "expired")]}) is False
    assert is_replay({"x-swarmq-replay-id": "r1"}) is True
    assert is_replay({"x-death": [_death("tasks", 1)]}) is True
    assert is_replay({"x-swarmq-xdeath-base": 1}) is True
