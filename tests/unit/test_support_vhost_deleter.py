"""Löschen der Worker-vhosts im abgelösten Prozess (TS-05).

Bricht der Client ein ``DELETE /vhosts/<v>`` ab, bricht RabbitMQ auch die
Löschung ab; vhost und Queues bleiben liegen. ``tests/support/vhost_deleter.py``
läuft deshalb außerhalb der pytest-timeout-Frist und lässt jedem ``DELETE``
Zeit.

Test-Liste:
- [x] alle vhosts werden gelöscht, auch wenn das Löschen auf einem Broker scheitert
- [x] kein ``DELETE`` mit kurzer Client-Frist
- [x] Kommandozeile: Paare aus Management-URL und vhost-Pfadsegment
"""

from __future__ import annotations

import httpx
import pytest

from tests.support import vhost_deleter


def test_every_vhost_is_deleted_even_when_one_broker_fails() -> None:
    geloescht: list[tuple[str, str]] = []

    def antworten(request: httpx.Request) -> httpx.Response:
        geloescht.append((request.url.host, request.url.path))
        if request.url.host == "haupt":
            raise httpx.ReadTimeout("langsam", request=request)
        return httpx.Response(204)

    vhost_deleter.delete_vhosts(
        [("http://haupt:15672/api", "swarmq-test-gw1"), ("http://zweit:15672/api", "swarmq-test-gw1")],
        transport=httpx.MockTransport(antworten),
    )

    assert sorted(geloescht) == [
        ("haupt", "/api/vhosts/swarmq-test-gw1"),
        ("zweit", "/api/vhosts/swarmq-test-gw1"),
    ]


def test_a_delete_is_given_time_to_finish() -> None:
    fristen: list[float | None] = []

    def antworten(request: httpx.Request) -> httpx.Response:
        fristen.append(request.extensions["timeout"]["read"])
        return httpx.Response(204)

    vhost_deleter.delete_vhosts([("http://haupt:15672/api", "v")], transport=httpx.MockTransport(antworten))

    assert fristen == [vhost_deleter.DELETE_TIMEOUT_S]
    assert vhost_deleter.DELETE_TIMEOUT_S >= 60


def test_the_command_line_takes_pairs_of_url_and_vhost(monkeypatch: pytest.MonkeyPatch) -> None:
    gerufen: list[list[tuple[str, str]]] = []
    monkeypatch.setattr(vhost_deleter, "delete_vhosts", gerufen.append)

    vhost_deleter.main(["http://a/api", "v1", "http://b/api", "v2"])

    assert gerufen == [[("http://a/api", "v1"), ("http://b/api", "v2")]]
