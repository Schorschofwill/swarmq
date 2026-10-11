"""``UnroutableMessage`` (BZ-03, MR 6) ist Teil der oeffentlichen Oberflaeche:
Der Changelog nennt ``swarmq.UnroutableMessage``, also muss der Name direkt
aus ``swarmq`` importierbar sein und in ``__all__`` stehen.

Test-Liste:
- [x] Export aus ``swarmq``, gleiche Klasse wie in ``swarmq.exceptions``
"""

from __future__ import annotations


def test_unroutable_message_is_exported_from_the_package() -> None:
    import swarmq
    from swarmq import UnroutableMessage, exceptions

    assert UnroutableMessage is exceptions.UnroutableMessage
    assert issubclass(UnroutableMessage, exceptions.BrokerConnectionError)
    assert "UnroutableMessage" in swarmq.__all__
