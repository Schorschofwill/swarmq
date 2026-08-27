"""Geteilte Contract-Testklassen (KTD9).

Kein ``test_``-Präfix in den Modulnamen — die Klassen werden nicht hier
gesammelt, sondern von Bindings in ``tests/unit`` (Stub) und
``tests/integration`` (RabbitMQ, Valkey) per Ableitung::

    from tests.contracts import coordination

    class TestLockStoreInMemory(coordination.LockStoreContract):
        @pytest.fixture
        def impl(self, ...): ...

Bindings liefern Fixtures, überschreiben aber keine ``test_*``-Methoden
und tragen kein ``skip``/``xfail`` (Struktur-Guard). Gemeinsame Fixtures
aller Verträge:

- ``clock``: Objekt mit ``await advance(seconds)`` und ``now_ms()`` — beim
  Stub eine Fake-Clock, gegen echte Dienste echte Wartezeit.
"""
