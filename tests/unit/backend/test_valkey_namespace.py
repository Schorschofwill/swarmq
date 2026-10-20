"""Namensraum am Valkey-Backend (Plan 0.12.0, MR 15, BZ-10).

``ValkeyConnection(namespace=...)`` setzt ``<ns>:`` vor alle Keys und Kanaele
des Backends und seiner Stores. Ohne ausdrueckliche Angabe gilt
``SWARMQ_NAMESPACE`` (gelesen ueber ``config.py``, Q11).

Test-Liste:
- [x] ``namespace="app1"``: Kanaele und Keys des Backends tragen ``app1:``
- [x] die Stores (Locks, Rate-Limit, Schedules, Guard, Chords) nutzen denselben KeySpace
- [x] ohne Angabe: ``SWARMQ_NAMESPACE`` aus der Umgebung; leer/ungesetzt -> ``sq:`` wie bisher
- [x] ausdruecklich ``namespace=""`` schlaegt die Umgebung (kein Namensraum)
- [x] ungueltiger Namensraum (Parameter oder Umgebung) -> ``ConfigurationError``
- [x] ``keys=`` und ``namespace=`` zugleich -> ``ConfigurationError``
- [x] ``namespace`` als lesbare Eigenschaft (fuer ``check`` und den Worker-Abgleich)
- [x] Kontrollkanal (``ValkeySetpointStore``) mit Namensraum
- [x] Abgleich mit einer Konfiguration ohne Namensraum: der aus der Umgebung
      übernommene Namensraum fällt weg (Testlücke D-9 der Runde 4)
"""

from __future__ import annotations

import pytest

from swarmq.backend.valkey import ValkeyBackend

pytestmark = pytest.mark.timeout(5)


@pytest.fixture(autouse=True)
def _no_env_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWARMQ_NAMESPACE", raising=False)


def test_namespace_prefixes_the_backend_channels() -> None:
    backend = ValkeyBackend("valkey://x", namespace="app1")

    assert backend.notify_channel_name("t1") == "app1:sq:notify:t1"


def test_backend_key_helpers_use_the_namespace() -> None:
    backend = ValkeyBackend("valkey://x", namespace="app1")

    assert backend._sched_fired_key("s:1") == "app1:sq:sched:fired:s:1"
    assert backend._sched_inflight_key("s:1") == "app1:sq:sched:inflight:s:1"
    assert backend._sched_handover_key("o") == "app1:sq:sched:handover:o"


def test_without_a_parameter_the_environment_sets_the_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "app2")

    assert ValkeyBackend("valkey://x").notify_channel_name("t1") == "app2:sq:notify:t1"


def test_an_explicit_empty_namespace_beats_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "app2")

    assert ValkeyBackend("valkey://x", namespace="").notify_channel_name("t1") == "sq:notify:t1"


@pytest.mark.parametrize("source", ["parameter", "environment"])
def test_an_invalid_namespace_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch, source: str) -> None:
    from swarmq.exceptions import ConfigurationError

    if source == "environment":
        monkeypatch.setenv("SWARMQ_NAMESPACE", "a:b")
    with pytest.raises(ConfigurationError, match="namespace"):
        ValkeyBackend("valkey://x", namespace="a:b" if source == "parameter" else None)


def test_keys_and_namespace_together_are_rejected() -> None:
    from swarmq.exceptions import ConfigurationError
    from swarmq.keyspace import KeySpace

    with pytest.raises(ConfigurationError, match="keys or namespace"):
        ValkeyBackend("valkey://x", keys=KeySpace("a"), namespace="b")


def test_the_namespace_is_readable() -> None:
    assert ValkeyBackend("valkey://x", namespace="app1").namespace == "app1"
    assert ValkeyBackend("valkey://x").namespace is None


@pytest.mark.entscheidung("E-012")
def test_the_control_channel_uses_the_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    from swarmq.backend.valkey.control import ValkeySetpointStore

    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")

    assert ValkeySetpointStore("valkey://x")._setpoint_key("g") == "app1:sq:control:setpoint:g"


@pytest.mark.entscheidung("E-012")
def test_reconciling_with_a_configuration_without_namespace_drops_the_environment_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Testlücke D-9 (Runde 4): Ohne ausdrücklichen Parameter übernimmt das
    Backend den Namensraum der Konfiguration. Hat das Backend ihn aus
    ``SWARMQ_NAMESPACE`` gelesen, die Konfiguration aber keinen, gilt danach
    kein Namensraum: Kanäle ohne Präfix, und der Rückgabewert meldet ``None``.

    Gegenprobe-Mutant: ``connection.py`` in ``reconcile_namespace``
    ``_keyspace(configured or "")`` → ``_keyspace(configured)`` (liest dann
    erneut die Umgebung)."""
    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")
    backend = ValkeyBackend("valkey://x")
    from_environment = backend.namespace

    effective = backend.reconcile_namespace(None)

    assert from_environment == "app1"
    assert (effective, backend.namespace) == (None, None)
    assert backend.notify_channel_name("t1") == "sq:notify:t1"
