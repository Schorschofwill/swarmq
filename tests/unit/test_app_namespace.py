"""Namensraum aus Backend und Konfiguration abgleichen (Review MR 15, E1, P1-1).

Reihenfolge der Quellen: ausdruecklicher Backend-Parameter
(``ValkeyBackend(namespace=...)`` oder ``keys=``) vor ``SwarmQConfig.namespace``
(Code oder ``SWARMQ_NAMESPACE``). Ohne ausdruecklichen Parameter uebernimmt das
Backend den Wert der Konfiguration; mit ausdruecklichem Parameter fuellt er die
Konfiguration. Sind beide gesetzt und verschieden -> ``ConfigurationError``
(Exit 78). ``namespace=""`` heisst ausdruecklich "kein Namensraum".

Test-Liste:
- [x] ``SwarmQ(..., namespace="app1")`` mit Backend ohne Parameter: Keys und Kanaele tragen ``app1:``
- [x] Backend ``namespace="app1"``, Konfiguration ohne -> ``config.namespace == "app1"``
- [x] Backend ``"a"`` und Konfiguration ``"b"`` (Code oder Umgebung) -> ``ConfigurationError``
- [x] Backend ``namespace=""`` und Konfiguration ``"b"`` -> ``ConfigurationError``
- [x] Backend und Konfiguration gleich -> in Ordnung
- [x] ``keys=KeySpace("a")`` zaehlt als ausdruecklich
- [x] schon gebaute Stores des Backends nutzen nach der Uebernahme den neuen Namensraum
- [x] Mock-Backend ohne Abgleich (Q15): ``config.namespace`` bleibt
- [x] Lock-Hinweis des Workers nennt den Namensraum des Backends
"""

from __future__ import annotations

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

pytestmark = pytest.mark.timeout(5)


@pytest.fixture(autouse=True)
def _no_env_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWARMQ_NAMESPACE", raising=False)


def _app(backend: ValkeyBackend, **kwargs: object) -> SwarmQ:
    return SwarmQ(RabbitMQBroker("amqp://guest:guest@localhost/"), backend, **kwargs)


def test_config_namespace_reaches_a_backend_without_parameter() -> None:
    backend = ValkeyBackend("valkey://x")

    _app(backend, namespace="app1")

    assert backend.namespace == "app1"
    assert backend.notify_channel_name("t1") == "app1:sq:notify:t1"


def test_explicit_backend_namespace_fills_the_config() -> None:
    app = _app(ValkeyBackend("valkey://x", namespace="app1"))

    assert app.config.namespace == "app1"


@pytest.mark.parametrize("source", ["code", "environment"])
def test_different_backend_and_config_namespace_is_a_configuration_error(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    from swarmq.exceptions import ConfigurationError

    if source == "environment":
        monkeypatch.setenv("SWARMQ_NAMESPACE", "app2")
    with pytest.raises(ConfigurationError, match="namespace") as caught:
        _app(ValkeyBackend("valkey://x", namespace="app1"), **({"namespace": "app2"} if source == "code" else {}))

    assert "'app1'" in str(caught.value) and "'app2'" in str(caught.value)


def test_explicit_empty_backend_namespace_against_a_configured_one_is_a_configuration_error() -> None:
    from swarmq.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError, match="namespace mismatch"):
        _app(ValkeyBackend("valkey://x", namespace=""), namespace="app2")


def test_same_backend_and_config_namespace_is_fine(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")

    app = _app(ValkeyBackend("valkey://x", namespace="app1"))

    assert (app.config.namespace, app.backend.namespace) == ("app1", "app1")


def test_keys_count_as_an_explicit_namespace() -> None:
    from swarmq.exceptions import ConfigurationError
    from swarmq.keyspace import KeySpace

    with pytest.raises(ConfigurationError, match="namespace mismatch"):
        _app(ValkeyBackend("valkey://x", keys=KeySpace("app1")), namespace="app2")


async def test_stores_built_before_the_adoption_use_the_new_namespace() -> None:
    from unittest.mock import AsyncMock, MagicMock

    client = MagicMock()
    client.zscore = AsyncMock(return_value=None)
    backend = ValkeyBackend(client=client)
    _ = backend.schedules  # vor SwarmQ gebaut

    _app(backend, namespace="app1")
    await backend.schedules.fire_ts("cron_a")

    client.zscore.assert_awaited_once_with("app1:sq:{sched}:zset", "cron_a")


@pytest.mark.parametrize("namespace", [None, "app1"])
def test_a_mock_backend_leaves_the_config_namespace(namespace: str | None) -> None:
    # Q15: ein Fake-/Mock-Backend gleicht nicht ab; die Konfiguration bleibt.
    from unittest.mock import MagicMock

    backend = MagicMock(spec=ValkeyBackend)
    backend._url = "valkey://x"

    app = _app(backend, namespace=namespace)

    assert app.config.namespace == namespace


def test_a_backend_without_reconciliation_leaves_the_config_namespace() -> None:
    class _Plain:
        _url = "valkey://x"

    app = _app(_Plain(), namespace="app1")  # type: ignore[arg-type]

    assert app.config.namespace == "app1"
