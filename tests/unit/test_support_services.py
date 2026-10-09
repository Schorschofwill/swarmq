"""Schutz der Test-Infrastruktur vor fremden Diensten auf Standardports.

Ein Integrationslauf ohne Port-Variablen hat das Valkey eines fremden
Projekts auf ``localhost:6379`` geflusht. ``default_port_refusal`` entscheidet,
ob ein flushendes Fixture ein Ziel anfassen darf.

Test-Liste:
- [x] localhost + Valkey-Standardport 6379 -> verweigert
- [x] 127.0.0.1 und ::1 + 6379 -> verweigert
- [x] localhost + RabbitMQ-Standardport 5672 -> verweigert
- [x] Freigabe-Variable SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1 -> erlaubt
- [x] Freigabe-Variable mit anderem Wert als "1" -> weiter verweigert
- [x] CI-Service-Host ``valkey`` / ``rabbitmq`` auf Standardport -> erlaubt
- [x] Testports 6399 / 5699 auf localhost -> erlaubt
- [x] Meldung nennt Ziel und Freigabe-Variable
- [x] Standardports der Testinfrastruktur sind 5699/15699/6399
- [x] require_safe_target schlägt mit pytest.fail fehl, erlaubt sonst
- [x] require_safe_valkey_url liest Host/Port aus der URL
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from tests.support import services
from tests.support.services import default_port_refusal


class TestDefaultPortRefusal:
    def test_localhost_on_valkey_default_port_is_refused(self) -> None:
        assert default_port_refusal("localhost", 6379, env={}) is not None

    @pytest.mark.parametrize(("host", "port"), [("localhost", 6399), ("localhost", 5699)])
    def test_test_container_ports_are_allowed(self, host: str, port: int) -> None:
        assert default_port_refusal(host, port, env={}) is None

    @pytest.mark.parametrize(("host", "port"), [("valkey", 6379), ("rabbitmq", 5672)])
    def test_ci_service_hosts_on_default_ports_are_allowed(self, host: str, port: int) -> None:
        assert default_port_refusal(host, port, env={}) is None

    @pytest.mark.parametrize("host", ["127.0.0.1", "::1", "[::1]", "127.0.1.1", "0.0.0.0", "LOCALHOST"])
    def test_other_local_hosts_on_valkey_default_port_are_refused(self, host: str) -> None:
        assert default_port_refusal(host, 6379, env={}) is not None

    @pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
    def test_local_host_on_rabbitmq_default_port_is_refused(self, host: str) -> None:
        assert default_port_refusal(host, 5672, env={}) is not None

    def test_allow_variable_permits_local_default_ports(self) -> None:
        env = {"SWARMQ_TEST_ALLOW_DEFAULT_PORTS": "1"}
        assert default_port_refusal("localhost", 6379, env=env) is None
        assert default_port_refusal("127.0.0.1", 5672, env=env) is None

    @pytest.mark.parametrize("value", ["", "0", "true", "yes"])
    def test_allow_variable_requires_exactly_one(self, value: str) -> None:
        env = {"SWARMQ_TEST_ALLOW_DEFAULT_PORTS": value}
        assert default_port_refusal("localhost", 6379, env=env) is not None

    def test_refusal_names_target_service_and_allow_variable(self) -> None:
        reason = default_port_refusal("localhost", 6379, env={})
        assert reason is not None
        assert "localhost:6379" in reason
        assert "Valkey" in reason
        assert "SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1" in reason
        assert "SWARMQ_TEST_VALKEY_PORT" in reason


class TestDefaultPorts:
    def test_defaults_point_at_swarmq_test_containers(self) -> None:
        assert services.DEFAULT_RABBITMQ_PORT == 5699
        assert services.DEFAULT_RABBITMQ_MGMT_PORT == 15699
        assert services.DEFAULT_VALKEY_PORT == 6399

    def test_defaults_pass_the_guard_on_localhost(self) -> None:
        for port in (services.DEFAULT_RABBITMQ_PORT, services.DEFAULT_VALKEY_PORT):
            assert default_port_refusal("localhost", port, env={}) is None


class TestRequireSafeTarget:
    def test_fails_hard_on_local_default_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SWARMQ_TEST_ALLOW_DEFAULT_PORTS", raising=False)
        with pytest.raises(pytest.fail.Exception, match="localhost:6379"):
            services.require_safe_target("localhost", 6379)

    def test_passes_on_test_container_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SWARMQ_TEST_ALLOW_DEFAULT_PORTS", raising=False)
        services.require_safe_target("localhost", 6399)

    def test_reads_allow_variable_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SWARMQ_TEST_ALLOW_DEFAULT_PORTS", "1")
        services.require_safe_target("localhost", 6379)


class TestRequireSafeValkeyUrl:
    @pytest.mark.parametrize(
        "url", ["valkey://localhost:6379/0", "redis://127.0.0.1/0", "valkey://[::1]:6379/0"],
    )
    def test_fails_on_local_default_port_url(self, url: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SWARMQ_TEST_ALLOW_DEFAULT_PORTS", raising=False)
        with pytest.raises(pytest.fail.Exception, match="6379"):
            services.require_safe_valkey_url(url)

    @pytest.mark.parametrize("url", ["valkey://localhost:6399/0", "valkey://valkey:6379/0"])
    def test_passes_on_test_container_or_ci_host(self, url: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SWARMQ_TEST_ALLOW_DEFAULT_PORTS", raising=False)
        services.require_safe_valkey_url(url)


class TestFlushValkeyDb:
    @pytest.mark.timeout(5)
    async def test_refuses_before_connecting(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import valkey.asyncio as aio_valkey

        monkeypatch.delenv("SWARMQ_TEST_ALLOW_DEFAULT_PORTS", raising=False)
        connect = Mock(side_effect=AssertionError("must not connect"))
        monkeypatch.setattr(aio_valkey.Valkey, "from_url", connect)
        with pytest.raises(pytest.fail.Exception, match="localhost:6379"):
            await services.flush_valkey_db("valkey://localhost:6379/0")
        connect.assert_not_called()

    @pytest.mark.timeout(5)
    async def test_flushes_and_closes_allowed_target(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import valkey.asyncio as aio_valkey

        client = AsyncMock()
        from_url = Mock(return_value=client)
        monkeypatch.setattr(aio_valkey.Valkey, "from_url", from_url)
        await services.flush_valkey_db("valkey://localhost:6399/0")
        from_url.assert_called_once_with("valkey://localhost:6399/0")
        client.flushdb.assert_awaited_once()
        client.aclose.assert_awaited_once()


class TestRequireSafeServices:
    """Sitzungsstart jeder Dienst-Stufe (``check_docker``) prüft beide Dienste."""

    @pytest.fixture(autouse=True)
    def _safe_targets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SWARMQ_TEST_ALLOW_DEFAULT_PORTS", raising=False)
        monkeypatch.setattr(services, "RABBITMQ_HOST", "localhost")
        monkeypatch.setattr(services, "RABBITMQ_PORT", 5699)
        monkeypatch.setattr(services, "VALKEY_HOST", "localhost")
        monkeypatch.setattr(services, "VALKEY_PORT", 6399)

    def test_passes_on_test_containers(self) -> None:
        services.require_safe_services()

    def test_fails_on_local_valkey_default_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(services, "VALKEY_PORT", 6379)
        with pytest.raises(pytest.fail.Exception, match="Valkey"):
            services.require_safe_services()

    def test_fails_on_local_rabbitmq_default_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(services, "RABBITMQ_HOST", "127.0.0.1")
        monkeypatch.setattr(services, "RABBITMQ_PORT", 5672)
        with pytest.raises(pytest.fail.Exception, match="RabbitMQ"):
            services.require_safe_services()

    def test_passes_on_ci_service_hosts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(services, "RABBITMQ_HOST", "rabbitmq")
        monkeypatch.setattr(services, "RABBITMQ_PORT", 5672)
        monkeypatch.setattr(services, "VALKEY_HOST", "valkey")
        monkeypatch.setattr(services, "VALKEY_PORT", 6379)
        services.require_safe_services()
