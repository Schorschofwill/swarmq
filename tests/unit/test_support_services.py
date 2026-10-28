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
- [x] ohne xdist: vhost ``/`` und Valkey-DB 0
- [x] xdist-Prozess gwN: vhost ``swarmq-test-gwN`` und DB N+1
- [x] mehr als 15 xdist-Prozesse -> Fehler (Valkey hat 16 Datenbanken)
- [x] vhost-Name je Testlauf eindeutig (RabbitMQ löscht vhosts asynchron; ein
      neuer Lauf darf nicht in die noch laufende Löschung des alten geraten)
- [x] RabbitMQNode: AMQP-URL und API-Pfad mit vhost; ``/`` bleibt ``/`` bzw. ``%2F``
- [x] URLs über einen anderen Host/Port (Toxiproxy) behalten vhost und DB des Prozesses
- [x] Löschen der Worker-vhosts teilt sich eine Frist über alle Broker: Der Sitzungsabbau
      zählt zur pytest-timeout-Frist des letzten Tests (5 s bei ``timeout(5)``); zwei Broker
      mit je 3 s sprengten sie unter Last (CI-Pipeline 441)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import httpx
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
        "url",
        ["valkey://localhost:6379/0", "redis://127.0.0.1/0", "valkey://[::1]:6379/0"],
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


class TestWorkerNamespace:
    def test_without_xdist_uses_default_vhost_and_db_zero(self) -> None:
        assert services.worker_namespace("") == ("/", 0)

    @pytest.mark.parametrize(
        ("worker", "expected"), [("gw0", ("swarmq-test-gw0", 1)), ("gw14", ("swarmq-test-gw14", 15))]
    )
    def test_xdist_worker_gets_own_vhost_and_db(self, worker: str, expected: tuple[str, int]) -> None:
        assert services.worker_namespace(worker) == expected

    def test_vhost_name_carries_the_test_run(self) -> None:
        first, _ = services.worker_namespace("gw2", run_id="0a1b2c3d4e5f")
        second, _ = services.worker_namespace("gw2", run_id="9f8e7d6c5b4a")
        assert first == "swarmq-test-gw2-0a1b2c3d"
        assert first != second

    def test_more_workers_than_valkey_databases_is_refused(self) -> None:
        with pytest.raises(RuntimeError, match="at most 15"):
            services.worker_namespace("gw15")


class TestRabbitMQNodeVhost:
    def test_default_vhost_keeps_root_paths(self) -> None:
        node = services.RabbitMQNode("h", 1, 2)
        assert node.amqp_url == "amqp://guest:guest@h:1/"
        assert node.vhost_api == "%2F"

    def test_worker_vhost_appears_in_url_and_api_path(self) -> None:
        node = services.RabbitMQNode("h", 1, 2, "swarmq-test-gw3")
        assert node.amqp_url == "amqp://guest:guest@h:1/swarmq-test-gw3"
        assert node.vhost_api == "swarmq-test-gw3"


class TestUrlsThroughProxy:
    def test_proxy_urls_keep_the_worker_namespace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(services, "RABBITMQ_VHOST", "swarmq-test-gw2")
        monkeypatch.setattr(services, "VALKEY_DB", 3)
        assert services.rabbitmq_amqp_url("p", 9) == "amqp://guest:guest@p:9/swarmq-test-gw2"
        assert services.valkey_db_url("p", 8) == "valkey://p:8/3"


class TestWorkerVhostCleanup:
    def test_deleting_the_vhosts_of_all_brokers_shares_one_deadline(self, monkeypatch: pytest.MonkeyPatch) -> None:
        jetzt = [100.0]
        geloescht: list[tuple[str, float]] = []

        def antworten(request: httpx.Request) -> httpx.Response:
            if request.method == "DELETE":
                geloescht.append((request.url.host, request.extensions["timeout"]["read"]))
                jetzt[0] += 2.5  # langsamer Broker: das Löschen braucht 2,5 s
            return httpx.Response(204)

        echter_client = httpx.Client
        monkeypatch.setattr(
            httpx,
            "Client",
            lambda **kwargs: echter_client(transport=httpx.MockTransport(antworten), **kwargs),
        )
        brokers = [
            services.RabbitMQNode("haupt", 5672, 15672, "swarmq-test-gw1"),
            services.RabbitMQNode("zweit", 5672, 15672, "swarmq-test-gw1"),
        ]

        with services.worker_vhosts(brokers, budget=3.0, clock=lambda: jetzt[0]):
            pass

        assert geloescht == [("haupt", 3.0), ("zweit", 0.5)]
