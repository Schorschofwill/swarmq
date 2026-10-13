"""Toxiproxy HTTP client + helpers for the combinations Tier-2/3 tests.

Spike validation lives in
``docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md``. The
spike confirmed that the Toxiproxy Admin REST API on ``:8474`` is
trivial to drive from ``httpx.AsyncClient``, so we keep the dependency
surface tiny and skip the ``toxiproxy-python`` package.

Concepts mirrored from the Toxiproxy data model:
- A **proxy** sits in front of an upstream and listens on ``listen``.
- A **toxic** attaches to a proxy and degrades traffic in one direction
  (``downstream`` = server → client, ``upstream`` = client → server).
  Common toxic types: ``timeout``, ``latency``, ``bandwidth``,
  ``slow_close``, ``slicer``, ``limit_data``.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any, Self

import httpx

# Default Admin-API location matches docker-compose.chaos.yml.
DEFAULT_ADMIN_HOST = "localhost"
DEFAULT_ADMIN_PORT = 8474


def proxy_defaults(
    service: str,
    host: str,
    port: int,
    compose_port: int,
    compose_proxy_port: int,
) -> tuple[int, str]:
    """Proxy-Port und Upstream eines Dienstes, abgeleitet aus seinem Testport.

    Zeigen die Tests auf den Compose-Stack (``port == compose_port``), lauscht
    Toxiproxy im Compose-Netz auf ``compose_proxy_port`` und erreicht den
    Dienst über seinen Namen. Sonst läuft ein eigener Toxiproxy im Host-Netz
    einen Port unter dem Dienst und leitet an ``host:port`` weiter.
    """
    if port == compose_port:
        return compose_proxy_port, f"{service}:{compose_port}"
    return port - 1, f"{host}:{port}"


@dataclass(frozen=True)
class ProxyConfig:
    """Declarative description of a Toxiproxy proxy."""

    name: str
    listen: str  # host:port the proxy listens on (e.g. "0.0.0.0:5673")
    upstream: str  # service:port toxiproxy forwards to (e.g. "rabbitmq:5672")
    enabled: bool = True


class ToxiproxyClient:
    """Async client against the Toxiproxy Admin REST API.

    Usage::

        async with ToxiproxyClient() as tp:
            await tp.create_proxy(ProxyConfig(name="rmq", listen="...", upstream="..."))
            toxic_id = await tp.add_toxic("rmq", "timeout", attributes={"timeout": 0})
            # ... do failure-injection ...
            await tp.remove_toxic("rmq", toxic_id)

    The context manager ``reset()``s all proxies on exit so a leaked test
    doesn't leak failure-injection state into the next test.
    """

    def __init__(
        self,
        host: str = DEFAULT_ADMIN_HOST,
        port: int = DEFAULT_ADMIN_PORT,
        *,
        timeout: float = 5.0,
    ) -> None:
        self._base_url = f"http://{host}:{port}"
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
        )
        return self

    async def __aexit__(self, *_exc: object) -> None:
        try:
            with contextlib.suppress(Exception):
                await self.reset()
        finally:
            if self._client is not None:
                await self._client.aclose()
                self._client = None

    @property
    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError(
                "ToxiproxyClient must be used as an async context manager",
            )
        return self._client

    # ---- Proxy lifecycle ----

    async def list_proxies(self) -> dict[str, Any]:
        """Return ``{proxy_name: proxy_dict}`` for all registered proxies."""
        resp = await self._http.get("/proxies")
        resp.raise_for_status()
        proxies: dict[str, Any] = resp.json()
        return proxies

    async def create_proxy(self, config: ProxyConfig) -> dict[str, Any]:
        """Idempotent-ish: if the proxy already exists with matching listen+upstream,
        return its dict. Otherwise create it. If it exists with different config,
        delete and re-create (tests routinely reuse names across runs)."""
        existing = await self.list_proxies()
        current: dict[str, Any] | None = existing.get(config.name)
        if current is not None:
            same = (
                current.get("listen", "").endswith(config.listen.split(":")[-1])
                and current.get("upstream") == config.upstream
            )
            if same:
                return current
            await self.delete_proxy(config.name)
        payload = {
            "name": config.name,
            "listen": config.listen,
            "upstream": config.upstream,
            "enabled": config.enabled,
        }
        resp = await self._http.post("/proxies", json=payload)
        if resp.is_error:
            # Toxiproxy answers 500 when the listen port is taken; name the
            # proxy and its config instead of a bare status line.
            msg = (
                f"Toxiproxy could not create proxy {config.name!r} "
                f"(listen {config.listen}, upstream {config.upstream}): "
                f"{resp.status_code} {resp.text.strip()}. Set the "
                "SWARMQ_TEST_TOXIPROXY_* variables, see docs/testing/test-strategy.md."
            )
            raise RuntimeError(msg)
        created: dict[str, Any] = resp.json()
        return created

    async def delete_proxy(self, name: str) -> None:
        resp = await self._http.delete(f"/proxies/{name}")
        # 404 is fine — caller wanted it gone, it is.
        if resp.status_code not in (204, 404):
            resp.raise_for_status()

    async def set_proxy_enabled(self, name: str, enabled: bool) -> None:
        """Disabling a proxy drops all current connections and refuses new ones.
        Re-enabling restores it. Useful for hard network-partition simulation."""
        resp = await self._http.post(
            f"/proxies/{name}",
            json={"enabled": enabled},
        )
        resp.raise_for_status()

    # ---- Toxics ----

    async def add_toxic(
        self,
        proxy_name: str,
        toxic_type: str,
        *,
        attributes: dict[str, Any] | None = None,
        stream: str = "downstream",
        toxicity: float = 1.0,
        name: str | None = None,
    ) -> str:
        """Attach a toxic to a proxy. Returns the toxic's ``name`` for later removal.

        ``toxic_type`` examples: ``timeout``, ``latency``, ``bandwidth``,
        ``slow_close``, ``slicer``, ``limit_data``. See Toxiproxy README for
        ``attributes`` per type.

        ``stream`` is ``downstream`` (server → client) or ``upstream``
        (client → server). ``toxicity`` is the probability the toxic
        applies to each connection (1.0 = always).
        """
        payload: dict[str, Any] = {
            "type": toxic_type,
            "stream": stream,
            "toxicity": toxicity,
            "attributes": attributes or {},
        }
        if name is not None:
            payload["name"] = name
        resp = await self._http.post(
            f"/proxies/{proxy_name}/toxics",
            json=payload,
        )
        resp.raise_for_status()
        toxic_name: str = resp.json()["name"]
        return toxic_name

    async def remove_toxic(self, proxy_name: str, toxic_name: str) -> None:
        resp = await self._http.delete(
            f"/proxies/{proxy_name}/toxics/{toxic_name}",
        )
        if resp.status_code not in (204, 404):
            resp.raise_for_status()

    async def list_toxics(self, proxy_name: str) -> list[dict[str, Any]]:
        resp = await self._http.get(f"/proxies/{proxy_name}/toxics")
        resp.raise_for_status()
        toxics: list[dict[str, Any]] = resp.json()
        return toxics

    # ---- Bulk operations ----

    async def reset(self) -> None:
        """Re-enable all proxies and remove all toxics. Idempotent: safe to call
        on a clean Toxiproxy instance."""
        resp = await self._http.post("/reset")
        resp.raise_for_status()
