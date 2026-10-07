"""Smoke tests for the combinations fixtures + helpers (Plan 2026-05-26-002 Unit 2).

These tests verify that the foundation lifts cleanly — module imports,
dataclass shapes, helper signatures, and Toxiproxy round-trip when
available. They are intentionally narrow; the heavier per-combo tests
live in ``test_state_sharing.py`` etc.
"""

from __future__ import annotations

import inspect

import pytest

from tests.support import worker_subprocess as _subprocess

from ._toxiproxy import ProxyConfig, ToxiproxyClient

# ---------------------------------------------------------------------------
# Import / structure
# ---------------------------------------------------------------------------


def test_subprocess_module_exports_expected_helpers() -> None:
    """Public surface of ``tests.support.worker_subprocess`` matches what the original
    ``test_worker_shutdown_load.py`` lifted from."""
    expected = {
        "port_open",
        "spawn_worker",
        "wait_for_worker_started",
        "kill_subprocess",
        "ensure_fixture_registered",
    }
    actual = {name for name in dir(_subprocess) if not name.startswith("_")}
    assert expected <= actual, (
        f"_subprocess missing helpers: {expected - actual}"
    )


def test_spawn_worker_signature_requires_fixture_module() -> None:
    """Parametrised ``spawn_worker`` must accept fixture_module / broker_url /
    backend_url — this is the lift's whole point versus the bound original."""
    sig = inspect.signature(_subprocess.spawn_worker)
    required = {"fixture_module", "broker_url", "backend_url"}
    actual_kwonly = {
        name for name, p in sig.parameters.items()
        if p.kind == inspect.Parameter.KEYWORD_ONLY
    }
    assert required <= actual_kwonly, (
        f"spawn_worker missing kwonly params: {required - actual_kwonly}"
    )


def test_test_worker_shutdown_load_still_importable() -> None:
    """Regression: the shutdown-load suite must remain importable after the
    helper lift — its file-local shims delegate to ``_subprocess``."""
    import tests.e2e.test_worker_shutdown_load as shutdown_mod

    # Backward-compat shim names still present on the module.
    for name in ("_spawn_worker", "_kill_subprocess", "_wait_for_worker_started"):
        assert hasattr(shutdown_mod, name), f"shim {name!r} disappeared"


# ---------------------------------------------------------------------------
# Toxiproxy client — pure-Python checks
# ---------------------------------------------------------------------------


def test_proxy_config_is_immutable_dataclass() -> None:
    """ProxyConfig is frozen so tests can pass it around freely without
    accidentally mutating shared state."""
    cfg = ProxyConfig(name="x", listen="0.0.0.0:1", upstream="y:1")
    with pytest.raises((AttributeError, Exception)):
        cfg.enabled = False  # type: ignore[misc]


def test_toxiproxy_client_requires_async_context() -> None:
    """Using ToxiproxyClient outside ``async with`` must fail loudly, not
    return ``None`` from ``_http``."""
    tp = ToxiproxyClient()
    with pytest.raises(RuntimeError, match="async context manager"):
        _ = tp._http


# ---------------------------------------------------------------------------
# Toxiproxy round-trip — skipped when Toxiproxy is not running
# ---------------------------------------------------------------------------


async def test_toxiproxy_client_create_proxy_smoke(toxiproxy: ToxiproxyClient) -> None:
    """Happy-path: create a proxy, list it, delete it. Skipped when Toxiproxy
    is unreachable (fixture handles the skip)."""
    cfg = ProxyConfig(
        name="smoke-rmq",
        listen="0.0.0.0:25673",
        upstream="rabbitmq:5672",
    )
    await toxiproxy.create_proxy(cfg)
    try:
        proxies = await toxiproxy.list_proxies()
        assert "smoke-rmq" in proxies
        assert proxies["smoke-rmq"]["upstream"] == "rabbitmq:5672"
    finally:
        await toxiproxy.delete_proxy("smoke-rmq")
        proxies_after = await toxiproxy.list_proxies()
        assert "smoke-rmq" not in proxies_after


async def test_toxiproxy_client_add_remove_toxic(
    toxiproxy: ToxiproxyClient,
) -> None:
    """Toxic-Lifecycle: create proxy, add timeout toxic, remove it, verify."""
    cfg = ProxyConfig(
        name="smoke-toxic",
        listen="0.0.0.0:25674",
        upstream="rabbitmq:5672",
    )
    await toxiproxy.create_proxy(cfg)
    try:
        toxic_name = await toxiproxy.add_toxic(
            "smoke-toxic",
            "timeout",
            attributes={"timeout": 100},
            name="my-timeout",
        )
        assert toxic_name == "my-timeout"

        toxics = await toxiproxy.list_toxics("smoke-toxic")
        assert any(t["name"] == "my-timeout" for t in toxics)

        await toxiproxy.remove_toxic("smoke-toxic", "my-timeout")
        toxics_after = await toxiproxy.list_toxics("smoke-toxic")
        assert all(t["name"] != "my-timeout" for t in toxics_after)
    finally:
        await toxiproxy.delete_proxy("smoke-toxic")


async def test_toxiproxy_reset_idempotent(toxiproxy: ToxiproxyClient) -> None:
    """``reset()`` on a clean Toxiproxy (no proxies, no toxics) must succeed —
    tests rely on calling it during context-manager cleanup unconditionally."""
    await toxiproxy.reset()
    proxies = await toxiproxy.list_proxies()
    # After reset, no proxies registered (or they're empty of toxics).
    for proxy in proxies.values():
        assert proxy.get("toxics") in (None, [], {})


# Note: autouse ``_combinations_cleanup`` fixture is verified implicitly by
# the per-combo tests in U4-U7 — each one registers its own Task subclasses
# and depends on the previous test's registrations being cleared.
