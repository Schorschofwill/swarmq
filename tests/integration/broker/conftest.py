"""Broker-Integration: Speicheralarm auf dem Test-Broker (BZ-16, Plan 0.14 KTD6).

``memory_alarm`` löst auf dem Test-Broker (Port 5699) per ``rabbitmqctl``
einen Speicher-Alarm aus und setzt die Wasserlinie danach exakt auf den
vorher gelesenen Wert zurück. Sie läuft nur, wenn der Docker-Container des
Test-Brokers eindeutig zu ``SWARMQ_TEST_RABBITMQ_PORT`` gehört. Tests, die sie
nutzen, tragen ``exclusive``: Der Alarm blockiert jeden Verleger des Brokers,
auch die parallelen xdist-Prozesse (``tests/support/xdist_isolation.py``).
Genutzt von BZ-16, U4 (Abbruch im Alarm) und U6 (PF-19 a).

``toxiproxy`` legt den Proxy ``rabbitmq`` vor den Test-Broker (wie die
Kombinationen: Compose-Stack 5673 -> rabbitmq:5672, sonst ein Host-Toxiproxy
einen Port unter dem Testport, Standard 5698 -> 5699) und schaltet ihn am Ende
wieder ein. Tests mit dieser Fixture teilen eine xdist-Gruppe
(``tests/support/xdist_isolation.py``). Genutzt von B24 und FS-08.
"""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
import time
from collections.abc import AsyncIterator, Iterator

import pytest

from tests.e2e.combinations._toxiproxy import (
    DEFAULT_ADMIN_HOST,
    DEFAULT_ADMIN_PORT,
    ProxyConfig,
    ToxiproxyClient,
    proxy_defaults,
)
from tests.support.services import RABBITMQ_HOST, RABBITMQ_PORT, port_is_open
from tests.support.wait import wait_until

CONTAINER = os.environ.get("SWARMQ_TEST_RABBITMQ_CONTAINER", "swarmq-test-rabbit")
_ALARM_WATERMARK = "0.0000001"


def rabbitmqctl(*args: str) -> str:
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "rabbitmqctl", *args],
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    return result.stdout.strip()


def rabbitmq_eval(expr: str) -> str:
    return rabbitmqctl("eval", expr)


def container_serves_test_port() -> bool:
    try:
        mapping = subprocess.run(
            ["docker", "port", CONTAINER, "5672/tcp"], capture_output=True, text=True, timeout=10, check=True
        ).stdout
    except OSError, subprocess.SubprocessError:
        return False
    return any(line.rsplit(":", 1)[-1].strip() == str(RABBITMQ_PORT) for line in mapping.splitlines())


async def wait_for_alarm(active: bool, timeout: float = 10.0) -> None:
    """Wartet, bis der Broker einen Alarm meldet (``active``) bzw. keinen mehr."""

    async def reached() -> bool:
        alarms = await asyncio.to_thread(rabbitmq_eval, "rabbit_alarm:get_alarms().")
        return (alarms != "[]") == active

    # rabbitmqctl kennt kein Warten auf Alarme.
    if not await wait_until(reached, timeout=timeout, interval=0.1):
        raise AssertionError(f"broker alarm state did not become active={active} within {timeout}s")


_ABSOLUTE = re.compile(r"^\{absolute,(\d+)\}$")


def _restore_args(original: str) -> list[str] | None:
    """Argumente für ``set_vm_memory_high_watermark``, die *original* exakt
    wiederherstellen: relativ (``0.4``) oder absolut (``{absolute,N}``, etwa
    das 4-GB-Limit des lokalen Test-Brokers). ``None``: unbekannte Form."""
    try:
        float(original)
    except ValueError:
        match = _ABSOLUTE.match(original)
        return ["absolute", match.group(1)] if match else None
    return [original]


class MemoryAlarm:
    """Aufruf löst den Speicher-Alarm aus; ``release()`` stellt die vorher
    gelesene Wasserlinie wieder her und wartet, bis der Alarm weg ist."""

    def __init__(self, original: str, restore: list[str]) -> None:
        self._original = original
        self._restore = restore

    def __call__(self) -> None:
        rabbitmqctl("set_vm_memory_high_watermark", _ALARM_WATERMARK)

    def release(self) -> None:
        rabbitmqctl("set_vm_memory_high_watermark", *self._restore)
        restored = rabbitmq_eval("vm_memory_monitor:get_vm_memory_high_watermark().")
        assert _restore_args(restored) == self._restore, f"watermark not restored: {restored} != {self._original}"
        # Erst zurückgeben, wenn der Alarm wirklich weg ist.
        deadline = time.monotonic() + 15
        while rabbitmq_eval("rabbit_alarm:get_alarms().") != "[]":
            assert time.monotonic() < deadline, "broker alarm still active after restoring the watermark"
            time.sleep(0.1)


@pytest.fixture
def memory_alarm() -> Iterator[MemoryAlarm]:
    """Löst einen Speicher-Alarm aus und setzt die Wasserlinie sicher zurück
    (am Testende immer, auch nach einem ``release()`` im Test)."""
    if not container_serves_test_port():
        pytest.skip(f"BZ-16: Docker-Container {CONTAINER!r} bedient nicht Port {RABBITMQ_PORT}")
    original = rabbitmq_eval("vm_memory_monitor:get_vm_memory_high_watermark().")
    restore = _restore_args(original)
    if restore is None:
        pytest.skip(f"BZ-16: unerwartete Wasserlinie {original!r}, Broker bleibt unangetastet")
    alarm = MemoryAlarm(original, restore)
    try:
        yield alarm
    finally:
        alarm.release()


_TOXIPROXY_HOST = os.environ.get("SWARMQ_TEST_TOXIPROXY_HOST", DEFAULT_ADMIN_HOST)
_TOXIPROXY_PORT = int(os.environ.get("SWARMQ_TEST_TOXIPROXY_PORT", str(DEFAULT_ADMIN_PORT)))
_DEFAULT_PROXY_PORT, _DEFAULT_UPSTREAM = proxy_defaults("rabbitmq", RABBITMQ_HOST, RABBITMQ_PORT, 5672, 5673)
#: Port, auf dem der Proxy vor dem Test-Broker lauscht.
PROXY_PORT = int(os.environ.get("SWARMQ_TEST_TOXIPROXY_RMQ_PORT", str(_DEFAULT_PROXY_PORT)))
_UPSTREAM = os.environ.get("SWARMQ_TEST_TOXIPROXY_RMQ_UPSTREAM", _DEFAULT_UPSTREAM)
#: Name des Proxys vor dem Test-Broker.
PROXY = "rabbitmq"


@pytest.fixture
async def toxiproxy() -> AsyncIterator[ToxiproxyClient]:
    """Proxy ``PROXY`` vor dem Test-Broker; am Ende wieder eingeschaltet."""
    if not port_is_open(_TOXIPROXY_HOST, _TOXIPROXY_PORT):
        pytest.skip(f"Toxiproxy not reachable at {_TOXIPROXY_HOST}:{_TOXIPROXY_PORT}")
    async with ToxiproxyClient(host=_TOXIPROXY_HOST, port=_TOXIPROXY_PORT) as client:
        await client.create_proxy(ProxyConfig(name=PROXY, listen=f"0.0.0.0:{PROXY_PORT}", upstream=_UPSTREAM))
        try:
            yield client
        finally:
            await client.set_proxy_enabled(PROXY, True)
