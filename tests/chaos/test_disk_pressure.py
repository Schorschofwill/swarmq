"""F4 — Disk / memory pressure chaos tests.

Plan: docs/plans/2026-04-17-007-feat-e2e-hardening-plan.md U2.4.

These tests exercise the backend behaviour when the underlying data
store runs out of memory:

* Valkey with ``maxmemory 20mb`` + ``maxmemory-policy noeviction`` —
  a SET beyond the limit returns an OOM ``ResponseError`` which the
  Valkey client raises as a plain ``ResponseError`` (subclass of
  ``ValkeyError``). ``ValkeyBackend.store_result`` catches
  ``ValkeyError`` and re-raises ``BackendConnectionError``. The
  contract we lock here is: OOM surfaces as ``BackendConnectionError``
  (never silent corruption).

* RabbitMQ disk-alarm behaviour is much harder to trigger reliably —
  it requires a custom ``rabbitmq.conf`` with ``disk_free_limit``
  bumped above the actual free disk, or memory-watermark manipulation.
  Deferred; the test exists as a documented skip.

* Recovery — after freeing memory (DELing the filler keys) subsequent
  SETs succeed again.

CI-hostile (per plan §6 F4): the whole module is default-skipped.
Set ``SWARMQ_RUN_DISK_TESTS=1`` locally to execute.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import Iterator

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError
from tests.chaos.conftest import _free_port, _wait_port

pytestmark = pytest.mark.skipif(
    os.environ.get("SWARMQ_RUN_DISK_TESTS") != "1",
    reason=("set SWARMQ_RUN_DISK_TESTS=1 to enable — flaky in shared CI (F4 disk-pressure)"),
)


# Large but not pathological payload so we reach 20 MB in O(100) SETs.
_BIG_VALUE = b"x" * (256 * 1024)  # 256 KiB per key
_MAX_FILL_KEYS = 400  # 400 * 256 KiB = 100 MiB — well past 20 MiB limit


@pytest.fixture
def oom_valkey() -> Iterator[str]:
    """Spin up a throwaway Valkey with a tight ``maxmemory`` cap.

    Uses a single-shot ``docker run -d --rm`` rather than the
    ``chaos_stack`` fixture because we want a dedicated container with
    a hard memory limit and ``noeviction`` policy — chaos_stack's
    Valkey is AOF-on, no maxmemory, suitable for network tests but not
    for OOM tests. Yields a ``valkey://127.0.0.1:PORT/0`` URL.
    """
    if subprocess.run(["docker", "info"], capture_output=True, check=False).returncode != 0:
        pytest.skip("docker not available — disk-pressure tests need a daemon")

    container = f"swarmq-oom-vkey-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    run = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container,
            "-p",
            f"{port}:6379",
            "valkey/valkey:8-alpine",
            "valkey-server",
            "--maxmemory",
            "20mb",
            "--maxmemory-policy",
            "noeviction",
            "--save",
            "",  # no RDB snapshots — memory accounting is cleaner
            "--appendonly",
            "no",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if run.returncode != 0:
        pytest.skip(f"could not start constrained valkey container: {run.stderr[-300:] if run.stderr else 'no stderr'}")

    url = f"valkey://127.0.0.1:{port}/0"
    try:
        _wait_port("127.0.0.1", port, time.monotonic() + 20)
        # Give valkey-server an extra moment after the port opens so
        # the first SET doesn't race with boot.
        time.sleep(0.3)
        yield url
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container],
            capture_output=True,
            timeout=15,
            check=False,
        )


async def _fill_until_oom(
    backend: ValkeyBackend,
    *,
    max_keys: int = _MAX_FILL_KEYS,
) -> list[str]:
    """Write ``k_$i`` keys with a large value until the backend raises.

    Returns the list of keys written up to (but not including) the
    key that triggered OOM. The caller uses this list to DEL in the
    recovery test.
    """
    assert backend._client is not None
    written: list[str] = []
    for i in range(max_keys):
        key = f"k_{i}"
        try:
            # No TTL so keys accumulate toward the maxmemory cap.
            await backend._client.set(key, _BIG_VALUE)
        except Exception:
            # Filler SET hit OOM — the recovery test needs the keys
            # written so far. Bubble up to the test.
            raise
        written.append(key)
    # Reached max_keys without OOM — the cap isn't being enforced as
    # expected. Fail loudly so the test doesn't silently become a no-op.
    raise AssertionError(
        f"filled {max_keys} keys (~{max_keys * len(_BIG_VALUE) // 1024} KiB) "
        f"without hitting OOM; check --maxmemory / --maxmemory-policy"
    )


class TestDiskPressure:
    """F4 — disk / memory exhaustion behaviour."""

    async def test_valkey_oom_surfaces_as_backend_connection_error(
        self,
        oom_valkey: str,
    ) -> None:
        """OOM on store_result raises BackendConnectionError, not silent.

        Fills the tight-memory Valkey until a SET raises, then calls
        ``store_result`` and asserts that the OOM ``ResponseError``
        propagates as ``BackendConnectionError``.
        """
        backend = ValkeyBackend(oom_valkey)
        await backend.connect()
        try:
            # Prime the backend to near-OOM via direct SETs.
            # valkey-py raises ResponseError("OOM ...") which is a
            # ValkeyError subclass. We accept any ValkeyError here with an
            # OOM-flavoured message; the real contract check is on
            # store_result below.
            with pytest.raises(Exception, match=r"OOM|(?i:memory)"):
                await _fill_until_oom(backend)

            # Now the user-facing path: store_result must surface
            # this as BackendConnectionError (not Valkey ResponseError
            # leaking through, not silent success).
            with pytest.raises(BackendConnectionError) as bce:
                await backend.store_result(
                    task_id="t-oom-probe",
                    data=_BIG_VALUE,
                    ttl_ms=60_000,
                )
            assert "OOM" in str(bce.value) or "memory" in str(bce.value).lower(), (
                f"BackendConnectionError should mention OOM/memory: {bce.value}"
            )
        finally:
            await backend.close()

    @pytest.mark.skip(
        reason=("requires custom RabbitMQ config (disk_free_limit / memory_high_watermark); deferred — documented stub")
    )
    async def test_rabbitmq_disk_alarm_publisher_blocks(self) -> None:
        """RabbitMQ disk alarm blocks publishers cleanly.

        Deferred: triggering RMQ's disk alarm reliably in CI needs
        either a custom ``rabbitmq.conf`` with ``disk_free_limit``
        pushed above actual free disk, or runtime control-channel
        calls (``rabbitmqctl set_disk_free_limit``) inside the
        container. Neither is in scope for U2.4.

        Expected behaviour when wired up:
        * Producer ``publish_confirm`` hangs / returns False under
          alarm (blocked), never silently drops.
        * On alarm-clear, the producer resumes without restart.
        """

    async def test_recovery_after_freeing_memory(
        self,
        oom_valkey: str,
    ) -> None:
        """After DELing filler keys the backend accepts writes again."""
        backend = ValkeyBackend(oom_valkey)
        await backend.connect()
        try:
            # Fill until OOM and capture the filler keys.
            assert backend._client is not None
            written: list[str] = []
            try:
                await _fill_until_oom(backend)
            except Exception:
                # Re-read the keys we wrote by scanning — the helper
                # raises before returning, so we need to recover the
                # filler set from the server itself.
                written.extend(
                    [
                        key.decode() if isinstance(key, bytes) else key
                        async for key in backend._client.scan_iter(
                            match="k_*",
                            count=200,
                        )
                    ]
                )

            assert written, "expected at least one filler key present after OOM-fill"

            # Sanity: while full, a fresh store_result fails.
            with pytest.raises(BackendConnectionError):
                await backend.store_result(
                    task_id="t-pre-recovery",
                    data=_BIG_VALUE,
                    ttl_ms=60_000,
                )

            # Free memory by deleting the filler keys — do it in
            # batches to avoid huge pipelines.
            for i in range(0, len(written), 100):
                batch = written[i : i + 100]
                await backend._client.delete(*batch)

            # Give Valkey a breath to finalise memory accounting.
            # (noeviction checks against used_memory at command entry.)
            time.sleep(0.1)  # noqa: ASYNC251 - kurze Pause fuer Valkeys Speicherabrechnung, kein nebenlaeufiger Task wird blockiert

            # Recovery: store_result now succeeds.
            await backend.store_result(
                task_id="t-post-recovery",
                data=b"ok",
                ttl_ms=60_000,
            )
            value = await backend.get_result("t-post-recovery")
            assert value == b"ok", f"expected b'ok' after recovery, got {value!r}"
        finally:
            await backend.close()
