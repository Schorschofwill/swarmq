"""Valkey-Umgebung pruefen (Plan 0.12.0, MR 14, BZ-04).

``probe_eviction`` liest ``maxmemory-policy`` und ``maxmemory`` und bewertet
sie (``swarmq.valkey_policy``); ``describe_valkey`` liefert zusaetzlich
Version, AOF und Cluster-Modus fuer ``swarmq check``. Nur lesend
(``CONFIG GET``, ``INFO``); keine Abfrage wirft.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

from swarmq.logging import get_logger
from swarmq.valkey_policy import EvictionCheck, assess

logger = get_logger(__name__)

# Zeitlimit der ganzen Pruefung (beide Quellen): sie darf den Start nicht aufhalten.
_PROBE_TIMEOUT_S = 2.0


def _text(value: Any) -> str | None:
    if isinstance(value, bytes):
        return value.decode()
    return value if isinstance(value, str) else None


def _int(value: Any) -> int | None:
    text = _text(value) if not isinstance(value, int) else str(value)
    try:
        return int(text) if text is not None else None
    except ValueError:
        return None


async def _from_config(client: Any) -> tuple[str | None, int | None]:
    raw = await client.config_get("maxmemory*")
    values = {_text(k): v for k, v in raw.items()} if isinstance(raw, dict) else {}
    return _text(values.get("maxmemory-policy")), _int(values.get("maxmemory"))


async def _from_info(client: Any) -> tuple[str | None, int | None]:
    raw = await client.info("memory")
    values = raw if isinstance(raw, dict) else {}
    return _text(values.get("maxmemory_policy")), _int(values.get("maxmemory"))


async def _used_memory(client: Any) -> int | None:
    try:
        raw = await client.info("memory")
    except Exception:
        return None
    return _int(raw.get("used_memory")) if isinstance(raw, dict) else None


async def probe_eviction(client: Any, *, memory: bool = False) -> EvictionCheck:
    """``CONFIG GET``, sonst ``INFO memory`` (Managed-Dienste verbieten
    ``CONFIG`` oft per ACL), zusammen hoechstens ``_PROBE_TIMEOUT_S``. Mit
    *memory* zusaetzlich ``used_memory``. Wirft nie: ohne Antwort ``unknown``."""
    check = EvictionCheck(status="unknown")
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT_S):
            for source in (_from_config, _from_info):
                try:
                    policy, maxmemory = await source(client)
                except Exception:
                    continue
                if policy is not None:
                    check = EvictionCheck(status=assess(policy, maxmemory), policy=policy, maxmemory=maxmemory)
                    break
            if memory:
                check = replace(check, used_memory=await _used_memory(client))
    except TimeoutError:
        pass
    return check


def log_eviction_check(check: EvictionCheck) -> None:
    if check.status == "unsafe":
        logger.warning(
            "valkey_eviction_policy_unsafe",
            policy=check.policy,
            maxmemory=check.maxmemory,
            hint=(
                "Valkey may evict swarmq keys (locks, execution guard, unique and fired markers): "
                "tasks can run twice; set maxmemory-policy noeviction"
            ),
        )
    elif check.status == "unknown":
        logger.warning(
            "valkey_eviction_policy_unknown",
            hint=(
                "could not read maxmemory-policy (CONFIG GET and INFO memory failed or are forbidden); "
                "make sure Valkey runs with maxmemory-policy noeviction, or run 'swarmq check'"
            ),
        )


async def _info_section(client: Any, section: str) -> dict[str, Any]:
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT_S):
            raw = await client.info(section)
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _flag(value: Any) -> bool | None:
    number = _int(value)
    return None if number is None else number != 0


async def describe_valkey(client: Any) -> dict[str, Any]:
    """Policy, Speicher, Version, AOF und Cluster-Modus; unbekannte Werte
    sind ``None``."""
    check = await probe_eviction(client, memory=True)
    server = await _info_section(client, "server")
    persistence = await _info_section(client, "persistence")
    cluster = await _info_section(client, "cluster")
    return {
        "eviction": check.status,
        "maxmemory_policy": check.policy,
        "maxmemory": check.maxmemory,
        "used_memory": check.used_memory,
        "version": _text(server.get("valkey_version")) or _text(server.get("redis_version")),
        "aof_enabled": _flag(persistence.get("aof_enabled")),
        "cluster_enabled": _flag(cluster.get("cluster_enabled")),
    }


#: Obergrenze der Zaehlung in ``count_keys`` und deren Zeitlimit.
COUNT_KEYS_LIMIT = 10_000
_COUNT_TIMEOUT_S = 5.0


async def count_keys(client: Any, pattern: str, *, limit: int = COUNT_KEYS_LIMIT) -> int | None:
    """Keys zu *pattern* per ``SCAN`` zaehlen, hoechstens *limit* (dann
    *limit*), zusammen hoechstens ``_COUNT_TIMEOUT_S``. ``None``, wenn
    ``SCAN`` scheitert (ACL, Zeitlimit). Nur lesend; wirft nie."""
    total = 0
    cursor = 0
    try:
        async with asyncio.timeout(_COUNT_TIMEOUT_S):
            while True:
                cursor, keys = await client.scan(cursor, match=pattern, count=1000)
                total += len(keys)
                if total >= limit:
                    return limit
                if not cursor:
                    return total
    except Exception:
        return None
