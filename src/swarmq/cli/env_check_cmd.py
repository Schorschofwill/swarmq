"""``swarmq check``: die Betriebsumgebung pruefen (Plan 0.12.0, MR 14, BZ-04).

Gedacht fuer den Lauf **vor** einem Upgrade, mit der neuen swarmq-Version in
einem eigenen venv (oder als Init-Container): nur lesend, ``CONFIG GET`` und
``INFO`` gegen Valkey. Nicht zu verwechseln mit ``swarmq worker --check``
(Dry-Run des Worker-Starts, ``check_cmd.py``).

Abschnitte:

- ``valkey``: Eviction-Policy, ``maxmemory``, Speicher, AOF, Version, Cluster,
  Namensraum, Keys ohne Praefix.
- ``shutdown``: ``shutdown_timeout`` und die Mindest-Gnadenfrist fuer K8s
  (``terminationGracePeriodSeconds``) und Docker (``stop_grace_period``).
- ``queues`` (Plan 0.12.0, MR 18, BZ-14): nur mit ``SWARMQ_BROKER_URL``; die
  von Workern ab 0.12 gemeldeten Queues per passivem Declare: verwaiste Queues
  (Rueckstand, kein Consumer, kein lebender Worker meldet sie) und fehlende
  ``.XQ``. Beides sind Warnungen.

Exit-Codes: 0 ohne Fehler (Warnungen erlaubt), 78, wenn ein Worker mit dieser
Umgebung den Start verweigern wuerde (verdraengende Policy unter ``strict``),
1 bei Laufzeitfehlern (Backend oder, mit ``SWARMQ_BROKER_URL``, Broker nicht
erreichbar, ``SWARMQ_BACKEND_URL`` fehlt).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any

from swarmq.exceptions import ConfigurationError
from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

if TYPE_CHECKING:
    from swarmq.contracts.broker import QueueStatus

#: Mindestabstand der Gnadenfrist zu ``shutdown_timeout`` (BZ-18-Entscheidung).
GRACE_RESERVE_S = 15
#: Kleinste unterstuetzte Valkey-Hauptversion.
MIN_VALKEY_MAJOR = 8


@dataclass(frozen=True)
class QueueState:
    """Gelesene Werte fuer den Abschnitt ``queues`` (BZ-14): Meldungen mit
    Valkey-Zeit, das Lebendigkeitsfenster und der Zustand je Queue samt
    ``.XQ`` (``None``: fehlt). ``error`` statt Werten, wenn Valkey oder der
    Broker nicht lesbar waren."""

    now_ms: int = 0
    last_seen_ms: Mapping[str, int] | None = None
    live_window_ms: int = 0
    statuses: Mapping[str, QueueStatus | None] | None = None
    error: str | None = None


def _finding(section: str, level: str, code: str, message: str) -> dict[str, str]:
    return {"section": section, "level": level, "code": code, "message": message}


async def _describe_backend(url: str) -> dict[str, Any]:
    # Lazy: ``swarmq --help`` bleibt ohne valkey-Import (R-1a).
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.backend.valkey.environment import count_keys, describe_valkey
    from swarmq.keyspace import KeySpace

    # ``off``: die Pruefung beim Verbinden wuerde nur ins Log warnen; hier
    # landet das Ergebnis im Bericht. Der Namensraum kommt aus
    # ``SWARMQ_NAMESPACE`` (BZ-10).
    backend = ValkeyBackend(url, eviction_check="off")
    await backend.connect()
    try:
        described = await describe_valkey(backend.client)
        described["namespace"] = backend.namespace
        # Keys ohne Praefix zaehlen nur mit Namensraum etwas: Apps ohne
        # Namensraum auf demselben Valkey oder Daten von vor dem Setzen.
        counted = await count_keys(backend.client, KeySpace(None).stem_pattern()) if backend.namespace else None
        described["unprefixed_keys"] = None if counted is None else counted[0]
        described["unprefixed_keys_complete"] = None if counted is None else counted[1]
        return described
    finally:
        await backend.close()


def _seen_interval_s() -> float:
    from swarmq.config import SwarmQConfig, parse_env_value

    default = next(f.default for f in fields(SwarmQConfig) if f.name == "scheduler_seen_interval_s")
    raw = os.environ.get("SWARMQ_SCHEDULER_SEEN_INTERVAL_S", "")
    value = parse_env_value("float", raw, "SWARMQ_SCHEDULER_SEEN_INTERVAL_S") if raw else default
    assert isinstance(value, float)
    return value


async def _read_queue_state(url: str, broker_url: str) -> QueueState:
    """BZ-14: gemeldete Queues aus Valkey (Namensraum aus ``SWARMQ_NAMESPACE``)
    und ihr Zustand samt ``.XQ`` per passivem Declare; nur lesend. Sind die
    Meldungen nicht lesbar (etwa ACL), steht der Fehler im Ergebnis (Warnung).
    Ein nicht erreichbarer Broker wirft: Exit 1 wie bei Valkey (Entscheidung
    Georg 2026-10-11)."""
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker
    from swarmq.logging import sanitize_url
    from swarmq.queue_names import queue_family
    from swarmq.scheduler.reports import live_window_ms

    backend = ValkeyBackend(url, eviction_check="off")
    await backend.connect()
    try:
        reports = await backend.schedules.queue_reports()
    except Exception as exc:
        return QueueState(error=f"{type(exc).__name__}: {exc}")
    finally:
        await backend.close()
    broker = RabbitMQBroker(broker_url)
    try:
        await broker.connect()
    except Exception as exc:
        raise RuntimeError(f"broker {sanitize_url(broker_url)} is not reachable: {exc}") from exc
    try:
        names = [name for queue in sorted(reports.last_seen_ms) for name in (queue, queue_family(queue)[2])]
        statuses = await broker.queue_statuses(names)
    finally:
        await broker.close()
    return QueueState(
        now_ms=reports.now_ms,
        last_seen_ms=reports.last_seen_ms,
        live_window_ms=live_window_ms(_seen_interval_s()),
        statuses=statuses,
    )


def _valkey_section(url: str, described: dict[str, Any], mode: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
    from swarmq.logging import sanitize_url

    section = {
        "url": sanitize_url(url),
        "eviction_check": mode,
        "namespace": None,
        "unprefixed_keys": None,
        "unprefixed_keys_complete": None,
    }
    section.update(described)
    return section, _valkey_findings(section, mode) + _namespace_findings(section)


def _namespace_findings(section: dict[str, Any]) -> list[dict[str, str]]:
    """BZ-10: Keys ohne Praefix neben einem Namensraum (Warnung, Exit 0)."""
    from swarmq.backend.valkey.environment import COUNT_KEYS_LIMIT
    from swarmq.keyspace import KeySpace

    if section["namespace"] is None:
        return []
    count = section["unprefixed_keys"]
    if count is None:
        return [
            _finding(
                "valkey",
                "warning",
                "unprefixed_keys_unknown",
                "could not count keys without namespace (SCAN failed or is forbidden)",
            )
        ]
    incomplete = section["unprefixed_keys_complete"] is False
    if count == 0 and incomplete:
        return [
            _finding(
                "valkey",
                "warning",
                "unprefixed_keys_unknown",
                "could not count keys without namespace: SCAN stopped at its time limit before finding any",
            )
        ]
    if count == 0:
        return []
    amount = f"at least {count}" if count >= COUNT_KEYS_LIMIT or incomplete else str(count)
    plain = KeySpace(None).stem_pattern()
    return [
        _finding(
            "valkey",
            "warning",
            "unprefixed_keys",
            f"{amount} keys without namespace ({plain}) exist next to namespace {section['namespace']!r}: apps "
            "without SWARMQ_NAMESPACE share this Valkey, or data from before the namespace was set; the "
            f"namespace does not migrate keys, so schedules, results and locks under {plain} are not seen"
            + (" (count incomplete: SCAN stopped at its time limit)" if incomplete else "")
            + (" (cluster mode: SCAN counted one node only)" if section.get("cluster_enabled") is True else ""),
        )
    ]


def _valkey_findings(described: dict[str, Any], mode: str) -> list[dict[str, str]]:
    findings = []
    if described["eviction"] == "unsafe":
        level = "error" if mode == "strict" else "warning"
        findings.append(
            _finding(
                "valkey",
                level,
                "eviction_policy",
                f"maxmemory-policy {described['maxmemory_policy']} may evict swarmq keys (locks, execution "
                "guard, unique and fired markers) and run tasks twice; set maxmemory-policy noeviction"
                + (" (a worker refuses to start: SWARMQ_VALKEY_EVICTION_CHECK=strict)" if level == "error" else ""),
            )
        )
    if described["eviction"] == "unknown":
        findings.append(
            _finding(
                "valkey",
                "warning",
                "eviction_policy_unknown",
                "could not read maxmemory-policy (CONFIG GET and INFO memory failed or are forbidden); "
                "make sure Valkey runs with maxmemory-policy noeviction",
            )
        )
    if described["aof_enabled"] is False:
        findings.append(
            _finding(
                "valkey",
                "warning",
                "aof_disabled",
                "AOF is off: after a Valkey restart delayed tasks, CLI crons and 'schedule remove' "
                "switch-offs since the last RDB snapshot are lost; set appendonly yes",
            )
        )
    if described["cluster_enabled"] is True:
        findings.append(
            _finding(
                "valkey",
                "warning",
                "cluster",
                "Valkey runs in cluster mode, which swarmq does not support; use a single primary (with replicas)",
            )
        )
    major = _major(described["version"])
    if major is not None and major < MIN_VALKEY_MAJOR:
        findings.append(
            _finding(
                "valkey",
                "warning",
                "version",
                f"Valkey {described['version']} is older than the supported minimum {MIN_VALKEY_MAJOR}",
            )
        )
    return findings


def _major(version: str | None) -> int | None:
    head = (version or "").split(".", 1)[0]
    return int(head) if head.isdigit() else None


def _shutdown_section() -> tuple[dict[str, Any], list[dict[str, str]]]:
    from swarmq.config import SwarmQConfig, parse_env_value

    default = next(f.default for f in fields(SwarmQConfig) if f.name == "shutdown_timeout")
    raw = os.environ.get("SWARMQ_SHUTDOWN_TIMEOUT", "")
    timeout = parse_env_value("int", raw, "SWARMQ_SHUTDOWN_TIMEOUT") if raw else default
    assert isinstance(timeout, int)
    return {"shutdown_timeout": timeout, "min_grace_period_s": timeout + GRACE_RESERVE_S}, []


def _queues_section(state: QueueState | None) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """BZ-14: verwaiste Queues und fehlende ``.XQ`` (Warnungen, Exit 0)."""
    from swarmq.queue_names import queue_family
    from swarmq.scheduler.orphaned_queues import is_orphaned

    section: dict[str, Any] = {"checked": False, "reported": None, "orphaned": [], "missing_dead_letter_queues": []}
    if state is not None and state.error is not None:
        return section, [
            _finding("queues", "warning", "queues_unchecked", f"could not check the reported queues: {state.error}")
        ]
    if state is None or state.last_seen_ms is None or state.statuses is None:
        return section, []
    findings = []
    live_since = state.now_ms - state.live_window_ms
    section["checked"] = True
    section["reported"] = len(state.last_seen_ms)
    for queue, last_seen in sorted(state.last_seen_ms.items()):
        status = state.statuses.get(queue)
        dead_letter_queue = queue_family(queue)[2]
        if status is not None and state.statuses.get(dead_letter_queue) is None:
            section["missing_dead_letter_queues"].append(queue)
            findings.append(
                _finding(
                    "queues",
                    "warning",
                    "dead_letter_queue_missing",
                    f"queue {queue!r} has no dead-letter queue {dead_letter_queue!r}: failed tasks "
                    "routed there by the broker are dropped; restart a worker of this queue to declare it again",
                )
            )
        if status is None or not is_orphaned(status, last_seen_ms=last_seen, live_since_ms=live_since):
            continue
        ago = max(0, (state.now_ms - last_seen) // 1000)
        section["orphaned"].append({"queue": queue, "messages": status.message_count, "last_reported_s_ago": ago})
        findings.append(
            _finding(
                "queues",
                "warning",
                "orphaned_queue",
                f"queue {queue!r} holds {status.message_count} messages without a consumer; no live worker has "
                f"reported it for {ago} s (task moved to another queue or --queues changed?): consume or move "
                "the backlog, or delete the queue",
            )
        )
    return section, findings


def build_report(url: str, described: dict[str, Any], mode: str, *, queues: QueueState | None = None) -> dict[str, Any]:
    """Bericht aus den gelesenen Werten (rein, ohne I/O)."""
    valkey_section, findings = _valkey_section(url, described, mode)
    shutdown_section, shutdown_findings = _shutdown_section()
    findings += shutdown_findings
    queues_section, queue_findings = _queues_section(queues)
    findings += queue_findings
    ok = not any(f["level"] == "error" for f in findings)
    return {
        "ok": ok,
        "valkey": valkey_section,
        "shutdown": shutdown_section,
        "queues": queues_section,
        "findings": findings,
    }


def _render_table(report: dict[str, Any]) -> str:
    lines = []
    for name in ("valkey", "shutdown", "queues"):
        for key, value in report[name].items():
            lines.append(f"{name}  {key}  {'-' if value is None else value}")
    for finding in report["findings"]:
        lines.append(f"{finding['level'].upper()}  {finding['section']}: {finding['message']}")
    lines.append("OK" if report["ok"] else "FAILED")
    return "\n".join(lines)


def run_env_check(args: argparse.Namespace) -> int:
    """Einstieg aus ``swarmq.cli.main``; liefert den Exit-Code."""
    from swarmq.config import env_valkey_eviction_check

    try:
        url = os.environ.get("SWARMQ_BACKEND_URL", "")
        if not url:
            raise ConfigurationError(
                "SWARMQ_BACKEND_URL is not set; export it or pass it via env, "
                "e.g. SWARMQ_BACKEND_URL=valkey://localhost:6379/0"
            )
        described = asyncio.run(_describe_backend(url))
        broker_url = os.environ.get("SWARMQ_BROKER_URL", "")
        queues = asyncio.run(_read_queue_state(url, broker_url)) if broker_url else None
        report = build_report(url, described, env_valkey_eviction_check(), queues=queues)
    except Exception as exc:
        sys.stderr.write(f"swarmq check: {exc}\n")
        return 1
    if args.format == "json":
        sys.stdout.write(json.dumps(report, indent=2) + "\n")
    else:
        sys.stdout.write(_render_table(report) + "\n")
    return 0 if report["ok"] else CONFIG_ERROR_EXIT_CODE
