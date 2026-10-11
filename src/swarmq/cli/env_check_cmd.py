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
- ``queues`` (verwaiste Queues, fehlende ``.XQ``) kommt mit Plan 0.12.0, MR 18
  (BZ-14) dazu: ein weiterer Eintrag in ``_SECTIONS``.

Exit-Codes: 0 ohne Fehler (Warnungen erlaubt), 78, wenn ein Worker mit dieser
Umgebung den Start verweigern wuerde (verdraengende Policy unter ``strict``),
1 bei Laufzeitfehlern (Backend nicht erreichbar, ``SWARMQ_BACKEND_URL`` fehlt).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import fields
from typing import Any

from swarmq.exceptions import ConfigurationError
from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

#: Mindestabstand der Gnadenfrist zu ``shutdown_timeout`` (BZ-18-Entscheidung).
GRACE_RESERVE_S = 15
#: Kleinste unterstuetzte Valkey-Hauptversion.
MIN_VALKEY_MAJOR = 8


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
        described["unprefixed_keys"] = (
            await count_keys(backend.client, KeySpace(None).stem_pattern()) if backend.namespace else None
        )
        return described
    finally:
        await backend.close()


def _valkey_section(url: str, described: dict[str, Any], mode: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
    from swarmq.logging import sanitize_url

    section = {"url": sanitize_url(url), "eviction_check": mode, "namespace": None, "unprefixed_keys": None}
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
    if count == 0:
        return []
    amount = f"at least {count}" if count >= COUNT_KEYS_LIMIT else str(count)
    plain = KeySpace(None).stem_pattern()
    return [
        _finding(
            "valkey",
            "warning",
            "unprefixed_keys",
            f"{amount} keys without namespace ({plain}) exist next to namespace {section['namespace']!r}: apps "
            "without SWARMQ_NAMESPACE share this Valkey, or data from before the namespace was set; the "
            f"namespace does not migrate keys, so schedules, results and locks under {plain} are not seen",
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


def build_report(url: str, described: dict[str, Any], mode: str) -> dict[str, Any]:
    """Bericht aus den gelesenen Werten (rein, ohne I/O)."""
    valkey_section, findings = _valkey_section(url, described, mode)
    shutdown_section, shutdown_findings = _shutdown_section()
    findings += shutdown_findings
    ok = not any(f["level"] == "error" for f in findings)
    return {"ok": ok, "valkey": valkey_section, "shutdown": shutdown_section, "findings": findings}


def _render_table(report: dict[str, Any]) -> str:
    lines = []
    for name in ("valkey", "shutdown"):
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
        report = build_report(url, described, env_valkey_eviction_check())
    except Exception as exc:
        sys.stderr.write(f"swarmq check: {exc}\n")
        return 1
    if args.format == "json":
        sys.stdout.write(json.dumps(report, indent=2) + "\n")
    else:
        sys.stdout.write(_render_table(report) + "\n")
    return 0 if report["ok"] else CONFIG_ERROR_EXIT_CODE
