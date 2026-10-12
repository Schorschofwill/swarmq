"""Optionen von ``swarmq worker`` einmal aufloesen: CLI > Env > Default.

Der Supervisor loest auf und reicht die Werte ueber ``WorkerConfig`` an
seine Kinder; ``SwarmQConfig`` laesst die Env-Overrides fuer diese Felder
dann aus (``resolved_fields``). So sehen Gruppenschluessel, Autoscale-
Sampler, Pool-Wahl, Portbereiche und Worker dieselben Werte -- vorher schlug
im Worker die Umgebung die Option.
"""

from __future__ import annotations

import argparse
import copy
from collections.abc import Mapping
from typing import Any

from swarmq.config import SwarmQConfig, parse_env_value

#: (Attribut in ``args``, Feld in ``SwarmQConfig``, Default ohne Flag und Env).
_OPTIONS: tuple[tuple[str, str, object], ...] = (
    ("queues", "queues", ["default"]),
    ("concurrency", "concurrency", None),
    ("burst", "burst_mode", False),
    ("burst_idle_timeout", "burst_idle_timeout", 5.0),
    ("priorities", "priorities", False),
    ("metrics_port", "metrics_port", None),
    ("health_port", "health_port", None),
    ("scheduler", "scheduler", "on"),
)


def _env_spec(config_field: str) -> tuple[str, str]:
    """Env-Name und Parser-Art des Felds, aus der Override-Tabelle."""
    for env_name, field_name, kind in SwarmQConfig._ENV_OVERRIDES:
        if field_name == config_field:
            return env_name, kind
    raise KeyError(config_field)  # pragma: no cover - Tabellenfehler


def resolve_worker_options(args: argparse.Namespace, env: Mapping[str, str]) -> None:
    """Schreibt die aufgeloesten Werte nach ``args`` zurueck.

    ``args.cli_fields`` nennt danach die Optionen, die als Flag kamen --
    ``--check`` braucht die Herkunft fuer seine Meldungen.
    """
    if getattr(args, "cli_fields", None) is not None:
        return  # schon aufgeloest; ein zweiter Lauf hielte Env-Werte fuer Flags
    cli_fields: set[str] = set()
    for attr, config_field, default in _OPTIONS:
        if getattr(args, attr, None) is not None:
            cli_fields.add(attr)
            continue
        env_name, kind = _env_spec(config_field)
        raw = env.get(env_name, "")
        # Kopie, damit kein Aufrufer die Default-Liste der Tabelle veraendert.
        setattr(args, attr, parse_env_value(kind, raw, env_name) if raw else copy.copy(default))
    args.cli_fields = frozenset(cli_fields)


def cli_value(args: argparse.Namespace, attr: str) -> Any:
    """Der Wert von ``attr``, wenn er als Flag kam, sonst ``None``."""
    return getattr(args, attr, None) if attr in getattr(args, "cli_fields", ()) else None


def worker_config_kwargs(args: argparse.Namespace) -> dict[str, object]:
    """Die aufgeloesten Optionen als ``SwarmQConfig``-kwargs samt
    ``resolved_fields``, damit das Env sie im Worker nicht ueberschreibt.

    ``None`` bleibt weg: ein nicht aufgeloester Wert (Aufrufer ohne
    Supervisor) faellt auf Default und Env zurueck wie eingebettet.
    """
    pairs = [(config_field, getattr(args, attr, None)) for attr, config_field, _ in _OPTIONS]
    pairs.append(("pool", getattr(args, "pool", None)))
    kwargs: dict[str, object] = {name: value for name, value in pairs if value is not None}
    kwargs["resolved_fields"] = tuple(sorted(kwargs))
    return kwargs
