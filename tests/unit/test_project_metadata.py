"""Paket-Metadaten für PyPI: Klassifikatoren, Schlagworte, Autor.

Testliste:
# [x] jeder Klassifikator steht in der offiziellen Liste (trove-classifiers)
# [x] Python 3.14, Typed, AsyncIO und ein Entwicklungsstand sind angegeben
# [x] keine Lizenz-Klassifikatoren: die Lizenz steht als SPDX-Ausdruck in ``license`` (PEP 639)
# [x] Schlagworte und Autor sind gesetzt
# [x] aiormq liegt in [7.2.4, 7.3), aio-pika in [10.2.1, 10.3), und uv.lock liegt innerhalb der Grenzen
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from packaging.requirements import Requirement
from trove_classifiers import classifiers as known

_ROOT = Path(__file__).resolve().parents[2]


def _project() -> dict[str, Any]:
    project: dict[str, Any] = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    return project


def test_every_classifier_is_an_official_one() -> None:
    classifiers = _project()["classifiers"]

    assert classifiers
    assert [c for c in classifiers if c not in known] == []


def test_the_classifiers_name_python_typing_asyncio_and_a_status() -> None:
    classifiers = _project()["classifiers"]

    assert "Programming Language :: Python :: 3.14" in classifiers
    assert "Typing :: Typed" in classifiers
    assert "Framework :: AsyncIO" in classifiers
    assert any(c.startswith("Development Status :: ") for c in classifiers)
    assert not any(c.startswith("License :: ") for c in classifiers)


def test_keywords_and_author_are_set() -> None:
    project = _project()

    assert "task-queue" in project["keywords"]
    assert project["authors"] == [{"name": "Georg Stricker"}]


def test_aiormq_and_aio_pika_are_bounded_to_the_tested_minor() -> None:
    """Untergrenzen: erst aiormq 7.2.4 und aio-pika 10.2.1 bringen die
    Korrekturen, auf die swarmq sich verlässt (Sperre mit erneuter
    Schließprüfung, ``sent`` in ``basic_publish``). Obergrenzen beim nächsten
    Minor, weil swarmq an privaten Interna hängt (docs/plans/
    2026-10-29-1945-chore-aiormq-7-2-anheben-plan.md, KTD8). aio-pika
    erlaubt ``aiormq<8``, darum braucht aiormq eine eigene Grenze."""
    anforderungen = {r.name: r for r in map(Requirement, _project()["dependencies"])}
    lock = tomllib.loads((_ROOT / "uv.lock").read_text(encoding="utf-8"))
    gesperrt = {p["name"]: p["version"] for p in lock["package"] if "version" in p}

    assert not anforderungen["aiormq"].specifier.contains("7.2.3")
    assert not anforderungen["aiormq"].specifier.contains("7.3.0")
    assert not anforderungen["aio-pika"].specifier.contains("10.2.0")
    assert not anforderungen["aio-pika"].specifier.contains("10.3.0")
    assert anforderungen["aiormq"].specifier.contains(gesperrt["aiormq"])
    assert anforderungen["aio-pika"].specifier.contains(gesperrt["aio-pika"])
