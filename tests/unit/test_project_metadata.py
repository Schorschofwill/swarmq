"""Paket-Metadaten für PyPI: Klassifikatoren, Schlagworte, Autor.

Testliste:
# [x] jeder Klassifikator steht in der offiziellen Liste (trove-classifiers)
# [x] Python 3.14, Typed, AsyncIO und ein Entwicklungsstand sind angegeben
# [x] keine Lizenz-Klassifikatoren: die Lizenz steht als SPDX-Ausdruck in ``license`` (PEP 639)
# [x] Schlagworte und Autor sind gesetzt
# [x] aiormq ist unter 7.1 begrenzt, aio-pika unter 10.1, und uv.lock liegt innerhalb der Grenzen
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


def test_aiormq_is_capped_below_the_untested_7_1() -> None:
    """aiormq 7.1/7.2 haben ``basic_publish`` umgebaut; swarmqs Umgehungen um
    aiormq sind dagegen nicht geprüft (docs/plans/
    2026-10-29-1700-chore-abhaengigkeiten-anheben-plan.md, KTD1). aio-pika
    10.0 erlaubt ``aiormq<8``, darum braucht aiormq eine eigene Grenze."""
    anforderungen = {r.name: r for r in map(Requirement, _project()["dependencies"])}
    lock = tomllib.loads((_ROOT / "uv.lock").read_text(encoding="utf-8"))
    gesperrt = {p["name"]: p["version"] for p in lock["package"] if "version" in p}

    assert not anforderungen["aiormq"].specifier.contains("7.1.0")
    assert not anforderungen["aio-pika"].specifier.contains("10.1.0")
    assert anforderungen["aiormq"].specifier.contains(gesperrt["aiormq"])
    assert anforderungen["aio-pika"].specifier.contains(gesperrt["aio-pika"])
