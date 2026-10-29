"""Paket-Metadaten für PyPI: Klassifikatoren, Schlagworte, Autor.

Testliste:
# [x] jeder Klassifikator steht in der offiziellen Liste (trove-classifiers)
# [x] Python 3.14, Typed, AsyncIO und ein Entwicklungsstand sind angegeben
# [x] keine Lizenz-Klassifikatoren: die Lizenz steht als SPDX-Ausdruck in ``license`` (PEP 639)
# [x] Schlagworte und Autor sind gesetzt
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

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
