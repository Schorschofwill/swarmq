"""Community-Dateien für GitHub: Sicherheit, Mitarbeit, Verhaltenskodex, Vorlagen.

GitHub wertet diese Dateien selbst aus (Community-Profil, Formular beim
Anlegen eines Issues, Text im PR-Feld). Fehler darin fallen erst auf, wenn
jemand ein Issue öffnen will; darum prüft dieser Test Form und Verweise.

Testliste:
# [x] SECURITY.md, CONTRIBUTING.md und CODE_OF_CONDUCT.md liegen im Wurzelverzeichnis
# [x] Relative Verweise in den Community-Dateien zeigen auf vorhandene Dateien
# [x] Jede Issue-Vorlage ist ein gültiges Formular mit Name, Beschreibung und eindeutigen Feld-IDs
# [x] Leere Issues sind abgeschaltet, Sicherheitsmeldungen gehen auf den privaten Meldeweg
# [x] SECURITY.md verweist auf den privaten Meldeweg und nennt keine E-Mail-Adresse
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_ADVISORY = "https://github.com/Schorschofwill/swarmq/security/advisories/new"
_COMMUNITY = [
    "SECURITY.md",
    "CONTRIBUTING.md",
    "CODE_OF_CONDUCT.md",
    ".github/pull_request_template.md",
]
_FORMS = sorted((_ROOT / ".github/ISSUE_TEMPLATE").glob("*.yml"))
_LINK = re.compile(r"\]\((?!https?://|mailto:|#)([^)#\s]+)(?:#[^)]*)?\)")


@pytest.mark.parametrize("datei", _COMMUNITY)
def test_community_file_exists(datei: str) -> None:
    assert (_ROOT / datei).is_file()


@pytest.mark.parametrize("datei", _COMMUNITY)
def test_relative_links_resolve(datei: str) -> None:
    pfad = _ROOT / datei
    ziele = _LINK.findall(pfad.read_text(encoding="utf-8"))

    fehlend = [z for z in ziele if not (pfad.parent / z).exists()]

    assert fehlend == []


def test_there_are_issue_forms() -> None:
    namen = {p.name for p in _FORMS}

    assert {"bug_report.yml", "feature_request.yml", "config.yml"} <= namen


@pytest.mark.parametrize("form", [p for p in _FORMS if p.name != "config.yml"], ids=lambda p: p.name)
def test_issue_form_is_valid(form: Path) -> None:
    daten = yaml.safe_load(form.read_text(encoding="utf-8"))

    assert daten["name"]
    assert daten["description"]
    ids = [feld["id"] for feld in daten["body"] if "id" in feld]
    assert len(ids) == len(set(ids))
    eingaben = [feld for feld in daten["body"] if feld["type"] != "markdown"]
    assert eingaben
    assert all("id" in feld and feld["attributes"]["label"] for feld in eingaben)


def test_blank_issues_are_off_and_security_goes_private() -> None:
    daten = yaml.safe_load((_ROOT / ".github/ISSUE_TEMPLATE/config.yml").read_text(encoding="utf-8"))

    assert daten["blank_issues_enabled"] is False
    assert _ADVISORY in [link["url"] for link in daten["contact_links"]]


def test_security_policy_points_to_private_reporting() -> None:
    text = (_ROOT / "SECURITY.md").read_text(encoding="utf-8")

    assert _ADVISORY in text
    assert re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", text) is None
