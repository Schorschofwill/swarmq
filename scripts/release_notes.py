"""Release-Notizen einer Version aus dem Changelog.

Aufruf: ``python scripts/release_notes.py <version> [changelog]``. Gibt den
Abschnitt ``## [<version>]`` ohne Überschrift aus, bis zur nächsten
``## [``-Überschrift. ``release.yml`` legt damit das GitHub Release zu einem
Tag an. Ohne Abschnitt bricht das Skript ab: Ein Release ohne Notizen soll
auffallen, nicht leer erscheinen.

Relative Links des Changelogs (``api.md#x``, ``../guides/y.md``, ``#z``)
gelten neben ``website/reference/changelog.md``; im Release auf GitHub
fuehren sie ins Leere. Sie zeigen deshalb auf die Seiten bei Read the Docs.
"""

from __future__ import annotations

import posixpath
import re
import sys
from pathlib import Path

_CHANGELOG = Path("website/reference/changelog.md")
_DOCS = "https://swarmq.readthedocs.io/en/latest/"
# Lage des Changelogs unter website/, Grundlage relativer Links.
_PAGE_DIR = "reference"
_LINK = re.compile(r"\]\((?!https?://)([^)#]*)(#[^)]*)?\)")


def _absolute(match: re.Match[str]) -> str:
    path, anchor = match.group(1), match.group(2) or ""
    if not path:
        return f"]({_DOCS}{_PAGE_DIR}/changelog/{anchor})"
    page = posixpath.normpath(posixpath.join(_PAGE_DIR, path)).removesuffix(".md")
    page = page.removesuffix("index").rstrip("/")
    return f"]({_DOCS}{page}/{anchor})"


def notes(text: str, version: str) -> str | None:
    match = re.search(rf"(?m)^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[|\Z)", text, re.DOTALL)
    return None if match is None else _LINK.sub(_absolute, match.group(1).strip()) + "\n"


def main(argv: list[str]) -> int:
    version = argv[0]
    changelog = Path(argv[1]) if len(argv) > 1 else _CHANGELOG
    section = notes(changelog.read_text(encoding="utf-8"), version)
    if section is None:
        print(f"release_notes: kein Abschnitt für {version} in {changelog}", file=sys.stderr)
        return 1
    sys.stdout.write(section)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
