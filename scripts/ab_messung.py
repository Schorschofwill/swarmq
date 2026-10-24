"""A/B-Messung zweier Refs (Plan 2026-10-24-1118, KTD12, R5).

Aufruf: ``uv run just ab --a <ref> --b <ref> --befehl <befehl> --kennzahl
<schlüssel> --besser groesser|kleiner --ergebnis <datei.json> [--paare N]``.
Wertungsläufe nur auf der Referenzmaschine (celsius); Laptop-Läufe sind
Probeläufe.

Ablauf:

- Beide Refs kommen in getrennte git-Worktrees unter einem temporären
  Verzeichnis; sie werden am Ende immer entfernt, auch nach einem Fehler.
- Je Seite ein Profillauf mit ``py-spy record --subprocesses``; die Profile
  liegen neben der Ergebnisdatei (``<name>-profil-a.svg``). Der Lauf wärmt
  beide Seiten zugleich vor und zählt nicht als Messung. Scheitert py-spy
  (etwa weil der Befehl zu kurz läuft), lautet ein Gewinn „nicht belegt“.
- Der Messbefehl läuft per ``bash -c`` im Worktree, abwechselnd A, B, A, B,
  mindestens 10 Paare. Die letzte nicht leere Zeile seiner Standardausgabe
  ist ein JSON-Objekt; ``--kennzahl`` nennt den Schlüssel, Ebenen mit Punkt
  (``latenz.p99``).
- Messbefehl und Profillauf sehen das venv nicht, unter dem ``just ab``
  läuft (``VIRTUAL_ENV`` und dessen ``bin`` im ``PATH``): Ein bloßes
  ``python`` träfe sonst auf beiden Seiten den Haupt-Checkout.
- Ist der Unterschied nach den ersten Paaren nicht gesichert (p ≥ 0,05),
  wird einmal auf 30 Paare erweitert, und dieses Ergebnis gilt.
- Statistik und Urteil: ``scripts/ab_statistik.py``. Die Ergebnisdatei hält
  Refs, Commits, Rohwerte, U, p, Verschiebung und Urteil. Ein Gewinn unter
  5 % zählt nur, wenn der Code einfacher wird (R5, AE2); das entscheidet
  das Review, nicht das Werkzeug.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from ab_statistik import ALPHA, Richtung, auswerten, mann_whitney_u

_MIN_PAARE = 10
_ERWEITERT = 30


class _AbbruchError(Exception):
    pass


def _paare(text: str) -> int:
    paare = int(text)
    if paare < _MIN_PAARE:
        msg = f"mindestens {_MIN_PAARE} Paare (R5), nicht {paare}"
        raise argparse.ArgumentTypeError(msg)
    return paare


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout.strip()


def _kennzahl(ausgabe: str, schluessel: str) -> float:
    """Wert unter ``schluessel`` (Punkte trennen Ebenen) aus der letzten nicht leeren Zeile."""
    wert = json.loads(ausgabe.strip().splitlines()[-1])
    for teil in schluessel.split("."):
        wert = wert[teil]
    return float(wert)


def _umgebung() -> dict[str, str]:
    """Umgebung ohne das äußere venv: ``just ab`` läuft unter ``uv run``.

    Sonst träfe ein bloßes ``python`` im Messbefehl auf beiden Seiten den Interpreter und das
    editierbare swarmq des Haupt-Checkouts statt des Worktrees.
    """
    umgebung = dict(os.environ)
    venv = umgebung.pop("VIRTUAL_ENV", None)
    if venv is not None:
        bin_dir = Path(venv) / "bin"
        pfade = umgebung.get("PATH", "").split(os.pathsep)
        umgebung["PATH"] = os.pathsep.join(pfad for pfad in pfade if Path(pfad) != bin_dir)
    return umgebung


def _lauf(befehl: str, worktree: Path, schluessel: str) -> float:
    lauf = subprocess.run(
        ["bash", "-c", befehl], cwd=worktree, env=_umgebung(), capture_output=True, text=True, check=False
    )
    if lauf.returncode != 0:
        msg = f"Messbefehl in {worktree} endete mit {lauf.returncode}:\n{lauf.stderr.strip()}"
        raise _AbbruchError(msg)
    return _kennzahl(lauf.stdout, schluessel)


def _profil(befehl: str, worktree: Path, ziel: Path) -> bool:
    # py-spy stammt aus dem äußeren venv (Gruppe ``messung``): vor dem Bereinigen auflösen.
    lauf = subprocess.run(
        [
            shutil.which("py-spy") or "py-spy",
            "record",
            "--subprocesses",
            "--output",
            str(ziel),
            "--",
            "bash",
            "-c",
            befehl,
        ],
        cwd=worktree,
        env=_umgebung(),
        capture_output=True,
        text=True,
        check=False,
    )
    if lauf.returncode == 0 and ziel.exists():
        return True
    print(f"ab_messung: kein Profil in {worktree}:\n{lauf.stderr.strip()}", file=sys.stderr)
    return False


def _messreihe(
    befehl: str, kennzahl: str, worktrees: dict[str, Path], werte: dict[str, list[float]], paare: int
) -> None:
    """``paare`` Läufe je Seite, abwechselnd A, B, A, B."""
    for _ in range(paare):
        for seite, worktree in worktrees.items():
            werte[seite].append(_lauf(befehl, worktree, kennzahl))


def _messen(args: argparse.Namespace, tmp: Path) -> dict[str, object]:
    """Worktrees anlegen, profilieren, messen, auswerten; die Worktrees werden immer entfernt."""
    seiten = {"a": args.a, "b": args.b}
    werte: dict[str, list[float]] = {"a": [], "b": []}
    profile = {seite: args.ergebnis.with_name(f"{args.ergebnis.stem}-profil-{seite}.svg") for seite in seiten}
    worktrees = {seite: tmp / seite for seite in seiten}
    try:
        for seite, ref in seiten.items():
            _git("worktree", "add", "--detach", str(worktrees[seite]), ref)
        commits = {seite: _git("-C", str(worktrees[seite]), "rev-parse", "HEAD") for seite in seiten}
        # Beide Seiten immer: Der Profillauf wärmt zugleich jede Seite gleich vor (etwa ``uv run``).
        profiliert = all([_profil(args.befehl, worktrees[seite], profile[seite]) for seite in seiten])  # noqa: C419 - beide Seiten, kein Kurzschluss
        paare = args.paare
        _messreihe(args.befehl, args.kennzahl, worktrees, werte, paare)
        # KTD12: ungesichert nach den ersten Paaren, einmal auf 30 erweitern; dieses Ergebnis gilt.
        erweitert = paare < _ERWEITERT and mann_whitney_u(werte["a"], werte["b"]).p >= ALPHA
        if erweitert:
            _messreihe(args.befehl, args.kennzahl, worktrees, werte, _ERWEITERT - paare)
            paare = _ERWEITERT
    finally:
        for worktree in worktrees.values():
            subprocess.run(["git", "worktree", "remove", "--force", str(worktree)], capture_output=True, check=False)
    auswertung = auswerten(werte["a"], werte["b"], besser=args.besser, profile=profiliert)
    return {
        "ref_a": args.a,
        "ref_b": args.b,
        "commit_a": commits["a"],
        "commit_b": commits["b"],
        "befehl": args.befehl,
        "kennzahl": args.kennzahl,
        "besser": str(args.besser),
        "paare": paare,
        "erweitert": erweitert,
        "werte_a": werte["a"],
        "werte_b": werte["b"],
        "u": auswertung.u,
        "p": auswertung.p,
        "verschiebung": auswertung.verschiebung,
        "relativ": auswertung.relativ,
        "profile": {seite: str(pfad) for seite, pfad in profile.items()} if profiliert else None,
        "urteil": str(auswertung.urteil),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="ab_messung", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--a", required=True, help="Ref der Vergleichsseite (vorher)")
    parser.add_argument("--b", required=True, help="Ref der geänderten Seite (nachher)")
    parser.add_argument("--befehl", required=True, help="Messbefehl, läuft per bash im Worktree der Seite")
    parser.add_argument(
        "--kennzahl", required=True, help="Schlüssel im JSON der letzten Ausgabezeile, Ebenen mit Punkt"
    )
    parser.add_argument(
        "--besser", type=Richtung, choices=list(Richtung), required=True, help="welche Richtung besser ist"
    )
    parser.add_argument("--ergebnis", type=Path, required=True, help="Ergebnisdatei (JSON); Profile daneben")
    parser.add_argument("--paare", type=_paare, default=_MIN_PAARE, help="Paare vor der Auswertung (mindestens 10)")
    args = parser.parse_args(argv)
    # py-spy läuft im Worktree: relative Pfade dort würden mit ihm gelöscht.
    args.ergebnis = args.ergebnis.absolute()
    with tempfile.TemporaryDirectory(prefix="ab-messung-") as tmp:
        try:
            ergebnis = _messen(args, Path(tmp))
        except _AbbruchError as fehler:
            print(f"ab_messung: {fehler}", file=sys.stderr)
            return 1
    args.ergebnis.write_text(json.dumps(ergebnis, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"ab_messung: {ergebnis['urteil']} (p = {ergebnis['p']:.4f}, Verschiebung {ergebnis['relativ']:+.1%})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
