# ruff: noqa: INP001  # eigenständiges Skript neben den Logos, kein Paket
"""Logo-Set für SwarmQ / SwarmV / Loadsuite nach der Navy-Gold-Vorlage.

Gemeinsames Gerüst: konzentrische Bögen (durchgezogen und gestrichelt) mit
Ring-Knoten wie auf einer Platine. Das Projekt zeigt sich am Linienbündel:
Q-Schweif (SwarmQ), V aus zwei Bündeln (SwarmV), Tacho-Zeiger (Loadsuite).
Wortmarke: Outfit Bold als Pfade, neutraler Teil Navy, Kennung Gold.

Aufruf (Outfit als variable Schrift von Google Fonts, OFL):
    uv run --no-project --with fonttools --with uharfbuzz \\
        python gen_logos.py <Outfit[wght].ttf> <ausgabeordner>
"""

import io
import math
import sys
from pathlib import Path

import uharfbuzz as hb
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont

FONT = Path(sys.argv[1])
OUT = Path(sys.argv[2])

NAVY = "#1F2B47"
LIGHT = "#E8ECF4"  # ersetzt Navy auf dunklem Grund
GOLD_STOPS = (("0", "#B4842F"), ("0.5", "#E8C766"), ("1", "#BF8F36"))

HALF_TURN = 180
C = 100.0  # Mittelpunkt im 200er-Raster
SW = 9.0  # Strichstärke aller Bögen und Linien
NODE = 7.0  # Radius der Ring-Knoten
NODE_SW = SW * 0.6  # Strichstärke der Ring-Knoten
CLEAR = 4.0  # Mindestabstand zwischen Knoten und Bogenende
DASH, DASH_GAP = 14.0, 20.0  # Strich und kleinste Lücke (ohne runde Kappen)
MIN_DASHES = 2


# --------------------------------------------------------------------------
# Geometrie


def pt(r, a):
    t = math.radians(a)
    return C + r * math.cos(t), C + r * math.sin(t)


def arc_d(r, a0, a1):
    if a1 < a0:
        a1 += 360
    x0, y0 = pt(r, a0)
    x1, y1 = pt(r, a1)
    large = 1 if a1 - a0 > HALF_TURN else 0
    return f"M{x0:.2f},{y0:.2f} A{r},{r} 0 {large},1 {x1:.2f},{y1:.2f}"


def dash_pattern(r, a0, a1):
    """Lücke so strecken, dass der Bogen mit einem ganzen Strich endet."""
    length = r * math.radians((a1 - a0) % 360)
    n = math.floor((length + DASH_GAP) / (DASH + DASH_GAP))
    if n < MIN_DASHES:  # zu kurz für zwei volle Striche: zwei kürzere
        return f"{(length - DASH_GAP) / MIN_DASHES:.2f} {DASH_GAP}"
    return f"{DASH} {(length - n * DASH) / (n - 1):.2f}"


def arc(r, a0, a1, col, dash=False, node0=False, node1=False):
    """Bogen im Uhrzeigersinn von a0 nach a1 (Grad, 0 = rechts, 90 = unten).

    node0/node1 setzen einen Ring-Knoten genau auf a0 bzw. a1; der Bogen
    endet entsprechend früher.
    """
    # Bogenende samt runder Kappe bleibt CLEAR vor dem Knotenrand.
    gap = math.degrees((SW / 2 + NODE + NODE_SW / 2 + CLEAR) / r)
    parts = []
    s, e = a0 + (gap if node0 else 0), a1 - (gap if node1 else 0)
    extra = f' stroke-dasharray="{dash_pattern(r, s, e)}"' if dash else ""
    parts.append(
        f'<path d="{arc_d(r, s, e)}" fill="none" stroke="{col}" stroke-width="{SW}" stroke-linecap="round"{extra}/>'
    )
    for flag, a in ((node0, a0), (node1, a1)):
        if flag:
            x, y = pt(r, a)
            parts.append(
                f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{NODE}" fill="none" '
                f'stroke="{col}" stroke-width="{NODE_SW:.2f}"/>'
            )
    return "".join(parts)


def line(x0, y0, x1, y1, col):
    return (
        f'<path d="M{x0:.2f},{y0:.2f} L{x1:.2f},{y1:.2f}" stroke="{col}" stroke-width="{SW}" stroke-linecap="round"/>'
    )


def bundle(angle, spans, cols, spacing=13.0, origin=(C, C)):
    """Parallele Linien entlang `angle`; spans = [(t0, t1)] je Linie."""
    t = math.radians(angle)
    dx, dy = math.cos(t), math.sin(t)
    nx, ny = -dy, dx
    n = len(spans)
    out = []
    for i, ((t0, t1), col) in enumerate(zip(spans, cols, strict=True)):
        off = (i - (n - 1) / 2) * spacing
        ox, oy = origin[0] + nx * off, origin[1] + ny * off
        out.append(line(ox + dx * t0, oy + dy * t0, ox + dx * t1, oy + dy * t1, col))
    return "".join(out)


# --------------------------------------------------------------------------
# Zeichen. ink = Navy-Farbe, gold = Gold-Farbe (Verlauf oder Vollton).


def mark_swarmq(ink, gold):
    rings = [
        arc(84, 120, 160, ink, dash=True),
        arc(84, 172, 270, ink, node1=True),
        arc(84, 284, 330, gold),
        arc(84, 72, 108, gold, dash=True),
        arc(84, 0, 24, gold, dash=True),
        arc(66, 150, 290, gold),
        arc(66, 304, 352, gold, node1=True),
        arc(66, 72, 136, ink),
        arc(48, 110, 240, ink),
        arc(48, 258, 296, ink, node1=True),
        arc(48, 318, 8, gold, dash=True),
        arc(30, 108, 212, gold),
        arc(30, 238, 330, ink, node1=True),
    ]
    tail = bundle(45, [(34, 108), (12, 114), (26, 98)], [gold, gold, ink], spacing=16)
    return "".join(rings) + tail


def mark_swarmv(ink, gold):
    # V aus zwei Bündeln, die unten in der Mitte zusammenlaufen.
    rings = [
        arc(84, 326, 12, ink),
        arc(84, 26, 56, gold, dash=True),
        arc(84, 70, 110, ink, node0=True, node1=True),
        arc(84, 124, 154, gold, dash=True),
        arc(84, 168, 214, ink),
        arc(66, 24, 156, gold),
        arc(66, 172, 205, gold, node1=True),
        arc(66, 335, 8, gold, node0=True),
        arc(48, 54, 126, ink),
        arc(48, 156, 192, ink, dash=True),
        arc(48, 348, 24, ink, dash=True),
    ]
    o = (C, C + 40)
    left = bundle(-115, [(14, 112), (34, 104)], [ink, gold], spacing=16, origin=o)
    right = bundle(-65, [(34, 104), (14, 112)], [gold, ink], spacing=16, origin=o)
    return "".join(rings) + left + right


def mark_loadsuite(ink, gold):
    # Tacho: Bögen unten offen, Zeiger steht im oberen Lastbereich.
    rings = [
        arc(84, 135, 200, ink, dash=True, node0=True),
        arc(84, 212, 290, ink),
        arc(84, 340, 405, gold, node1=True),
        arc(66, 140, 240, gold),
        arc(66, 254, 282, gold, node1=True),
        arc(66, 350, 45, ink, dash=True),
        arc(48, 150, 252, ink, node1=True),
        arc(48, 5, 40, gold, dash=True),
    ]
    needle = bundle(-45, [(30, 84), (26, 100), (30, 72)], [gold, gold, ink], spacing=16)
    hub = f'<circle cx="{C}" cy="{C}" r="{NODE + 5}" fill="none" stroke="{ink}" stroke-width="{SW}"/>'
    return "".join(rings) + needle + hub


# --------------------------------------------------------------------------
# Icons für 16-48 px: zwei Bögen, dicke Striche, keine Strichelung.

ISW = 17.0


def iarc(r, a0, a1, col):
    return f'<path d="{arc_d(r, a0, a1)}" fill="none" stroke="{col}" stroke-width="{ISW}" stroke-linecap="round"/>'


def iline(angle, t0, t1, off, col, origin=(C, C)):
    t = math.radians(angle)
    dx, dy = math.cos(t), math.sin(t)
    ox, oy = origin[0] - dy * off, origin[1] + dx * off
    return (
        f'<path d="M{ox + dx * t0:.2f},{oy + dy * t0:.2f} '
        f'L{ox + dx * t1:.2f},{oy + dy * t1:.2f}" stroke="{col}" '
        f'stroke-width="{ISW}" stroke-linecap="round"/>'
    )


def icon_swarmq(ink, gold):
    return "".join(
        [
            iarc(80, 100, 285, ink),
            iarc(80, 305, 15, gold),
            iarc(42, 130, 335, gold),
            iline(45, 20, 100, -10, gold),
            iline(45, 52, 92, 13, ink),
        ]
    )


def icon_swarmv(ink, gold):
    o = (C, C + 38)
    return "".join(
        [
            iarc(80, 20, 160, ink),
            iline(-115, 0, 104, 0, ink, o),
            iline(-65, 0, 104, 0, gold, o),
        ]
    )


def icon_loadsuite(ink, gold):
    hub = f'<circle cx="{C}" cy="{C}" r="13" fill="{ink}"/>'
    return "".join(
        [
            iarc(80, 135, 290, ink),
            iarc(80, 338, 405, gold),
            iline(-45, 0, 96, 0, gold),
            hub,
        ]
    )


ICONS = {"swarmq": icon_swarmq, "swarmv": icon_swarmv, "loadsuite": icon_loadsuite}


MARKS = {"swarmq": mark_swarmq, "swarmv": mark_swarmv, "loadsuite": mark_loadsuite}
WORDS = {"swarmq": ("Swarm", "Q"), "swarmv": ("Swarm", "V"), "loadsuite": ("Load", "suite")}
TITLES = {"swarmq": "SwarmQ", "swarmv": "SwarmV", "loadsuite": "Loadsuite"}


# --------------------------------------------------------------------------
# Wortmarke


class Wordmark:
    def __init__(self, font_path):
        tt = instantiateVariableFont(TTFont(font_path), {"wght": 700})
        data = io.BytesIO()
        tt.save(data)
        self.hbfont = hb.Font(hb.Face(hb.Blob(data.getvalue())))
        self.gs = tt.getGlyphSet()
        self.upm = tt["head"].unitsPerEm
        self.order = tt.getGlyphOrder()
        self.cap = tt["OS/2"].sCapHeight

    def layout(self, parts, size):
        """Pfade je Teil (Ursprung links, Grundlinie y=0) und Gesamtbreite."""
        buf = hb.Buffer()
        buf.add_str("".join(parts))
        buf.guess_segment_properties()
        hb.shape(self.hbfont, buf, {"kern": True})
        owner = [i for i, p in enumerate(parts) for _ in p]
        s = size / self.upm
        paths = {i: [] for i in range(len(parts))}
        x = 0
        for info, pos in zip(buf.glyph_infos, buf.glyph_positions, strict=True):
            pen = SVGPathPen(self.gs)
            self.gs[self.order[info.codepoint]].draw(TransformPen(pen, (s, 0, 0, -s, (x + pos.x_offset) * s, 0)))
            paths[owner[info.cluster]].append(pen.getCommands())
            x += pos.x_advance
        return paths, x * s, self.cap * s


# --------------------------------------------------------------------------
# Ausgabe


def gold_def(gid):
    stops = "".join(f'<stop offset="{o}" stop-color="{c}"/>' for o, c in GOLD_STOPS)
    return f'<linearGradient id="{gid}" x1="0" y1="0" x2="1" y2="1">{stops}</linearGradient>'


def svg(w, h, body, title, defs=""):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w:.1f} {h:.1f}" '
        f'width="{w:.1f}" height="{h:.1f}" role="img" aria-label="{title}">'
        f"<title>{title}</title><defs>{defs}</defs>{body}</svg>\n"
    )


VARIANTS = {
    # name: (navy-farbe, gold-vollton oder None für Verlauf)
    "light": (NAVY, None),
    "dark": (LIGHT, None),
    "mono-black": ("#000000", "#000000"),
    "mono-white": ("#FFFFFF", "#FFFFFF"),
}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    wm = Wordmark(FONT)
    for key, mark in MARKS.items():
        title = TITLES[key]
        for vname, (ink, solid) in VARIANTS.items():
            gid = f"gold-{key}-{vname}"
            # Verlauf auf dem Zeichen in Objektkoordinaten, gilt je Strich;
            # deshalb für Striche userSpaceOnUse über das ganze Zeichen.
            defs = ""
            if solid is None:
                stops = "".join(f'<stop offset="{o}" stop-color="{c}"/>' for o, c in GOLD_STOPS)
                defs = (
                    f'<linearGradient id="{gid}" gradientUnits="userSpaceOnUse" '
                    f'x1="14" y1="14" x2="186" y2="186">{stops}</linearGradient>' + gold_def(gid + "-t")
                )
                gold, gold_text = f"url(#{gid})", f"url(#{gid}-t)"
            else:
                gold = gold_text = solid
            body = mark(ink, gold)
            sfx = f"-{vname}"
            (OUT / f"{key}-icon{sfx}.svg").write_text(svg(200, 200, ICONS[key](ink, gold), title, defs))
            (OUT / f"{key}-mark{sfx}.svg").write_text(svg(200, 200, body, title, defs))

            # Gestapelt (wie Vorlage): Zeichen oben, Wortmarke darunter.
            size = 64
            paths, tw, cap = wm.layout(WORDS[key], size)
            fills = [ink, gold_text]
            width = max(200, tw) + 16
            mx = (width - 200) / 2
            tx, base = (width - tw) / 2, 200 + 22 + cap
            text = "".join(
                f'<path fill="{fills[i]}" transform="translate({tx:.2f},{base:.2f})" d="{" ".join(ps)}"/>'
                for i, ps in paths.items()
            )
            stacked = f'<g transform="translate({mx:.2f},0)">{body}</g>{text}'
            (OUT / f"{key}-stacked{sfx}.svg").write_text(svg(width, base + 12, stacked, title, defs))

            # Horizontal: Zeichen links, Wortmarke rechts.
            size = 92
            paths, tw, cap = wm.layout(WORDS[key], size)
            tx, base = 200 + 28, 100 + cap / 2
            text = "".join(
                f'<path fill="{fills[i]}" transform="translate({tx:.2f},{base:.2f})" d="{" ".join(ps)}"/>'
                for i, ps in paths.items()
            )
            (OUT / f"{key}-horizontal{sfx}.svg").write_text(svg(tx + tw + 8, 200, body + text, title, defs))

    (OUT / "favicon").mkdir(exist_ok=True)
    for key, icon in ICONS.items():
        stops = "".join(f'<stop offset="{o}" stop-color="{c}"/>' for o, c in GOLD_STOPS)
        defs = (
            f"<style>.s{{stroke:{NAVY}}}.f{{fill:{NAVY}}}"
            f"@media (prefers-color-scheme: dark)"
            f"{{.s{{stroke:{LIGHT}}}.f{{fill:{LIGHT}}}}}</style>"
            f'<linearGradient id="g" gradientUnits="userSpaceOnUse" x1="14" '
            f'y1="14" x2="186" y2="186">{stops}</linearGradient>'
        )
        body = icon("NAVY", "url(#g)").replace('stroke="NAVY"', 'class="s"').replace('fill="NAVY"', 'class="f"')
        (OUT / "favicon" / f"{key}-favicon.svg").write_text(svg(200, 200, body, TITLES[key], defs))


if __name__ == "__main__":
    main()
