# SwarmQ – Logo

Teil der gemeinsamen Logo-Familie von SwarmQ, SwarmV und Loadsuite: konzentrische
Bögen in Navy und Gold mit Ring-Knoten, das Projekt erkennt man am Linienbündel
(Q-Schweif). Alle Dateien haben einen transparenten Hintergrund.

| Datei | Verwendung |
|---|---|
| `swarmq-stacked-*.svg` | Zeichen über dem Namen (Titelseite, Präsentation) |
| `swarmq-horizontal-*.svg` | Zeichen neben dem Namen (README-Kopf, Navigationsleiste) |
| `swarmq-mark-*.svg` | nur das Zeichen, ab etwa 64 px |
| `swarmq-icon-*.svg` | vereinfachtes Zeichen für 16–48 px |
| `favicon/swarmq-favicon.svg` | Favicon, schaltet im Dunkelmodus selbst auf hell |
| `favicon/swarmq-favicon.ico` | Favicon 16/32/48 px für ältere Browser |
| `favicon/swarmq-apple-touch-icon.png`, `swarmq-icon-192.png`, `swarmq-icon-512.png` | Homescreen und Web-Manifest |
| `png/` | alle SVG-Varianten als PNG |

Varianten: `light` (für helle Hintergründe), `dark` (für dunkle: Navy wird
`#E8ECF4`), `mono-black`, `mono-white`.

Farben: Navy `#1F2B47`, Gold als Verlauf `#B4842F` → `#E8C766` → `#BF8F36`.
Schrift: Outfit Bold (OFL), in Pfade umgewandelt.

Erzeugt mit `docs/assets/logo/gen_logos.py` im swarmq-Repo; die PNGs sind daraus
mit Inkscape exportiert.

```html
<link rel="icon" href="favicon/swarmq-favicon.svg" type="image/svg+xml">
<link rel="icon" href="favicon/swarmq-favicon.ico" sizes="48x48">
<link rel="apple-touch-icon" href="favicon/swarmq-apple-touch-icon.png">
```

## Wo das Logo eingebunden ist

- `README.md`: `swarmq-horizontal-light/-dark.svg` per `<picture>` (hell/dunkel).
- Doku-Website: Kopien unter `website/assets/brand/` (MkDocs braucht sie im
  `docs_dir`). Kopfzeile über `overrides/partials/logo.html` (Icon je Farbschema),
  Startseite mit `swarmq-stacked-*.svg`, Favicons über `mkdocs.yml` und
  `overrides/main.html`. Farben der Website in `website/stylesheets/swarmq.css`.
- Nach einer Änderung am Logo die Kopien in `website/assets/brand/` erneuern.

## Neu erzeugen

```bash
curl -sSLo /tmp/Outfit.ttf 'https://github.com/google/fonts/raw/main/ofl/outfit/Outfit%5Bwght%5D.ttf'
uv run --no-project --with fonttools --with uharfbuzz \
    python docs/assets/logo/gen_logos.py /tmp/Outfit.ttf /tmp/logos
```

Das Skript schreibt die SVGs aller drei Projekte (Zeichen, Icon, gestapelt,
horizontal je in vier Varianten, dazu `favicon/*.svg`). PNG und ICO entstehen
danach mit Inkscape bzw. ImageMagick.
