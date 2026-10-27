# Messung: Bündeln im Log-Listener (Oktober 2026)

Messnachweis zu `docs/plans/2026-10-27-0800-perf-log-listener-buendeln-plan.md` (U5) und zur Entscheidung E-088. Gemessen wurde das Szenario `io_bound` des Vergleichsaufbaus (`benchmarks/comparison/`): 1000 Tasks mit je 10 ms `asyncio.sleep`, Profil `stark`, Parallelität `gleich` (10), uvloop.

## Ergebnis

Auf der Referenzmaschine celsius bringt das Bündeln **+10,8 % Durchsatz**:

| Seite | Ref | Tasks/s (Median) | Spanne |
|---|---|---:|---:|
| A (vorher) | `fa646592` (main) | 380,9 | 365,0–391,7 |
| B (nachher) | `b53b3ede` | 421,0 | 406,0–437,0 |

- 10 Paare, abwechselnd A und B, mit `uv run just ab` (`scripts/ab_messung.py`).
- Mann-Whitney U = 0: Jeder B-Lauf war schneller als jeder A-Lauf. p = 0,0002, Hodges-Lehmann-Verschiebung +41,2 Tasks/s.
- Urteil des Werkzeugs: „Gewinn“. Das liegt über der Schwelle von 5 % aus dem Qualitätsablauf-Plan (R5).
- py-spy-Profile beider Seiten liegen bei.
- ECC-Zähler vor und nach dem Lauf: 0/0.
- Rohdaten: `benchmarks/comparison/wertung/2026-10-27-b53b3ede-ab-buendeln/`.

## Ursache, auf dem Laptop gemessen

Die Untersuchung lief am 26. und 27.10.2026 auf dem Laptop (Python 3.14.4, gleicher Aufbau, Last durch fremde Container zwischen 2 und 6). Die Rohwerte aller Reihen stehen in `log-listener-buendeln-rohdaten-2026-10.csv`. Ausgewertet wurde jeweils der Median, die Hodges-Lehmann-Verschiebung gegen die Basis der Reihe und der Mann-Whitney-p-Wert.

**Was die Zeile kostet.** Ohne die Erfolgszeile (`--logger-level swarmq.task=WARNING`) stieg der Durchsatz um 12,9 % (36 Läufe je Seite, p < 0,001). Alle swarmq-Logger auf WARNING brachten nicht mehr. Es geht also um die eine Zeile `task_succeeded` je Task.

**Woran es liegt.** Einzelne Teile der Logkette wurden über eine Messhülle gekappt (Reihe 7, je 8 Läufe):

| Probe | gegenüber Basis |
|---|---:|
| Eintrag vor der Queue verwerfen | +10,7 % |
| Listener wacht auf, gibt aber nichts aus | +6,0 % |
| synchron im Haupt-Thread statt im Listener | +4,9 % |
| formatieren ohne Schreiben | ±0 |
| JSON statt `human` | ±0 |

Das Formatieren einer Zeile kostet 14 µs (`human`) bzw. 6,7 µs (JSON). Ausschlaggebend sind die Wechsel zwischen den Threads, gemessen mit den Kontextwechseln aus `/proc/<pid>/task/*/status`:
- Der Listener wachte je Task 2,0-mal auf.
- Der Haupt-Thread schlief je Task etwa einmal zusätzlich, weil er auf den GIL wartete: 2,3–2,7 statt 1,1–1,6 freiwillige Wechsel.

**Vortest vor der Umsetzung.** Eine Probe, die nach leerer Queue 50 ms schläft, brachte +10,7 % (13 Läufe je Seite, p = 0,001).

**Umsetzung.** Gemessen wurde `b53b3ede` mit `--log-batch-delay 0` gegen den Default 50 ms, je 16 Läufe:
- Durchsatz +6,9 % (p = 0,02).
- Der Listener wacht je Task nur noch 0,1–0,2-mal auf, seine CPU-Zeit sinkt von 60–70 auf 30 µs je Task.
- Die Wandzeit je Task sinkt von 1,61–1,66 auf 1,45–1,58 ms.
- Mit und ohne Bündeln kamen je Lauf 1100 `task_succeeded`-Zeilen an.

## Grenzen

- Nur `io_bound` mit 10-ms-Tasks. Bei längeren Tasks ist der Anteil der Logzeile an der Laufzeit kleiner, der Gewinn also auch.
- Der Rest der Kosten bleibt offen. Mit Bündeln liegt der Durchsatz noch rund 6 % unter dem ohne Zeile. Laut den Gegenproben entfallen etwa 2 Punkte auf den Logaufruf vor der Queue, die übrigen auf Einreihen und Listener.
- Die Laptop-Werte streuen stark. Den Ausschlag gibt die Messung auf celsius.
