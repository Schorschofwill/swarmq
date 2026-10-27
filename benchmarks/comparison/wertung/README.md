# Wertungsläufe

Rohdaten der Wertungsläufe auf der Referenzmaschine celsius (Xeon E5-2650 v3,
20 Threads, 60 GiB ECC, Samsung SSD 850), abgeholt mit
`scripts/vergleich-entfernt.sh abholen`. Methodik und Deutung:
`../METHODIK.md`. Die ECC-Zähler standen vor und nach jedem Lauf auf 0.

| Ordner | Commit | Teil | gültig für |
|---|---|---|---|
| `2026-10-25-b18499a9-garantie` | `b18499a9` | Garantie, 3 Durchgänge | alle Kombinationen |
| `2026-10-26-94343b9f-performance` | `94343b9f` | Performance, 3 Durchgänge | `io_bound` |
| `2026-10-26-a9237312-performance` | `a9237312` | Performance, 3 Durchgänge | `noop`, `latency` |
| `2026-10-27-b53b3ede-ab-buendeln` | `fa646592` gegen `b53b3ede` | A/B (`just ab`), 10 Paare | swarmq `io_bound`, Profil `stark` |

Hier liegen nur die gültigen Szenarien eines Laufs. `noop` und `latency` aus
`94343b9f` fehlen, weil sie im 10-ms-Raster abgefragt wurden (METHODIK,
Abschnitt „Parallelität im Performance-Teil“). `ausgang` ist der
Rückgabewert des Laufs.

- **Performance:** je Kombination und Durchgang eine Ergebnisdatei mit
  Kennzahlen, Parametern, Versionen, Images und Host. Bericht:
  `python3 report.py wertung/<ordner>/results`. Die Logs der Läufe (19 bis
  24 MB, nur Ausgaben der Worker) sind nicht abgelegt.
- **A/B:** `ergebnis.json` aus `scripts/ab_messung.py` mit Refs, Messbefehl,
  Rohwerten und Urteil; die py-spy-Profile beider Seiten gepackt in
  `profile.tar.xz`. Deutung:
  `docs/befunde/log-listener-buendeln-messung-2026-10.md`.
- **Garantie:** `bilanz.md` mit den Tabellen, `lauf.json`, je Kombination
  Bilanz (`.json`) und Versionen (`.meta.json`). Die Hauptbücher
  (`.sqlite`, jeder Publish und jede Ausführung) und die Logs von Erzeuger
  und Worker liegen gepackt in `hauptbuecher.tar.xz` und `logs.tar.xz`.
  Dramatiq im Profil `stark` holte in diesem Lauf noch eine Nachricht je
  Prozess statt je Thread (geändert in `94343b9f`); die Zahlen für Dramatiq
  `stark` gelten für diese Einstellung.
