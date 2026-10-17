<!-- Befundregister. IDs F4-nn sind die Ticketnummern fuer xfail-Marker (AGENTS.md: kein Skip ohne Ticket). -->
# Befundregister „Fehlersuche Runde 4“ – swarmq 0.12.3 (b424584), 2026-10-17

Status (2026-10-17): Skelett. Runde läuft nach `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md` autonom (KD6); Gegenprüfung ausstehend. Nichts behoben.

**Anlass.** Drei Lese-Runden (`betrieb-ueber-zeit-2026-10.md`, `fehlersuche-runde-2-2026-10.md`, `fehlersuche-runde-3-2026-10.md`) haben 28, 23 und 27 Fehler gefunden. Runde 4 sucht mit neuen Methoden und in bisher unbesuchten Modulen: Zustandsmaschinen (Hypothesis), Fehlerinjektion unter Last (Loadsuite mit Toxiproxy), Mutation-Testing Welle 2, Betriebsabläufe gegen den Teststack, Fuzzing. Stand ist `main` @ `b424584` (= v0.12.3 unter `src/swarmq/`). Untersucht wird im Worktree `swarmq-fehlersuche4`, Branch `test/fehlersuche-runde-4`.

**Ablauf.** Fünf Spuren mit je eigenem ID-Präfix für Verdachte:
- Z Zustandsmodelle (R4Z): Lock/Unique, Leader/Fire-Index, Workflow, Retry-Kette; Bericht `fehlersuche-runde-4-recherche/1-zustandsmodelle.md`.
- I Injektion (R4I): Chaos-Baseline (`2a-chaos-baseline.md`), Erwartungstabelle (`2b-erwartungen.md`), Störungsläufe der Loadsuite (`2c-injektion.md`).
- M Mutation (R4M): Lauf A (`3a-mutation-lauf-a.md`), Lauf B (`3b-mutation-lauf-b.md`), Sichtung (`3c-sichtung-{D,E,F}.md`).
- B Betrieb (R4B): sechs Betriebsabläufe (`4-betrieb.md`).
- F Fuzz (R4F): sechs Fuzz-Ziele (`5-fuzz.md`).

Danach Gegenprüfung nach `docs/befunde/README.md` durch gekreuzte Prüfer (`6-gegenpruefung-{Z,I,M,B,F}.md`), Gesamtliste `0-gesamtliste.md`. **Maßgeblich sind Einstufung und Priorität der Gegenprüfung.** Proben laufen ohne Dienste, unter der Testsperre gegen 5699/6399 oder auf dem Lastserver über den Docker-Kontext.

**Zusammenfassung.** (nach der Gegenprüfung)

| Einstufung | Anzahl | davon |
|---|---|---|
| FEHLER | – | |
| ENTSCHEIDUNG | – | |
| LÜCKE | – | |
| NICHT HALTBAR | – | |

Nachweis-Tests tragen `@pytest.mark.befund("F4-nn")` und `xfail(strict=True)`. Wird ein Befund behoben, wird sein Test XPASS (rot), und die Markierung muss entfernt werden. Alle Nachweise: `pytest -m befund --runxfail`, Integration nur über `uv run just integration` (Testports).

## Erwartungen je Störungsart (Spur 2, R11)

Siehe `fehlersuche-runde-4-recherche/2b-erwartungen.md` (wird in U11 geschrieben).

## P1 – FEHLER

| ID | Befund | Verletzte Zusage + Quelle | Fix | Verdacht |
|---|---|---|---|---|

## P2 – FEHLER

| ID | Befund | Verletzte Zusage + Quelle | Fix | Verdacht |
|---|---|---|---|---|

## P3 – FEHLER

| ID | Befund | Verletzte Zusage + Quelle | Fix | Verdacht |
|---|---|---|---|---|

## ENTSCHEIDUNG – kein Befund

| ID | Verdacht | Entscheidung (Zitat) | Trotzdem nötige Doku-Korrektur | Neues Argument |
|---|---|---|---|---|

## LÜCKE – Produktfragen

| ID | Prio | Frage | Optionen | Empfehlung | Status |
|---|---|---|---|---|---|
| PF-1 | P3 | Chaos-Stack (`tests/chaos/compose.yml`) von RabbitMQ 3.13.7 auf 4.3.6 heben? Baseline gilt bis dahin nur für 3.13-Verhalten. | (a) heben, (b) belassen | (a) in einer eigenen Änderung nach Runde 4 | offen (Georg) |
| PF-2 | P3 | `stoerung-6h` fahren? Fehlerinjektion fällt nach der Staffelung in die 6-h-Klasse. | (a) nach sauberem 1 h, (b) erst vor Phoenix | (b), solange der 1-h-Lauf nichts zeigt | offen (Georg) |

## NICHT HALTBAR

| ID | Verdacht | Grund |
|---|---|---|

## Vor dem Fix zu entscheiden

(nach der Gegenprüfung)

## Entscheidungen außerhalb des Registers

- Runde 4 ändert keinen Produktionscode (Plan R5). Lauf B der Mutation nur `backend/valkey`, nach den Störungsläufen (Plan KD2, R15). CLI-Sichtung wird bei mehr als 1500 Überlebenden auf vier Module begrenzt (Plan Open Questions).

## Nachweis-Tests

| ID | Geplante Testdatei | Art | Status |
|---|---|---|---|
