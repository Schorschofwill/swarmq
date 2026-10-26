# Befunde: Ablauf einer Fehlersuche

`docs/befunde/` enthält je Fehlersuche ein Register mit IDs (z. B. `BZ-nn`, `FS-nn`). Die IDs sind die Ticketnummern für Nachweis-Tests (`@pytest.mark.befund("ID")` mit `xfail(strict=True)`).

Ein Befund kommt erst ins Register, wenn er die **Gegenprüfung** bestanden hat. Sie verhindert zweierlei: dass eine frühere, bewusste Entscheidung als Fehler gemeldet wird, und dass ein unrealistisches Szenario (falscher Mock, Auslöser kommt nicht vor) als Fehler zählt.

## Ablauf

1. **Analyse:** Verdachte sammeln (Code-Analyse, Mutation-Testing, Probe-Skripte).
2. **Gegenprüfung** durch einen Prüfer, der den Verdacht nicht gefunden hat. Sein Auftrag ist, jeden Verdacht zu widerlegen:
   - **Entscheidungs-Abgleich:** zuerst das Entscheidungsregister `compound-packs/swarmq-entscheidungen/` (`grep` über `applies_when`, `tags`, `title`), dann die Abschnitte „Entscheidungen“ der Befundregister, `docs/plans/` (KTD, Scope Boundaries, Deferred), `docs/solutions/`, die Nutzer-Doku unter `website/`, den Changelog, Code-Kommentare an der Fundstelle, die git-Historie (`git log -S`, `git blame`) und Tests, die das Verhalten als erwartet prüfen.
   - **Quelle der Erwartung:** Welche Zusage (Doku, Changelog, Docstring, Anforderung, Entscheidung, gleiche Regel an anderer Stelle) wird verletzt? Ohne Quelle ist es keine Fehlermeldung, sondern eine Produktfrage.
   - **Realismus:** Mocks gegen die echte Bibliothek prüfen, den echten Aufrufpfad nachvollziehen und die Häufigkeit des Auslösers einschätzen.
3. **Einstufung** mit zitierter Quelle, je Verdacht genau eine:

   | Einstufung | Bedeutung | Folge |
   |---|---|---|
   | FEHLER | widerspricht einer zitierten Zusage oder Entscheidung | Befund mit Nachweis-Test |
   | ENTSCHEIDUNG | Verhalten wurde bewusst so festgelegt | kein Befund; nur „Entscheidung überdenken?“ mit neuem Argument |
   | LÜCKE | nie entschieden, keine Zusage verletzt | Produktfrage |
   | NICHT HALTBAR | Szenario unrealistisch oder sachlich falsch | entfällt |

4. **Nachweis-Tests** nur für FEHLER: Sie prüfen das zugesagte Verhalten und scheitern mit `--runxfail` an der fachlichen Assertion.
5. **Produktfragen** klärt Georg. Jede Antwort wird als Regel ins Entscheidungsregister geschrieben (z. B. per `/ce-compound`), samt festschreibendem Test mit `@pytest.mark.entscheidung("E-nnn")`.

Ein Nachweis-Test darf keinem Test mit `entscheidung`-Marker widersprechen. Tut er es, ist der Befund eine ENTSCHEIDUNG, solange die Regel gilt.

## Dauerhafter Qualitätsablauf

Seit Plan `docs/plans/2026-10-24-1118-chore-qualitaetsablauf-plan.md` laufen Suchen als tiefe Runden in festem Takt, über vier Säulen: Fehler, Zuverlässigkeit, Angriffssicherheit und Performance (R1). Jede Säule durchläuft den Ablauf oben (R2). Zuverlässigkeit hat eigene Methoden, aber kein eigenes Register.

### Beweisstandard je Säule

- **Fehler und Zuverlässigkeit:** Nachweis-Test wie oben (R3).
- **Angriffssicherheit:** Der Nachweis-Test löst die Schwachstelle über einen erreichbaren Pfad aus, also über die öffentliche API, eine eingehende Nachricht, die Konfiguration oder die CLI (R4). Ein Scanner-Treffer oder eine CVE ohne solchen Pfad ist als Befund NICHT HALTBAR und bleibt ein Signal im Sättigungsbuch.
- **Performance:** Messung mit `uv run just ab` nach R5 und KTD12, mit Profil je Seite. Eine Verbesserung, die eine Garantie oder Entscheidung schwächen würde, wird nicht umgesetzt, sondern geht mit Messwerten als „Entscheidung überdenken?“ an Georg (R6).

Dazu kommt eine fünfte Einstufung:

| Einstufung | Bedeutung | Folge |
|---|---|---|
| VERBESSERUNG | Performance-Gewinn, bewiesen nach R5 | Eintrag ins Register mit Messergebnis, Umsetzung im Fix-Plan |

### Eine Runde

1. **Takt:** alle 14 Tage, festgelegt von Georg am 2026-10-25 (statt je Minor-Release nach R11: Minor-Releases kamen teils mehrmals pro Woche). Gezählt wird ab dem Ende der vorigen Runde. Jede Runde bekommt einen eigenen Plan unter `docs/plans/`.
2. **Beginn:** `uv run just audit` fahren und jeden Treffer als offenes Signal ins Sättigungsbuch `saettigungsbuch.toml` eintragen. Dann mit `uv run just saettigung` die Kombinationen wählen, die am wenigsten abgedeckt und nicht gesättigt sind, und die Auswahl damit begründen (R13).
3. **Methoden** aus dem Methodenkatalog `methodenkatalog.md`. In Angriffssicherheit und Performance beginnt jede Untersuchung bei einem Signal aus dem Buch (R15). Die Lese-Methode gibt es nur bei Fehler und Zuverlässigkeit (R17). Wie der Konkurrenz-Katalog vorgeht, steht dort bei M-F-06, M-A-03 und M-P-03 (R16).
4. **Ergebnis:** ein gegengeprüftes Register mit Nachweis-Tests, Produktfragen und Fix-Plan. Umgesetzt wird erst nach Georgs Ok (R18).
5. **Ende:** Jeder Lauf kommt mit Commit, Prüfumfang und den IDs der bestätigten Befunde ins Buch. Der Wächter `tests/unit/test_saettigungsbuch.py` prüft die Einträge gegen Katalog und Register.

### Signale zwischen den Runden

Ein Signal aus der Pipeline (Abweichung der Kostenmetrik, Warnung von `abhaengigkeiten-pruefen`), aus der Loadsuite oder aus einem Fuzz-Gegenbeispiel kommt als offenes Signal ins Buch. Ein Sicherheitssignal mit Verdacht auf einen erreichbaren Pfad wartet nicht auf den Takt: Es wird sofort nach dem Ablauf oben untersucht, nur für dieses Signal, und Georg erfährt es direkt (KTD10).
