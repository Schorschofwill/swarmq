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
