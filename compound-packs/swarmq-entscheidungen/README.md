# Entscheidungsregister swarmq

Bewusst getroffene Entscheidungen über das Verhalten von swarmq, eine Datei je Entscheidung. Die Regeln verhindern, dass ein entschiedenes Verhalten später als Fehler gemeldet, „repariert“ oder in einem Plan stillschweigend umgeworfen wird.

Das Register wirkt auf drei Ebenen:

1. **Compound Pack:** `.compound-engineering/config.yaml` meldet diesen Ordner an. `ce-plan` und `ce-brainstorm` richten Pläne an den Regeln aus, `ce-code-review` und `ce-doc-review` melden Änderungen, die einer Regel widersprechen, mit Zitat `(pack: swarmq-entscheidungen, <datei>)`.
2. **Entscheidungstests:** Tests, die das entschiedene Verhalten festschreiben, tragen `@pytest.mark.entscheidung("E-nnn")`. Der Guard `tests/unit/test_entscheidungsregister.py` hält Register und Marker in beiden Richtungen konsistent.
3. **Fehlersuche:** Jeder Befund wird vor der Aufnahme in `docs/befunde/` gegen dieses Register abgeglichen (Ablauf: `docs/befunde/README.md`).

## Format

Nur `.md`-Dateien direkt in diesem Ordner sind Regeln; Unterordner sind Ablage (Entwürfe, Belege). Dateiname `eNNN-<kurzthema>.md`.

```markdown
---
title: <die Entscheidung als Aussage>
applies_when:
  - <Situation, in der die Regel greift: Planung, Code-Änderung, Fehlersuche>
tags: [<bereich>, ...]
id: E-nnn
datum: JJJJ-MM-TT          # wann entschieden
status: gilt               # gilt | überholt (dann ersetzt_durch: E-mmm)
quelle:                    # wo die Entscheidung steht (Plan/KTD, Register, Commit, Doku)
  - docs/plans/<plan>.md (KTD2)
tests:                     # Tests mit @pytest.mark.entscheidung("E-nnn")
  - tests/unit/<datei>.py::<test>
# ohne_test: <Begründung>  # statt tests, wenn sich die Entscheidung nicht testen lässt
---

**Entscheidung.** …

**Begründung.** …

**Bewusste Folgen.** Was dadurch absichtlich so ist und deshalb kein Fehler ist.

**Überdenken nur mit neuem Argument.** Offene Einwände mit Verweis (z. B. Befund-ID).
```

## Eine Entscheidung ändern

Eine Entscheidung wird nicht still überschrieben. Eine neue Regel bekommt die nächste freie ID, die alte `status: überholt` und `ersetzt_durch`. Die festschreibenden Tests wandern mit ihrem Marker zur neuen ID. Neue Entscheidungen aus einer Sitzung lassen sich mit `/ce-compound` direkt als Regel in diesen Pack schreiben.
