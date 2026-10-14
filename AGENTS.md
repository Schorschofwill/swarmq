# SwarmQ — Agent-Anweisungen

## Test-Driven Development (TDD) — Pflicht

Dieses Projekt wird ausschließlich test-driven entwickelt. Die folgenden Regeln sind **nicht optional** und gelten für jede Codeänderung.

### Grundprinzip

Für jede Änderung am Produktionscode MUSS zuerst ein fehlschlagender Test existieren. Es gibt KEINE Ausnahme für "einfachen Code", "offensichtliche Änderungen" oder "Infrastruktur-Code".

**Einzige Ausnahmen:** Reine Konfigurationsdateien (`pyproject.toml`, `docker-compose.yml`), Type-Stubs, und `__init__.py` ohne Logik.

### Der Zyklus — Schritt für Schritt

**Vor der Implementierung:** Test-Liste als Kommentar im Test-File anlegen. Mit dem einfachsten, unabhängigsten Test beginnen.

```
=== ZYKLUS N: [Beschreibung des zu testenden Verhaltens] ===

--- RED ---
1. EINEN fehlschlagenden Test schreiben (nicht zwei, nicht drei)
2. Test ausführen: pytest tests/unit/test_<modul>.py::<testname> -x
3. Verifizieren: Test MUSS fehlschlagen, und zwar aus dem RICHTIGEN Grund

--- GREEN ---
4. MINIMALEN Produktionscode schreiben (nur so viel wie nötig)
5. ALLE Tests ausführen: pytest tests/ -x
6. Verifizieren: ALLE Tests müssen grün sein

--- REFACTOR (optional, aber regelmäßig) ---
7. Code und Tests verbessern (Duplikation, Namen, Struktur)
8. ALLE Tests ausführen nach jedem Refactoring-Schritt
9. Bei Rot: Refactoring rückgängig machen

=== Nächster Zyklus ===
```

### Strategien für den GREEN-Schritt

| Strategie | Wann verwenden |
|---|---|
| **Fake It** | Hardcodierten Wert zurückgeben. Standard-Strategie. |
| **Triangulation** | Zweiten Test schreiben der die Hardcodierung aufbricht. |
| **Obvious Implementation** | Wenn die Lösung trivial ist. Sofort zu Fake It zurück wenn der Test nicht beim ersten Versuch grün wird. |

### Entscheidungsregeln

**Welchen Test als nächstes?**
1. Den einfachsten noch nicht abgedeckten Fall
2. Den nächsten logischen Schritt
3. Einen Edge-Case der während der Implementierung auffiel
4. Einen Test der eine Hardcodierung aufbricht (Triangulation)

**Unit Test oder Integration Test?**
- Unit: Logik isoliert von externen Systemen
- Integration: Zusammenspiel mit RabbitMQ, Valkey, Dateisystem
- Im Zweifel: Unit Test

**Wann mocken?**
- Mocken: I/O (Netzwerk, Dateisystem), externe Services (Broker, Backend), Systemaufrufe (time, signal)
- Nicht mocken: Datenklassen, Value Objects, reine Funktionen
- Faustregel: "Mock the boundaries, not the internals"

**Wann ist ein Feature fertig?**
- Alle Tests der Test-Liste sind geschrieben und grün
- Edge-Cases abgedeckt: None, leere Collections, Grenzwerte, Fehlerfälle
- Code ist refactored
- Man kann keinen sinnvollen fehlschlagenden Test mehr formulieren

### Qualitäts-Gates

| Gate | Beschreibung |
|---|---|
| Alle Tests grün | Kein Commit, kein neues Feature wenn Tests rot sind |
| Kein ungetesteter Code | Jede Zeile Produktionscode wurde durch einen fehlschlagenden Test motiviert |
| Tests sind schnell | Jeder Unit-Test < 100 ms. Die Gesamtlaufzeit darf mit der Zahl der Tests wachsen; Richtwert sind rund 15 ms pro Unit-Test im Mittel. Langsame Tests mit `pytest tests/unit --durations=20` finden |
| Langsame Tests verschieben, nicht abschwächen | Ein Unit-Test, der dauerhaft über 100 ms braucht (echte Prozesse, Timer, Fork), gehört nach `tests/integration/` oder trägt eine Begründung im Test |
| Tests sind deterministisch | Kein Test darf manchmal bestehen und manchmal fehlschlagen |
| Tests sind unabhängig | Kein Test darf von der Ausführungsreihenfolge abhängen |
| Format, Lint und Typen grün | `ruff format --check`, `ruff check` (alle Regeln, Ausnahmen begründet in `pyproject.toml`), mypy und basedpyright müssen grün sein, bevor committed wird. Die Hooks erzwingen Format und Lint beim Commit, die Typprüfer erst beim Push (auf dem Arbeitsbaum); vor dem Commit deshalb mypy und basedpyright selbst laufen lassen. Verbindlich ist der CI-Job `lint-and-types`. Hooks einrichten (`uv run prek install --hook-type pre-commit --hook-type pre-push --allow-missing-config`, siehe `docs/testing/test-strategy.md`). Jede neue Unterdrückung (`# noqa`, `# type: ignore`, `# pyright: ignore`) nennt Code und Grund; ältere `type: ignore` in `tests/` tragen teils nur den Code |

### Verbotene Aktionen

1. **Produktionscode ohne fehlschlagenden Test** — auch nicht "nur eine Hilfsfunktion"
2. **Mehr als ein fehlschlagender Test gleichzeitig** — immer nur ein roter Test
3. **Tests überspringen oder deaktivieren** — kein `@pytest.mark.skip` ohne Ticket-Nummer
4. **Den Red-Schritt überspringen** — IMMER ausführen, auch wenn "offensichtlich"
5. **Neues Verhalten während Refactoring** — Refactoring ändert Struktur, nicht Verhalten
6. **Tests für private Methoden** — die öffentliche API testen
7. **Implementierungsdetails in Assertions** — Verhalten testen, nicht Struktur
8. **Committen mit roten Tests** — NIEMALS
9. **Tests für Laufzeit opfern** — keinen Test weglassen, abschwächen oder mit verkürzten Wartezeiten unzuverlässiger machen, nur um Laufzeit zu sparen

### Erlaubte Abkürzungen

1. "Obvious Implementation" bei trivialer Lösung
2. Mehrere zusammenhängende Assertions im selben Test
3. Fixture-Code ohne eigene Tests
4. Type-Hints ohne Tests
5. Mehrere Zyklen zu einem Commit zusammenfassen
6. Test-Liste während der Implementierung ändern

### Async-Code testen

1. `async def test_...()` mit pytest-asyncio im Auto-Mode
2. `AsyncMock` für async Abhängigkeiten
3. `@pytest.mark.timeout(5)` für alle async Tests
4. Keine `asyncio.sleep()` in Tests — Events und Conditions verwenden
5. Event-Loop nicht manuell verwalten — pytest-asyncio übernimmt das

### Bestehenden Code ändern (Bug-Fix)

1. Test schreiben der den Bug demonstriert (Rot)
2. Minimaler Fix damit Test grün wird
3. Refactoren falls nötig
4. Test bleibt dauerhaft in der Suite

### Dateistruktur

```
src/swarmq/<modul>.py          → tests/unit/test_<modul>.py
src/swarmq/broker/rabbitmq.py  → tests/unit/broker/test_rabbitmq.py
docs/solutions/                — dokumentierte Lösungen zu vergangenen Problemen (Bugs, Best Practices, Async-Patterns), nach Kategorie organisiert mit YAML-Frontmatter (module, tags, problem_type)
```

Drei conftest.py Ebenen:
- `tests/conftest.py` — gemeinsame Fixtures
- `tests/unit/conftest.py` — Unit-spezifisch (Mocks, Factories)
- `tests/integration/conftest.py` — Integration-spezifisch (Connections)

## Entscheidungen respektieren

Bewusst getroffene Entscheidungen über das Verhalten stehen im Entscheidungsregister `compound-packs/swarmq-entscheidungen/` (eine Datei je Entscheidung, ID `E-nnn`). Tests, die ein entschiedenes Verhalten festschreiben, tragen `@pytest.mark.entscheidung("E-nnn")`.

1. **Vor einer Fehlermeldung oder einem Fix:** im Register nachsehen, ob das Verhalten entschieden ist. Ist es entschieden, ist es kein Fehler. Gemeldet wird höchstens „Entscheidung überdenken?“, und nur mit neuem Argument.
2. **Einen Test mit `entscheidung`-Marker nicht anpassen oder löschen**, um einen Fix grün zu bekommen. Erst muss die Entscheidung geändert werden (neue Regel, alte `überholt`).
3. **Fehlersuchen** folgen dem Ablauf in `docs/befunde/README.md` (Gegenprüfung vor Aufnahme ins Befundregister).
4. **Neue Produktentscheidungen** sofort als Regel ins Register schreiben (z. B. per `/ce-compound`), samt festschreibendem Test.
