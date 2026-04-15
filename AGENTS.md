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
| Tests sind schnell | Unit Tests < 100ms, Gesamte Suite < 30 Sekunden |
| Tests sind deterministisch | Kein Test darf manchmal bestehen und manchmal fehlschlagen |
| Tests sind unabhängig | Kein Test darf von der Ausführungsreihenfolge abhängen |

### Verbotene Aktionen

1. **Produktionscode ohne fehlschlagenden Test** — auch nicht "nur eine Hilfsfunktion"
2. **Mehr als ein fehlschlagender Test gleichzeitig** — immer nur ein roter Test
3. **Tests überspringen oder deaktivieren** — kein `@pytest.mark.skip` ohne Ticket-Nummer
4. **Den Red-Schritt überspringen** — IMMER ausführen, auch wenn "offensichtlich"
5. **Neues Verhalten während Refactoring** — Refactoring ändert Struktur, nicht Verhalten
6. **Tests für private Methoden** — die öffentliche API testen
7. **Implementierungsdetails in Assertions** — Verhalten testen, nicht Struktur
8. **Committen mit roten Tests** — NIEMALS

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
```

Drei conftest.py Ebenen:
- `tests/conftest.py` — gemeinsame Fixtures
- `tests/unit/conftest.py` — Unit-spezifisch (Mocks, Factories)
- `tests/integration/conftest.py` — Integration-spezifisch (Connections)
