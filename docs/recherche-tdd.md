# Recherche: Test-Driven Development (TDD) fuer SwarmQ

Stand: 2026-04-15 | Python 3.14+ | pytest 8.x | pytest-asyncio 0.25+

---

## Inhaltsverzeichnis

### Teil 1: TDD-Grundlagen
1. [Der Red-Green-Refactor-Zyklus](#1-der-red-green-refactor-zyklus)
2. [Die drei Gesetze von TDD (Robert C. Martin)](#2-die-drei-gesetze-von-tdd-robert-c-martin)
3. [Kent Becks Original-Regeln und Strategien](#3-kent-becks-original-regeln-und-strategien)

### Teil 2: TDD-Workflow im Detail
4. [Ein neues Feature starten](#4-ein-neues-feature-starten)
5. [Der Test-Schreibprozess](#5-der-test-schreibprozess)
6. [Der Implementierungsprozess](#6-der-implementierungsprozess)
7. [Der Refactoring-Prozess](#7-der-refactoring-prozess)

### Teil 3: TDD fuer Async Python
8. [pytest-asyncio Patterns](#8-pytest-asyncio-patterns)
9. [Async I/O testen](#9-async-io-testen)
10. [Test-Patterns fuer Task Queues](#10-test-patterns-fuer-task-queues)

### Teil 4: Test-Typen und ihre Rolle in TDD
11. [Unit Tests -- Das primaere Werkzeug](#11-unit-tests----das-primaere-werkzeug)
12. [Integration Tests](#12-integration-tests)
13. [End-to-End Tests und die Test-Pyramide](#13-end-to-end-tests-und-die-test-pyramide)

### Teil 5: Anti-Patterns und Test-Smells
14. [Haeufige TDD-Fehler](#14-haeufige-tdd-fehler)
15. [Test-Smells erkennen und beheben](#15-test-smells-erkennen-und-beheben)

### Teil 6: TDD und Architektur
16. [Wie TDD Design beeinflusst](#16-wie-tdd-design-beeinflusst)
17. [London School vs. Chicago School](#17-london-school-vs-chicago-school)

### Teil 7: TDD-Regeln fuer einen AI Coding Agent
18. [Operative Regeln -- Das vollstaendige Regelwerk](#18-operative-regeln----das-vollstaendige-regelwerk)

### Teil 8: Praktische pytest-Konfiguration
19. [Konfiguration und Plugin-Empfehlungen](#19-konfiguration-und-plugin-empfehlungen)
20. [Verzeichnisstruktur und Fixture-Organisation](#20-verzeichnisstruktur-und-fixture-organisation)

---

# Teil 1: TDD-Grundlagen

## 1. Der Red-Green-Refactor-Zyklus

TDD ist ein Entwicklungsprozess, bei dem Tests VOR dem Produktionscode geschrieben werden. Der Kern ist ein disziplinierter, sich wiederholender Mikrozyklus mit drei exakten Phasen.

### 1.1 Phase 1: Red (Rot) -- Ein fehlschlagender Test

**Was genau passiert:**
1. Man schreibt EINEN einzelnen Test fuer ein Verhalten, das noch nicht existiert
2. Man fuehrt diesen Test aus
3. Der Test MUSS fehlschlagen (rot)
4. Der Test muss **aus dem richtigen Grund** fehlschlagen

**Wichtige Unterscheidung:** "Rot" bedeutet nicht einfach "kompiliert nicht" oder "ImportError". Es bedeutet, dass der Test laeuft, aber die Assertion fehlschlaegt. In Python ist die Grenze fliessender als in kompilierten Sprachen -- ein `ImportError` oder `AttributeError` ist akzeptabel als "Rot", wenn die zu testende Klasse/Funktion noch nicht existiert. Aber sobald die Signatur existiert, muss der Fehler eine fehlschlagende Assertion sein, nicht ein Runtime-Error.

**Beispiel -- korrektes Rot:**
```python
# tests/test_task.py
from swarmq.task import Task

def test_task_has_name():
    task = Task(name="send_email")
    assert task.name == "send_email"
```

Wenn `swarmq/task.py` noch nicht existiert: `ModuleNotFoundError` -- akzeptables Rot.
Wenn `Task` existiert aber kein `name`-Attribut hat: `AttributeError` -- akzeptables Rot.
Wenn `Task(name="send_email")` funktioniert aber `task.name` etwas anderes zurueckgibt: `AssertionError` -- ideales Rot.

**Was NICHT akzeptabel ist:**
- Ein Test der bereits gruen ist (er testet dann nichts Neues)
- Ein Test der wegen eines Syntaxfehlers im Testcode selbst fehlschlaegt
- Ein Test der aufgrund von Infrastrukturproblemen fehlschlaegt (fehlende Fixtures, kaputte Imports von Test-Utilities)

### 1.2 Phase 2: Green (Gruen) -- Minimaler Code zum Bestehen

**Was genau passiert:**
1. Man schreibt den MINIMALEN Produktionscode, damit der fehlschlagende Test gruen wird
2. Man fuehrt ALLE Tests aus (nicht nur den neuen)
3. ALLE Tests muessen gruen sein

**Entscheidend: MINIMAL bedeutet MINIMAL.** Das heisst:
- Hardcodierte Rueckgabewerte sind erlaubt und sogar erwuenscht
- Keine Vorausoptimierung
- Kein "richtiger" Code, wenn ein einfacherer den Test besteht
- Keine zusaetzliche Funktionalitaet, die kein Test verlangt

**Beispiel -- korrektes minimales Gruen:**
```python
# swarmq/task.py
class Task:
    def __init__(self, name: str) -> None:
        self.name = name
```

**Warum Minimalismus so wichtig ist:**
- Jede Zeile Produktionscode die nicht durch einen Test erzwungen wurde, ist ungetestet
- Minimaler Code ist am einfachsten zu aendern, wenn der naechste Test eine andere Richtung erzwingt
- Es verhindert "Gold Plating" -- Features zu bauen, die niemand angefordert hat
- Es haelt den Entwickler im Rhythmus des Zyklus

### 1.3 Phase 3: Refactor -- Verbessern ohne Verhaltensaenderung

**Was genau passiert:**
1. Man betrachtet sowohl den Produktionscode als auch den Testcode
2. Man identifiziert Verbesserungsmoeglichkeiten: Duplikation entfernen, Namen verbessern, Struktur vereinfachen
3. Man refactored in kleinen Schritten
4. Nach JEDEM Refactoring-Schritt: ALLE Tests ausfuehren
5. Tests muessen gruen bleiben -- wenn ein Test rot wird, hat das Refactoring das Verhalten geaendert und muss rueckgaengig gemacht werden

**Was in dieser Phase erlaubt ist:**
- Duplikation im Produktionscode entfernen (DRY)
- Methoden/Funktionen extrahieren
- Variablen/Klassen/Methoden umbenennen
- Komplexitaet reduzieren
- Design-Patterns einfuehren (aber nur wenn die aktuelle Komplexitaet es rechtfertigt)
- Testcode aufraeuemen (Fixtures extrahieren, Hilfsmethoden erstellen)

**Was in dieser Phase NICHT erlaubt ist:**
- Neues Verhalten hinzufuegen
- Neue Tests schreiben (dafuer zurueck zu Phase 1)
- "Nur schnell noch dieses Feature einbauen"

### 1.4 Zyklusgroesse: Kent Becks "Baby Steps"

Kent Beck propagiert extrem kleine Zyklen -- idealerweise Minuten, nicht Stunden. Die Faustregel:

| Zykluslaenge | Bewertung |
|---|---|
| 1-5 Minuten | Ideal. So arbeiten TDD-Experten. |
| 5-15 Minuten | Akzeptabel fuer komplexere Logik. |
| 15-30 Minuten | Zu gross. Den naechsten Schritt in kleinere Teile zerlegen. |
| > 30 Minuten | TDD-Versagen. Sofort stoppen und die Aufgabe neu aufteilen. |

**Fuer einen AI Coding Agent:** Der Zyklus sollte noch kuerzer sein, da der Agent keinen mentalen Kontext verliert. Ziel: Ein Test, eine Implementierung, ein optionales Refactoring -- dann sofort der naechste Zyklus.

### 1.5 Wann committen?

Es gibt zwei verbreitete Strategien:

**Strategie A: Nach jedem gruenen Zyklus committen**
- Jeder Commit ist ein Inkrement: Test + Implementierung (+ Refactoring)
- Vorteil: Feingranulare History, leicht zu revert-en
- Nachteil: Sehr viele Commits

**Strategie B: Nach einem logischen Feature-Inkrement committen**
- Mehrere Red-Green-Refactor-Zyklen werden zu einem Commit zusammengefasst
- Vorteil: Saubere, lesbare History
- Nachteil: Groessere Commits

**Empfehlung fuer SwarmQ und einen AI Agent:** Strategie B -- nach einem logisch abgeschlossenen Feature-Inkrement committen (z.B. "Task kann erstellt und benannt werden" statt einzelner Commits fuer jedes Attribut). Aber niemals committen wenn Tests rot sind.

---

## 2. Die drei Gesetze von TDD (Robert C. Martin)

Robert C. Martin ("Uncle Bob") hat TDD auf drei Gesetze reduziert, die den Prozess praezise definieren:

### Gesetz 1: Kein Produktionscode ohne fehlschlagenden Test
> "You are not allowed to write any production code unless it is to make a failing unit test pass."

**Bedeutung:** Jede einzelne Zeile Produktionscode muss durch einen fehlschlagenden Test motiviert sein. Es gibt KEINE Ausnahme. Kein "ich fuege noch schnell diese Hilfsmethode hinzu" oder "dieser Edge-Case ist offensichtlich".

### Gesetz 2: Nicht mehr Test schreiben als zum Fehlschlagen noetig
> "You are not allowed to write any more of a unit test than is sufficient to fail; and compilation failures are failures."

**Bedeutung:** Sobald der Test fehlschlaegt -- aufhoeren den Test zu schreiben. Nicht drei Assertions hinzufuegen, wenn eine reicht um das gewuenschte Verhalten zu beschreiben. Nicht fuenf Testfaelle gleichzeitig schreiben.

**Praktisch:** Eine Test-Funktion mit einer klaren Assertion. Wenn der Test fehlschlaegt (oder gar nicht kompiliert/importiert), sofort zur Implementierung wechseln.

### Gesetz 3: Nicht mehr Produktionscode als zum Bestehen noetig
> "You are not allowed to write any more production code than is sufficient to pass the one failing unit test."

**Bedeutung:** Sobald der Test gruen ist -- aufhoeren Produktionscode zu schreiben. Auch wenn man "weiss", dass der naechste Test eine bestimmte Erweiterung braucht. Jede Erweiterung wird durch ihren eigenen fehlschlagenden Test motiviert.

### Zusammenspiel der drei Gesetze

Die drei Gesetze erzeugen einen extrem engen Feedback-Loop:

```
Schreibe ein paar Zeilen Test → Test schlaegt fehl → STOP
Schreibe ein paar Zeilen Produktion → Test ist gruen → STOP
Schreibe ein paar Zeilen Test → Test schlaegt fehl → STOP
Schreibe ein paar Zeilen Produktion → Test ist gruen → STOP
...
```

Das Ergebnis: Man wechselt alle paar Sekunden bis Minuten zwischen Test und Produktion. Der Code waechst inkrementell, und zu jedem Zeitpunkt sind alle Tests (bis auf den gerade geschriebenen) gruen.

---

## 3. Kent Becks Original-Regeln und Strategien

Kent Beck beschrieb in "Test-Driven Development: By Example" (2003) nicht nur den Zyklus, sondern auch drei konkrete Strategien, wie man vom fehlschlagenden Test zum gruenen Test kommt.

### 3.1 Strategie: Fake It Till You Make It

**Prinzip:** Den Test mit einer hartcodierten Konstante bestehen lassen, dann schrittweise generalisieren.

```python
# Test
def test_task_default_max_retries():
    task = Task(name="send_email")
    assert task.max_retries == 3

# Erste Implementierung -- FAKE IT
class Task:
    def __init__(self, name: str) -> None:
        self.name = name
        self.max_retries = 3  # Hartcodiert -- bewusst!
```

**Wann verwenden:**
- Wenn man sich unsicher ist, wie die allgemeine Loesung aussieht
- Wenn man das einfachste moegliche Gruen erreichen will
- Bei den ersten Tests eines neuen Moduls
- Immer, wenn die "richtige" Loesung nicht offensichtlich ist

**Warum das nicht doof ist:** Die Hardcodierung wird durch den naechsten Test aufgebrochen. Wenn kein weiterer Test sie aufbricht, war sie vielleicht sogar die richtige Loesung.

### 3.2 Strategie: Triangulation

**Prinzip:** Generalisierung durch das Hinzufuegen von zwei oder mehr Tests, die verschiedene Eingaben verwenden. Die generelle Implementierung ergibt sich aus der Notwendigkeit, alle Tests gleichzeitig zu bestehen.

```python
# Test 1 -- bereits gruen mit Hardcodierung
def test_backoff_first_retry():
    assert compute_backoff(attempts=1, factor=5) == 5

# Test 2 -- erzwingt Generalisierung
def test_backoff_second_retry():
    assert compute_backoff(attempts=2, factor=5) == 25

# Jetzt MUSS die Implementierung generalisiert werden:
def compute_backoff(attempts: int, factor: int) -> int:
    return factor ** attempts
```

**Wann verwenden:**
- Wenn man nicht sicher ist, in welche Richtung die Generalisierung gehen soll
- Bei algorithmischer Logik (Berechnungen, Transformationen)
- Wenn "Fake It" allein nicht genuegt, um die richtige Abstraktion zu finden

### 3.3 Strategie: Obvious Implementation

**Prinzip:** Wenn die Implementierung offensichtlich ist, einfach direkt schreiben -- kein Umweg ueber Hardcodierung.

```python
# Test
def test_task_full_queue_name():
    task = Task(name="send_email", queue="notifications")
    assert task.full_queue_name == "notifications.send_email"

# Implementierung -- direkt die offensichtliche Loesung
@property
def full_queue_name(self) -> str:
    return f"{self.queue}.{self.name}"
```

**Wann verwenden:**
- Wenn die Loesung trivial und offensichtlich ist
- Wenn man hohe Zuversicht hat, dass die Implementierung korrekt ist
- Bei einfachen Getter/Setter-Logik, String-Formatierung, einfacher Mathematik

**WARNUNG:** Wenn die "offensichtliche" Implementierung den Test nicht besteht, SOFORT zurueck zu "Fake It" oder "Triangulation" wechseln. Die eigene Zuversicht war offensichtlich unberechtigt.

### 3.4 Kent Becks Entscheidungsregel

Kent Beck formulierte es so:
> "Wenn du weisst, wie die Implementierung aussieht: Obvious Implementation.
> Wenn du es nicht genau weisst: Fake It.
> Wenn die Richtung der Generalisierung unklar ist: Triangulation."

Die Entscheidung wird Test fuer Test getroffen, nicht fuer das gesamte Feature.

### 3.5 "Make It Work, Make It Right, Make It Fast"

Kent Becks beruhmtes Mantra beschreibt die Reihenfolge der Prioritaeten:

1. **Make it work:** Der Test wird gruen (egal wie haesslich der Code ist)
2. **Make it right:** Der Code wird sauber refactored (Phase 3 des Zyklus)
3. **Make it fast:** Optimierung kommt ZULETZT, und nur wenn Messungen es rechtfertigen

**Fuer SwarmQ:** Performance ist ein wichtiges Ziel (siehe Aufgabe), aber Optimierung kommt NACH korrekt funktionierendem, sauberem Code. Erst testen, dann korrekt implementieren, dann mit Benchmarks optimieren.

---

# Teil 2: TDD-Workflow im Detail

## 4. Ein neues Feature starten

### 4.1 Feature-Dekomposition in testbare Inkremente

Bevor der erste Test geschrieben wird, erstellt man eine **Test-Liste** -- eine informelle Liste der Tests, die man voraussichtlich schreiben wird. Kent Beck beschrieb diese Technik ausfuehrlich.

**Beispiel: Feature "Task Retry mit Exponential Backoff"**

Test-Liste (erster Entwurf):
```
- [ ] Task ohne Retry-Konfiguration hat default max_retries=3
- [ ] Task mit max_retries=0 wird nie retried
- [ ] Task mit max_retries=5 wird bis zu 5 mal retried
- [ ] Backoff fuer ersten Retry ist min_backoff
- [ ] Backoff waechst exponentiell
- [ ] Backoff ueberschreitet nie max_backoff
- [ ] Jitter wird auf Backoff angewendet
- [ ] Task mit erschoepften Retries geht in DLQ
- [ ] Spezifische Exceptions koennen von Retry ausgeschlossen werden
```

**Wichtig:**
- Die Liste ist NICHT vollstaendig -- sie waechst waehrend der Implementierung
- Die Liste ist NICHT bindend -- Tests koennen geaendert, gestrichen oder hinzugefuegt werden
- Die Liste dient der Orientierung, nicht der Planung

### 4.2 Welchen Test zuerst schreiben?

Kent Becks Regeln fuer die Test-Reihenfolge:

1. **Den einfachsten Fall zuerst.** Nicht den kompliziertesten, nicht den interessantesten -- den einfachsten.
2. **Den "Happy Path" vor den Edge Cases.** Erst das Normalverhalten, dann die Sonderfaelle.
3. **Unabhaengige Tests vor abhaengigen.** Ein Test der auf dem Ergebnis eines anderen aufbaut, kommt spaeter.
4. **Hohe Unsicherheit frueh adressieren.** Wenn ein Teil des Features besonders unklar ist, dort anfangen -- der Test hilft, das Design zu klaeren.

**Fuer das Retry-Beispiel:** "Task ohne Retry-Konfiguration hat default max_retries=3" waere ein guter erster Test -- einfach, unabhaengig, Happy Path.

### 4.3 Die Wichtigkeit des einfachsten Falls

Der einfachste Test hat drei Funktionen:
1. **Infrastruktur pruefen:** Funktioniert der Import? Kann die Klasse instanziiert werden?
2. **Design-Entscheidung erzwingen:** Wie heisst die Klasse? Welche Parameter hat der Konstruktor?
3. **Schnelles Erfolgserlebnis:** Ein gruener Test nach wenigen Minuten bestaetigt, dass die Richtung stimmt.

---

## 5. Der Test-Schreibprozess

### 5.1 Das Arrange-Act-Assert Pattern (AAA)

Jeder Test besteht aus drei klar getrennten Abschnitten:

```python
def test_task_retry_increments_attempt_counter():
    # Arrange -- Vorbereitung
    task = Task(name="flaky_task", max_retries=3)

    # Act -- Ausfuehrung der zu testenden Aktion
    task.record_failure(error=RuntimeError("timeout"))

    # Assert -- Erwartetes Ergebnis pruefen
    assert task.current_attempt == 1
```

**Regeln:**
- **Arrange:** Alle Objekte erstellen, Zustand vorbereiten. Darf komplex sein (deshalb gibt es Fixtures).
- **Act:** EINE Aktion ausfuehren. Nicht zwei, nicht drei -- eine. Wenn mehrere Aktionen noetig sind, ist es wahrscheinlich mehr als ein Test.
- **Assert:** Das Ergebnis pruefen. Klar und direkt.

### 5.2 Das Given-When-Then Pattern

Semantisch aequivalent zu AAA, aber mit staerkerem Fokus auf Verhaltensbeschreibung:

```python
def test_expired_task_is_not_retried():
    # Given: Ein Task mit abgelaufenem TTL
    task = Task(name="old_task", ttl=timedelta(seconds=0))

    # When: Der Task fehlschlaegt
    result = task.should_retry(error=RuntimeError("fail"))

    # Then: Kein Retry
    assert result is False
```

Beide Patterns sind gleichwertig. Konsistenz innerhalb des Projekts ist wichtiger als die Wahl des Patterns.

### 5.3 Assertions: Eine oder mehrere?

**Grundregel:** Eine Assertion pro Test. Aber mit pragmatischen Ausnahmen:

**Eine Assertion -- der Idealfall:**
```python
def test_task_name():
    task = Task(name="send_email")
    assert task.name == "send_email"
```

**Mehrere zusammenhaengende Assertions -- akzeptabel:**
```python
def test_task_default_configuration():
    task = Task(name="send_email")
    assert task.max_retries == 3
    assert task.timeout == timedelta(seconds=300)
    assert task.queue == "default"
```

**Wann mehrere Assertions akzeptabel sind:**
- Wenn sie alle DASSELBE Verhalten testen (hier: Default-Konfiguration)
- Wenn die Assertions zusammen eine logische Einheit bilden
- Wenn separate Tests den Setup-Code unnoetig duplizieren wuerden

**Wann sie NICHT akzeptabel sind:**
- Wenn die Assertions verschiedene Verhaltensaspekte testen
- Wenn ein fruehes Assert-Failure die spaeteren Assertions verbirgt (in pytest wird nach dem ersten Failure abgebrochen)

### 5.4 Test-Naming-Konventionen

Der Testname muss das getestete Verhalten kommunizieren, nicht die Implementierung.

**Empfohlenes Format fuer SwarmQ:**
```
test_<was_getestet_wird>_<unter_welcher_bedingung>_<erwartetes_ergebnis>
```

**Gute Beispiele:**
```python
def test_task_with_zero_retries_is_not_retried():
def test_backoff_exceeding_max_is_capped():
def test_expired_message_is_rejected():
def test_worker_shutdown_waits_for_running_tasks():
```

**Schlechte Beispiele:**
```python
def test_task():  # Was genau?
def test_retry_logic():  # Welcher Aspekt?
def test_it_works():  # Was funktioniert?
def test_compute_backoff_returns_correct_value():  # "korrekt" ist nichtssagend
```

### 5.5 Tests fuer Code schreiben, der noch nicht existiert

In Python ist es normal, dass der erste Test einen `ImportError` oder `ModuleNotFoundError` erzeugt. Das ist akzeptables "Rot".

**Der Ablauf:**
```python
# Schritt 1: Test schreiben
# tests/test_middleware.py
from swarmq.middleware import RetryMiddleware  # Existiert noch nicht

async def test_retry_middleware_retries_on_failure():
    middleware = RetryMiddleware(max_retries=3)
    # ...

# Schritt 2: Test ausfuehren → ModuleNotFoundError (ROT -- akzeptabel)

# Schritt 3: Minimale Datei erstellen
# swarmq/middleware.py
class RetryMiddleware:
    def __init__(self, max_retries: int) -> None:
        self.max_retries = max_retries

# Schritt 4: Test ausfuehren → AssertionError (ROT -- ideal)

# Schritt 5: Minimaler Code zum Bestehen
# ...
```

---

## 6. Der Implementierungsprozess

### 6.1 Was zaehlt als "minimaler Code zum Bestehen"?

Die Antwort haengt von der gewaehlten Strategie ab (Fake It / Obvious Implementation):

**Fake It -- Extrembeispiel:**
```python
# Test
def test_compute_backoff_first_attempt():
    assert compute_backoff(1) == 1000  # 1 Sekunde

# Minimale Implementierung
def compute_backoff(attempts: int) -> int:
    return 1000  # Hardcodiert!
```

Das ist KORREKT nach TDD. Der naechste Test wird die Generalisierung erzwingen:
```python
# Naechster Test
def test_compute_backoff_second_attempt():
    assert compute_backoff(2) == 2000  # 2 Sekunden

# Jetzt MUSS generalisiert werden
def compute_backoff(attempts: int) -> int:
    return attempts * 1000
```

**Obvious Implementation:**
```python
# Test
def test_compute_backoff():
    assert compute_backoff(1) == 1000
    assert compute_backoff(2) == 2000
    assert compute_backoff(3) == 4000

# Direkt die offensichtliche Loesung
def compute_backoff(attempts: int) -> int:
    return 1000 * (2 ** (attempts - 1))
```

### 6.2 Wann ist Hardcodierung erlaubt?

**Immer.** Es gibt keine TDD-Regel, die Hardcodierung verbietet. Aber es gibt eine Regel, die sagt: Wenn mehrere Tests nur durch Generalisierung gleichzeitig bestanden werden koennen, DANN generalisieren.

Die Sequenz ist:
1. Erster Test: Hardcodierung ist OK
2. Zweiter Test: Wenn die Hardcodierung nicht mehr funktioniert, generalisieren
3. Wenn die Hardcodierung fuer alle Tests funktioniert: Sie IST die Implementierung

### 6.3 Wann generalisieren?

**Becks Regel:** Generalisiere, wenn du Duplikation siehst -- zwischen Test und Produktion oder zwischen zwei Code-Stellen.

```python
# VORHER: Duplikation zwischen Test und Produktion
def compute_backoff(attempts: int) -> int:
    return 1000  # Die "1000" ist auch im Test -- Duplikation!

# NACHHER: Generalisiert, Duplikation entfernt
def compute_backoff(attempts: int, base_ms: int = 1000) -> int:
    return base_ms * (2 ** (attempts - 1))
```

### 6.4 Wann ist ein Feature "fertig"?

Ein Feature ist fertig, wenn:
1. Alle Tests der Test-Liste sind geschrieben und gruen
2. Man kann sich keinen weiteren sinnvollen Test vorstellen
3. Die Edge-Cases sind abgedeckt (Null-Werte, leere Listen, Grenzwerte, Fehlerfaelle)
4. Der Code ist refactored und sauber

**Fuer einen AI Agent:** Ein Feature ist fertig, wenn die Akzeptanzkriterien durch Tests abgedeckt sind UND man keinen weiteren fehlschlagenden Test schreiben kann, der einen Bug aufdecken wuerde.

---

## 7. Der Refactoring-Prozess

### 7.1 Was refactoren?

In der Refactoring-Phase (gruene Tests) gibt es fuenf Hauptkategorien:

**1. Duplikation entfernen (DRY im Produktionscode):**
```python
# VORHER: Duplikation
class TaskA:
    def compute_delay(self) -> int:
        return min(1000 * (2 ** self.attempts), 60000)

class TaskB:
    def compute_delay(self) -> int:
        return min(1000 * (2 ** self.attempts), 60000)

# NACHHER: Extrahiert
def exponential_backoff(attempts: int, base: int = 1000, cap: int = 60000) -> int:
    return min(base * (2 ** attempts), cap)
```

**2. Namen verbessern:**
```python
# VORHER
def proc(m):
    ...

# NACHHER
def process_message(message: IncomingMessage) -> TaskResult:
    ...
```

**3. Methoden extrahieren:**
```python
# VORHER: Lange Methode
async def execute_task(self, message):
    # 20 Zeilen Deserialisierung
    # 15 Zeilen Validierung
    # 10 Zeilen Ausfuehrung
    # 10 Zeilen Ergebnis speichern

# NACHHER: Aufgeteilt
async def execute_task(self, message):
    task_data = self._deserialize(message)
    self._validate(task_data)
    result = await self._run(task_data)
    await self._store_result(result)
```

**4. Komplexitaet reduzieren:**
```python
# VORHER: Verschachteltes if
if task.is_expired():
    if task.has_retries_left():
        if not task.is_cancelled():
            retry(task)

# NACHHER: Guard Clauses
if task.is_expired():
    return
if not task.has_retries_left():
    return
if task.is_cancelled():
    return
retry(task)
```

**5. Design-Patterns einfuehren** (nur wenn die Komplexitaet es rechtfertigt):
```python
# VORHER: Switch/If-Ketten
if broker_type == "rabbitmq":
    ...
elif broker_type == "valkey":
    ...

# NACHHER: Strategy Pattern (nur wenn 3+ Broker existieren)
broker = broker_registry.get(broker_type)
await broker.publish(message)
```

### 7.2 DRY in Testcode vs. Produktionscode

**Kritischer Unterschied:** Im Testcode ist DRY WENIGER wichtig als Lesbarkeit.

**Produktionscode:** DRY ist fast immer richtig. Duplikation ist ein Code-Smell.

**Testcode:** Moderate Duplikation ist AKZEPTABEL und oft BESSER:
- Jeder Test soll fuer sich allein verstaendlich sein
- Zu viel Abstraktion in Tests macht sie schwer zu debuggen
- Wenn ein Test fehlschlaegt, soll man SOFORT sehen was getestet wird, ohne 5 Helper-Funktionen zu lesen

```python
# OK -- leichte Duplikation, aber jeder Test ist eigenstaendig lesbar
def test_task_with_default_retries():
    task = Task(name="send_email")
    assert task.max_retries == 3

def test_task_with_custom_retries():
    task = Task(name="send_email", max_retries=5)
    assert task.max_retries == 5

# SCHLECHT -- zu abstrahiert, schwer zu lesen
def _make_task(**kwargs):
    defaults = {"name": "send_email", "max_retries": 3, ...}
    defaults.update(kwargs)
    return Task(**defaults)

def test_task_retries(task_factory):
    assert _make_task().max_retries == 3
    assert _make_task(max_retries=5).max_retries == 5
```

**Wann Testcode refactoren:**
- Fixtures fuer teure Setup-Operationen (Datenbankverbindung, Broker-Connection)
- Factory-Funktionen wenn derselbe Setup in >5 Tests vorkommt UND die Lesbarkeit nicht leidet
- Nie um "Eleganz" willen -- Testcode darf langweilig sein

### 7.3 Wann NICHT refactoren

- **Wenn Tests rot sind:** Erst gruen machen, dann refactoren
- **Wenn man ein neues Feature einbaut:** Features gehoeren in Phase 1 (neuer Test), nicht in Phase 3
- **Wenn man nicht sicher ist, ob das Refactoring korrekt ist:** Lieber einen Test hinzufuegen, der das Verhalten vor dem Refactoring absichert

---

# Teil 3: TDD fuer Async Python

## 8. pytest-asyncio Patterns

### 8.1 Grundlegende async Testfunktionen

```python
import pytest
from swarmq.worker import Worker

@pytest.mark.asyncio
async def test_worker_starts_and_stops():
    worker = Worker(broker_url="amqp://localhost")
    await worker.start()
    assert worker.is_running
    await worker.stop()
    assert not worker.is_running
```

**pytest-asyncio Mode-Konfiguration (empfohlen: auto):**
```toml
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
```

Mit `asyncio_mode = "auto"` wird `@pytest.mark.asyncio` fuer alle async Funktionen automatisch angewendet. Das spart Boilerplate.

### 8.2 Async Fixtures

```python
import pytest
from swarmq.broker import RabbitMQBroker

@pytest.fixture
async def broker():
    """Fixture das eine Broker-Verbindung bereitstellt und aufraeumt."""
    b = RabbitMQBroker(url="amqp://guest:guest@localhost/")
    await b.connect()
    yield b
    await b.disconnect()

@pytest.fixture
async def worker(broker):
    """Fixture das einen Worker mit Broker bereitstellt."""
    w = Worker(broker=broker)
    await w.start()
    yield w
    await w.stop()
```

### 8.3 Event-Loop-Scope

Fuer Integration-Tests die eine gemeinsame Verbindung teilen:

```python
import pytest

@pytest.fixture(scope="module")
async def shared_broker():
    """Ein Broker fuer alle Tests im Modul (spart Verbindungsaufbau)."""
    broker = RabbitMQBroker(url="amqp://guest:guest@localhost/")
    await broker.connect()
    yield broker
    await broker.disconnect()
```

**Achtung:** Module-Scope Fixtures benoetigen eine passende Event-Loop-Konfiguration:
```python
# conftest.py
@pytest.fixture(scope="module")
def event_loop_policy():
    """Verwende den Default-Loop fuer module-scoped async Fixtures."""
    return asyncio.DefaultEventLoopPolicy()
```

### 8.4 Testen von Coroutinen, Tasks und Timeouts

```python
import asyncio

async def test_task_execution_respects_timeout():
    """Test dass ein Task bei Timeout abgebrochen wird."""
    async def slow_task():
        await asyncio.sleep(10)  # Simuliert langsamen Task

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(slow_task(), timeout=0.1)

async def test_concurrent_task_execution():
    """Test dass Tasks parallel ausgefuehrt werden."""
    results = []

    async def task(value: int):
        await asyncio.sleep(0.01)
        results.append(value)

    await asyncio.gather(task(1), task(2), task(3))
    assert sorted(results) == [1, 2, 3]
```

---

## 9. Async I/O testen

### 9.1 Mocking mit AsyncMock

`unittest.mock.AsyncMock` ist das Standardwerkzeug fuer async Mocks:

```python
from unittest.mock import AsyncMock, MagicMock, patch

async def test_worker_acknowledges_message_after_processing():
    # Arrange
    message = AsyncMock()
    message.body = b'{"task": "send_email", "args": []}'
    message.ack = AsyncMock()

    processor = TaskProcessor()

    # Act
    await processor.handle(message)

    # Assert
    message.ack.assert_awaited_once()
```

### 9.2 Mocking aio-pika (RabbitMQ)

```python
from unittest.mock import AsyncMock, MagicMock, patch

@pytest.fixture
def mock_channel():
    """Mock eines aio-pika Channels."""
    channel = AsyncMock()
    channel.default_exchange = AsyncMock()
    channel.default_exchange.publish = AsyncMock()

    queue = AsyncMock()
    queue.name = "test_queue"
    queue.consume = AsyncMock()
    channel.declare_queue = AsyncMock(return_value=queue)

    return channel

@pytest.fixture
def mock_connection(mock_channel):
    """Mock einer aio-pika Connection."""
    connection = AsyncMock()
    connection.channel = AsyncMock(return_value=mock_channel)
    return connection

async def test_broker_publishes_message(mock_connection, mock_channel):
    with patch("aio_pika.connect_robust", return_value=mock_connection):
        broker = RabbitMQBroker(url="amqp://localhost")
        await broker.connect()
        await broker.publish("test_queue", b'{"task": "hello"}')

        mock_channel.default_exchange.publish.assert_awaited_once()
```

### 9.3 Mocking valkey-py

```python
@pytest.fixture
def mock_valkey():
    """Mock eines Valkey-Clients."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=1)
    client.evalsha = AsyncMock()
    client.script_load = AsyncMock(return_value=b"abc123sha")
    return client

async def test_result_backend_stores_result(mock_valkey):
    with patch("valkey.asyncio.Valkey", return_value=mock_valkey):
        backend = ValkeyBackend(url="valkey://localhost")
        await backend.connect()
        await backend.store_result("task-123", {"status": "done"})

        mock_valkey.set.assert_awaited_once()
        call_args = mock_valkey.set.call_args
        assert b"task-123" in call_args[0][0]
```

### 9.4 Wann Mocks vs. echte Services verwenden

| Situation | Mock | Echter Service |
|---|---|---|
| Unit Tests (einzelne Klasse) | Ja | Nein |
| Geschaeftslogik testen | Ja | Nein |
| Protokoll-Interaktion testen | Bedingt | Besser echt |
| RabbitMQ Queue-Deklaration | Nein | Ja (docker-compose) |
| Valkey Lua-Scripts | Nein | Ja (docker-compose) |
| CI/CD Pipeline | Beides | Ja (docker-compose) |
| Schnelles lokales Feedback | Ja | Nein |

**Faustregel:** Mock die Grenze (den I/O-Aufruf), nicht die Logik. Wenn der Test primaer Geschaeftslogik testet, Mock verwenden. Wenn der Test die korrekte Interaktion mit einem externen System testet, echten Service verwenden.

### 9.5 pytest-mock fuer saubereres Mocking

```python
async def test_worker_logs_task_start(mocker):
    """pytest-mock bietet 'mocker' als Fixture."""
    mock_logger = mocker.patch("swarmq.worker.logger")

    worker = Worker(broker=AsyncMock())
    await worker.handle_task(task_data={"name": "send_email"})

    mock_logger.info.assert_called_once()
    assert "send_email" in str(mock_logger.info.call_args)
```

`pytest-mock` Vorteile gegenueber direktem `unittest.mock`:
- Automatisches Aufraeuemen (kein `with patch(...)` noetig)
- `mocker.patch` als Fixture statt Context-Manager
- Bessere Fehlermeldungen bei fehlgeschlagenen Assertions

---

## 10. Test-Patterns fuer Task Queues

### 10.1 Task-Ausfuehrung ohne Broker testen

```python
async def test_task_execution_without_broker():
    """Task direkt ausfuehren, ohne Broker-Infrastruktur."""
    @task(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    # Direkt aufrufen, nicht ueber den Broker
    result = await add(2, 3)
    assert result == 5
```

### 10.2 Middleware-Pipelines testen

```python
async def test_middleware_pipeline_executes_in_order():
    """Test dass Middlewares in der richtigen Reihenfolge ausgefuehrt werden."""
    call_order = []

    class LoggingMiddleware:
        async def before_task(self, context):
            call_order.append("log_before")
        async def after_task(self, context, result):
            call_order.append("log_after")

    class RetryMiddleware:
        async def before_task(self, context):
            call_order.append("retry_before")
        async def after_task(self, context, result):
            call_order.append("retry_after")

    pipeline = MiddlewarePipeline([LoggingMiddleware(), RetryMiddleware()])
    await pipeline.execute(context=TaskContext(...), handler=dummy_handler)

    assert call_order == ["log_before", "retry_before", "retry_after", "log_after"]
```

### 10.3 Retry-Logik testen

```python
async def test_task_is_retried_on_transient_error():
    call_count = 0

    async def flaky_handler(context):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("timeout")
        return "success"

    middleware = RetryMiddleware(max_retries=3)
    result = await middleware.execute(handler=flaky_handler)

    assert result == "success"
    assert call_count == 3

async def test_task_fails_after_max_retries():
    async def always_fails(context):
        raise ConnectionError("timeout")

    middleware = RetryMiddleware(max_retries=2)

    with pytest.raises(MaxRetriesExceeded):
        await middleware.execute(handler=always_fails)
```

### 10.4 Graceful Shutdown testen

```python
async def test_graceful_shutdown_waits_for_running_tasks():
    task_completed = asyncio.Event()
    shutdown_completed = asyncio.Event()

    async def slow_task():
        await asyncio.sleep(0.1)
        task_completed.set()

    worker = Worker(broker=AsyncMock())
    # Starte einen Task
    worker_task = asyncio.create_task(worker.execute(slow_task))

    # Initiiere Shutdown
    shutdown_task = asyncio.create_task(worker.shutdown(timeout=1.0))

    # Warte auf Shutdown
    await shutdown_task

    # Task muss abgeschlossen sein
    assert task_completed.is_set()

async def test_shutdown_cancels_tasks_after_timeout():
    async def very_slow_task():
        await asyncio.sleep(100)  # "Haengt"

    worker = Worker(broker=AsyncMock())
    worker_task = asyncio.create_task(worker.execute(very_slow_task))

    # Shutdown mit kurzem Timeout
    await worker.shutdown(timeout=0.1)

    assert worker_task.cancelled() or worker_task.done()
```

### 10.5 Signal-Handler testen

```python
import signal

async def test_sigterm_triggers_graceful_shutdown(mocker):
    worker = Worker(broker=AsyncMock())
    mock_shutdown = mocker.patch.object(worker, "shutdown", new=AsyncMock())

    # Simuliere SIGTERM
    worker._handle_signal(signal.SIGTERM, None)

    # Shutdown muss ausgeloest worden sein
    mock_shutdown.assert_awaited_once()

async def test_double_sigterm_forces_immediate_exit(mocker):
    worker = Worker(broker=AsyncMock())
    mock_exit = mocker.patch("sys.exit")

    # Erstes SIGTERM: graceful
    worker._handle_signal(signal.SIGTERM, None)
    mock_exit.assert_not_called()

    # Zweites SIGTERM: force exit
    worker._handle_signal(signal.SIGTERM, None)
    mock_exit.assert_called_once_with(1)
```

### 10.6 Distributed Locks testen

```python
async def test_distributed_lock_prevents_concurrent_execution():
    mock_valkey = AsyncMock()
    # Erster Acquire: erfolgreich
    mock_valkey.set = AsyncMock(side_effect=[True, False])

    lock = DistributedLock(client=mock_valkey, key="task:123:lock", ttl=30)

    assert await lock.acquire() is True
    assert await lock.acquire() is False  # Bereits gesperrt

async def test_distributed_lock_releases_on_completion():
    mock_valkey = AsyncMock()
    mock_valkey.set = AsyncMock(return_value=True)
    mock_valkey.delete = AsyncMock(return_value=1)

    lock = DistributedLock(client=mock_valkey, key="task:123:lock", ttl=30)

    async with lock:
        pass  # Task-Ausfuehrung

    mock_valkey.delete.assert_awaited_once()
```

---

# Teil 4: Test-Typen und ihre Rolle in TDD

## 11. Unit Tests -- Das primaere Werkzeug

### 11.1 Was ist eine "Unit"?

Es gibt keine universelle Definition. Fuer SwarmQ:

| Granularitaet | Beispiel | Empfehlung |
|---|---|---|
| Einzelne Funktion | `compute_backoff()` | Gut fuer reine Funktionen |
| Einzelne Klasse | `RetryMiddleware` | **Standard-Unit fuer SwarmQ** |
| Kleines Modul | `swarmq.serialization` | Akzeptabel wenn eng zusammenhaengend |

**Empfehlung:** Die Unit ist eine Klasse oder ein eng zusammenhaengendes Modul. Nicht kleiner (einzelne private Methoden) und nicht groesser (ganze Subsysteme).

### 11.2 Wie isoliert muessen Unit Tests sein?

**Puristischer Ansatz (London School):** Alle Abhaengigkeiten werden gemockt. Nur die getestete Klasse ist "real".

**Pragmatischer Ansatz (Chicago School):** Einfache Abhaengigkeiten (Value Objects, Datenklassen) sind real. Nur I/O und teure Operationen werden gemockt.

**Empfehlung fuer SwarmQ:** Pragmatischer Ansatz.
- **Mocken:** Broker-Verbindungen, Valkey-Client, Netzwerk-I/O, Dateisystem
- **Nicht mocken:** Datenklassen (`TaskInfo`, `TaskResult`), reine Funktionen (`compute_backoff`), Enums, einfache Helper

### 11.3 Performance-Ziel

- **Unit Tests:** < 100ms pro Test (idealerweise < 10ms)
- **Gesamte Unit-Test-Suite:** < 10 Sekunden
- **Wenn ein Unit Test laenger als 100ms dauert:** Er ist wahrscheinlich kein Unit Test (I/O-Zugriff? Sleep? Netzwerk?)

---

## 12. Integration Tests

### 12.1 Wann Integration Tests in TDD schreiben

Integration Tests werden in TDD geschrieben, wenn:
- Die Interaktion zwischen zwei oder mehr Komponenten getestet werden muss
- Ein Mock nicht ausreicht, um das reale Verhalten abzubilden (z.B. RabbitMQ Queue-Semantik)
- Lua-Scripts auf dem echten Valkey getestet werden muessen
- Das Zusammenspiel von Broker + Worker + Backend validiert werden muss

**TDD-Reihenfolge:** Erst Unit Tests fuer die einzelnen Komponenten, DANN Integration Tests fuer ihr Zusammenspiel.

### 12.2 Testing mit echtem RabbitMQ und Valkey

```python
# tests/integration/conftest.py
import pytest

@pytest.fixture(scope="session")
async def rabbitmq_url():
    """URL fuer den RabbitMQ Container aus docker-compose."""
    return "amqp://guest:guest@localhost:5672/"

@pytest.fixture(scope="session")
async def valkey_url():
    """URL fuer den Valkey Container aus docker-compose."""
    return "valkey://localhost:6379/0"

@pytest.fixture
async def clean_broker(rabbitmq_url):
    """Broker mit frischen Queues fuer jeden Test."""
    broker = RabbitMQBroker(url=rabbitmq_url)
    await broker.connect()
    yield broker
    # Cleanup: Queues loeschen
    await broker.delete_all_queues()
    await broker.disconnect()
```

### 12.3 Integration Tests schnell halten

| Technik | Beschreibung |
|---|---|
| Session-Scope Fixtures | Verbindungen einmal aufbauen, fuer alle Tests nutzen |
| Separate Queues pro Test | Statt Queue-Cleanup: eindeutige Queue-Namen pro Test |
| Parallele Ausfuehrung | `pytest-xdist` fuer parallel laufende Tests |
| Kein Sleep | Statt `asyncio.sleep()` auf Events warten |
| Timeouts | Jeder Integration-Test bekommt einen Timeout (pytest-timeout) |

---

## 13. End-to-End Tests und die Test-Pyramide

### 13.1 Die Test-Pyramide

```
        /\
       /E2E\        ← Wenige (5-10): Gesamtsystem, Hauptszenarien
      /------\
     /Integr. \     ← Einige (20-50): Komponenten-Interaktion
    /----------\
   / Unit Tests \   ← Viele (100+): Einzelne Klassen/Funktionen
  /--------------\
```

**Verhaeltnis fuer SwarmQ:**
- **80% Unit Tests:** Schnell, isoliert, testen Geschaeftslogik
- **15% Integration Tests:** Testen Broker/Backend-Interaktion mit echten Services
- **5% E2E Tests:** Testen den kompletten Workflow (Task senden → Worker verarbeitet → Ergebnis abrufbar)

### 13.2 Wann E2E Tests in TDD schreiben

E2E Tests sind in TDD eher die Ausnahme. Sie werden geschrieben fuer:
- Kritische User-Journeys ("Task wird gesendet und Ergebnis kommt zurueck")
- Smoke-Tests nach Deployments
- Regressionstests fuer Bugs, die nur im Gesamtsystem auftraten

**Nicht** fuer: Einzelne Features, Edge Cases, Fehlerbehandlung (dafuer Unit/Integration Tests).

---

# Teil 5: Anti-Patterns und Test-Smells

## 14. Haeufige TDD-Fehler

### 14.1 Test-After Development (TAD)

**Das Anti-Pattern:** Code schreiben, dann Tests dafuer schreiben.

**Warum es nicht TDD ist:**
- Die Tests bestaetigen nur, was der Code bereits tut (Confirmation Bias)
- Schwer testbarer Code wird nicht durch Tests verbessert, sondern Tests werden um den Code herum gebogen
- Kein Design-Feedback -- die Tests "hoeren" nicht auf Design-Probleme
- Tests werden oberflaechlicher, weil man "weiss", dass der Code funktioniert

### 14.2 Zu viele Tests auf einmal schreiben

**Das Anti-Pattern:** Fuenf Tests schreiben, dann alle fuenf implementieren.

**Warum problematisch:**
- Man verliert den engen Feedback-Loop
- Man neigt dazu, zu viel Code auf einmal zu schreiben
- Wenn mehrere Tests gleichzeitig rot sind, ist unklar welcher zuerst adressiert werden soll

**Regel:** IMMER nur EIN roter Test gleichzeitig.

### 14.3 Implementierungsdetails statt Verhalten testen

**Schlecht -- testet WIE:**
```python
def test_task_uses_dict_for_storage():
    task = Task(name="test")
    assert isinstance(task._storage, dict)  # Implementierungsdetail!

def test_retry_calls_sleep():
    """Testet dass sleep aufgerufen wird -- Implementierungsdetail."""
    with patch("asyncio.sleep") as mock_sleep:
        await retry(task)
        mock_sleep.assert_called_once_with(1.0)  # Fragil!
```

**Gut -- testet WAS:**
```python
def test_task_stores_and_retrieves_metadata():
    task = Task(name="test")
    task.set_metadata("key", "value")
    assert task.get_metadata("key") == "value"

async def test_retry_waits_before_next_attempt():
    """Testet das Verhalten (Wartezeit), nicht die Implementierung (sleep)."""
    start = time.monotonic()
    await retry(task, delay=0.1)
    elapsed = time.monotonic() - start
    assert elapsed >= 0.1
```

### 14.4 Over-Mocking

**Das Anti-Pattern:** So viel mocken, dass man nur noch die Mocks testet.

```python
# SCHLECHT: Man testet hier nur, ob Mock-Aufrufe in der richtigen Reihenfolge passieren
async def test_task_processing(mocker):
    mock_deserialize = mocker.patch("swarmq.deserialize", return_value={"task": "test"})
    mock_validate = mocker.patch("swarmq.validate", return_value=True)
    mock_execute = mocker.patch("swarmq.execute", return_value="done")
    mock_store = mocker.patch("swarmq.store_result")

    await process_task(message)

    mock_deserialize.assert_called_once()
    mock_validate.assert_called_once()
    mock_execute.assert_called_once()
    mock_store.assert_called_once()
    # Was wird hier eigentlich getestet? Dass process_task vier Funktionen aufruft.
    # Nicht ob es KORREKT funktioniert.
```

**Faustregel:** Wenn mehr als 50% des Testcodes aus Mock-Setup besteht, wird zu viel gemockt. Entweder die Klasse hat zu viele Abhaengigkeiten (Design-Problem) oder man sollte einen Integration-Test schreiben.

### 14.5 Refactoring-Phase ueberspringen

**Das Anti-Pattern:** Red → Green → Red → Green → Red → Green ... (ohne je zu refactoren)

**Konsequenz:** Der Code wird zunehmend chaotisch. Duplikation haeuft sich an. Irgendwann ist der Code so unlesbar, dass neue Tests schwer zu schreiben sind.

**Regel:** Mindestens nach jedem dritten Gruen bewusst nach Refactoring-Moeglichkeiten suchen.

### 14.6 Flaky Tests

**Das Anti-Pattern:** Tests die manchmal bestehen und manchmal fehlschlagen.

**Haeufige Ursachen in async Python:**
- `asyncio.sleep()` mit festen Zeiten statt Events/Conditions
- Race Conditions bei parallelen Tasks
- Abhaengigkeit von der Reihenfolge der Test-Ausfuehrung
- Nicht aufgeraeumter Zustand zwischen Tests (globale Variablen, Singletons)

**Loesung:** Statt auf Zeit zu warten, auf Events warten:
```python
# SCHLECHT: Zeitbasiert, flaky
async def test_worker_processes_task():
    await worker.submit(task)
    await asyncio.sleep(0.5)  # Hoffentlich fertig...
    assert worker.completed_count == 1

# GUT: Event-basiert, deterministisch
async def test_worker_processes_task():
    result_future = await worker.submit(task)
    result = await asyncio.wait_for(result_future, timeout=5.0)
    assert result is not None
```

### 14.7 Private Methoden direkt testen

**Das Anti-Pattern:**
```python
def test_internal_parse_method():
    parser = MessageParser()
    result = parser._parse_header(b"...")  # Private Methode!
    assert result == {"content_type": "json"}
```

**Warum problematisch:**
- Private Methoden sind Implementierungsdetails
- Der Test bricht bei jedem Refactoring
- Die oeffentliche API ist das, was getestet werden sollte

**Besser:**
```python
def test_parser_extracts_content_type():
    parser = MessageParser()
    message = parser.parse(raw_message)
    assert message.content_type == "json"
```

---

## 15. Test-Smells erkennen und beheben

### 15.1 Tests die bei Refactoring brechen

**Symptom:** Man refactored Produktionscode (ohne Verhaltensaenderung) und Tests werden rot.

**Ursache:** Tests sind an Implementierungsdetails gekoppelt (private Methoden, interne Datenstrukturen, Mock-Aufruf-Reihenfolge).

**Loesung:** Tests umschreiben, sodass sie Verhalten testen, nicht Implementierung.

### 15.2 Tests die nie fehlschlagen

**Symptom:** Ein Test ist seit Monaten gruen und hat nie eine Regression gefangen.

**Moegliche Ursachen:**
- Der Test testet etwas Triviales (Getter/Setter ohne Logik)
- Der Test testet das Framework statt eigenen Code
- Die Assertions sind zu schwach (`assert result is not None` statt konkreter Werte)

### 15.3 Tests mit komplexem Setup

**Symptom:** 30 Zeilen Setup fuer 3 Zeilen Test.

**Ursache:** Die getestete Klasse hat zu viele Abhaengigkeiten -- ein Design-Problem!

**Loesung:** "Hoere auf die Tests" -- komplexer Test-Setup ist ein Signal, dass die Klasse zu viel tut und aufgeteilt werden sollte.

### 15.4 Langsame Tests

**Symptom:** Die Test-Suite braucht Minuten statt Sekunden.

**Ursachen und Loesungen:**

| Ursache | Loesung |
|---|---|
| I/O in Unit Tests | Mocks verwenden |
| Unnoetige Sleep-Aufrufe | Events/Conditions verwenden |
| Teure Fixture-Erstellung | Scope erhoehen (session/module) |
| Zu viele Integration Tests | Mehr Unit Tests, weniger Integration |

---

# Teil 6: TDD und Architektur

## 16. Wie TDD Design beeinflusst

### 16.1 Loose Coupling durch Testbarkeit

Wenn eine Klasse schwer zu testen ist, hat sie zu viele Abhaengigkeiten. TDD erzwingt natuerlich:

**Vor TDD (schwer testbar):**
```python
class TaskProcessor:
    def __init__(self):
        self.broker = RabbitMQBroker("amqp://localhost")  # Hardcoded!
        self.backend = ValkeyBackend("valkey://localhost")  # Hardcoded!
        self.logger = logging.getLogger()  # Global state!
```

**Nach TDD (testbar durch Dependency Injection):**
```python
class TaskProcessor:
    def __init__(
        self,
        broker: BaseBroker,
        backend: BaseBackend,
        logger: Logger,
    ) -> None:
        self.broker = broker
        self.backend = backend
        self.logger = logger
```

TDD hat die Klasse nicht nur testbar gemacht, sondern auch flexibler, wiederverwendbarer und austauschbarer.

### 16.2 Interfaces/Protocols aus Mocking-Beduerfnissen

Wenn man einen Mock fuer eine Abhaengigkeit erstellt, definiert man implizit ein Interface. In Python werden daraus Protocols:

```python
from typing import Protocol

class Broker(Protocol):
    async def publish(self, queue: str, body: bytes) -> None: ...
    async def consume(self, queue: str) -> AsyncIterator[Message]: ...
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
```

Dieses Protocol entstand nicht durch Vorausplanung, sondern durch das Schreiben von Tests, die einen Mock brauchten.

### 16.3 "Hoere auf die Tests"

Dies ist Kent Becks wichtigste Design-Heuristik:

| Test-Signal | Design-Problem | Loesung |
|---|---|---|
| Test braucht viele Mocks | Klasse hat zu viele Abhaengigkeiten | Single Responsibility, Klasse aufteilen |
| Test-Setup ist komplex | Objekt-Erstellung ist komplex | Builder/Factory einfuehren |
| Test fuer eine Methode ist riesig | Methode tut zu viel | Methode in kleinere aufteilen |
| Aehnliche Tests in verschiedenen Dateien | Duplikation ueber Module | Gemeinsame Logik extrahieren |
| Test muss in globalen State eingreifen | Globaler Zustand, Singletons | Dependency Injection |

---

## 17. London School vs. Chicago School

### 17.1 London School (Mockist TDD)

**Kernidee:** Jede Klasse wird isoliert getestet. Alle Kollaborateure werden gemockt. Tests verifizieren Interaktionen (Methodenaufrufe).

```python
# London School: Alle Abhaengigkeiten sind Mocks
async def test_worker_acknowledges_after_success(mocker):
    broker = mocker.Mock(spec=BaseBroker)
    backend = mocker.Mock(spec=BaseBackend)
    message = AsyncMock()

    worker = Worker(broker=broker, backend=backend)
    await worker.process(message)

    message.ack.assert_awaited_once()
    backend.store_result.assert_awaited_once()
```

**Vorteile:**
- Extrem isoliert -- ein Fehler bricht genau einen Test
- Erzwingt klare Interfaces zwischen Komponenten
- Schnell (kein realer I/O)

**Nachteile:**
- Fragil: Refactoring bricht Tests (Mock-Erwartungen aendern sich)
- Kann "falsch gruen" sein (Mocks verhalten sich anders als reale Objekte)
- Hoher Wartungsaufwand

### 17.2 Chicago School (Classical TDD)

**Kernidee:** Echte Objekte verwenden, wo moeglich. Nur externe Abhaengigkeiten (I/O) mocken. Tests verifizieren Zustand (Ergebniswerte).

```python
# Chicago School: Echte Objekte, nur I/O gemockt
async def test_retry_middleware_retries_three_times():
    attempts = 0

    async def failing_handler(ctx):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient error")
        return "success"

    middleware = RetryMiddleware(max_retries=3)  # Echtes Objekt!
    result = await middleware.execute(handler=failing_handler)

    assert result == "success"
    assert attempts == 3
```

**Vorteile:**
- Robuster gegen Refactoring (testet Verhalten, nicht Aufrufe)
- Realistischer (echte Objekte verhalten sich wie in Produktion)
- Weniger Mock-Boilerplate

**Nachteile:**
- Ein Fehler in einer Abhaengigkeit bricht mehrere Tests
- Kann langsamer sein (echte Objekte tun echte Arbeit)

### 17.3 Empfehlung fuer SwarmQ

**Pragmatischer Mittelweg -- Chicago-School-lastig:**

| Komponente | Teststrategie |
|---|---|
| Reine Logik (Backoff, Serialisierung, Routing) | Chicago: Echte Objekte, kein Mocking |
| Middleware-Pipeline | Chicago: Echte Middleware-Objekte, Handler als Lambda/Closure |
| Worker | London: Broker und Backend mocken (I/O-Grenze) |
| Broker-Adapter | Integration: Echter RabbitMQ (docker-compose) |
| Backend-Adapter | Integration: Echtes Valkey (docker-compose) |
| CLI | London: Worker/Broker/Backend mocken |

**Begruendung:** SwarmQ ist eine I/O-lastige Anwendung. Die Geschaeftslogik (Retry, Routing, Middleware) laesst sich hervorragend mit echten Objekten testen. Nur an der I/O-Grenze (Broker, Backend) wird gemockt. Die tatsaechliche I/O-Interaktion wird in separaten Integration-Tests mit echten Services validiert.

---

# Teil 7: TDD-Regeln fuer einen AI Coding Agent

## 18. Operative Regeln -- Das vollstaendige Regelwerk

Dieser Abschnitt ist das Herzstuck des Dokuments. Er enthaelt alle Regeln, die ein AI Coding Agent befolgen muss, um TDD rigoros zu praktizieren.

---

### 18.1 Grundprinzip

**TDD ist nicht optional.** Fuer jede Aenderung am Produktionscode MUSS zuerst ein fehlschlagender Test existieren. Es gibt KEINE Ausnahme fuer "einfachen Code", "offensichtliche Aenderungen" oder "Infrastruktur-Code".

**Einzige Ausnahmen:**
- Reine Konfigurationsdateien (`pyproject.toml`, `docker-compose.yml`)
- Type-Stubs und `__init__.py` ohne Logik
- Reine Re-Exports in `__init__.py`

---

### 18.2 Bevor irgendein Produktionscode geschrieben wird

**Schritt 1: Test-Datei erstellen (falls nicht vorhanden)**

Fuer jede Produktionscode-Datei `src/swarmq/<modul>.py` existiert eine korrespondierende Testdatei `tests/unit/test_<modul>.py`.

```
src/swarmq/task.py          → tests/unit/test_task.py
src/swarmq/worker.py        → tests/unit/test_worker.py
src/swarmq/middleware.py     → tests/unit/test_middleware.py
src/swarmq/broker/rabbitmq.py → tests/unit/broker/test_rabbitmq.py
```

**Schritt 2: Test-Liste erstellen**

Vor der Implementierung eines Features eine informelle Test-Liste anlegen (als Kommentar im Test-File oder mental). Diese Liste wird waehrend der Implementierung aktualisiert.

```python
# tests/unit/test_retry.py
"""
Test-Liste fuer Retry-Logik:
- [ ] Default max_retries ist 3
- [ ] max_retries=0 deaktiviert Retry
- [ ] Erfolgreicher Task wird nicht retried
- [ ] Fehlgeschlagener Task wird retried
- [ ] Nach max_retries wird MaxRetriesExceeded geworfen
- [ ] Backoff zwischen Retries waechst exponentiell
- [ ] Backoff ueberschreitet nie max_backoff
- [ ] Bestimmte Exceptions koennen von Retry ausgenommen werden
"""
```

**Schritt 3: Den einfachsten Test waehlen und schreiben**

Immer mit dem einfachsten, unabhaengigsten Test beginnen.

---

### 18.3 Der exakte Zyklus -- Schritt fuer Schritt

**Zyklus-Schritt 1: EINEN fehlschlagenden Test schreiben**

```python
# Genau EINEN Test schreiben. Nicht zwei, nicht drei.
async def test_retry_middleware_default_max_retries():
    middleware = RetryMiddleware()
    assert middleware.max_retries == 3
```

Regeln:
- Der Test muss das VERHALTEN beschreiben, nicht die Implementierung
- Der Testname muss den getesteten Aspekt kommunizieren
- Der Test folgt dem AAA-Pattern (Arrange-Act-Assert)
- Maximal eine logische Assertion (mehrere zusammenhaengende `assert`-Statements sind erlaubt)

**Zyklus-Schritt 2: Test ausfuehren und ROTES Ergebnis verifizieren**

```bash
pytest tests/unit/test_retry.py::test_retry_middleware_default_max_retries -x
```

Regeln:
- Der Test MUSS fehlschlagen
- Wenn der Test gruen ist: Er testet nichts Neues -- loeschen oder aendern
- Der Fehlergrund muss KORREKT sein (ImportError/AttributeError fuer nicht-existierenden Code, AssertionError fuer falsches Verhalten)
- NIEMALS diesen Schritt ueberspringen -- auch wenn "offensichtlich" ist, dass der Test fehlschlagen wird

**Zyklus-Schritt 3: MINIMALEN Produktionscode schreiben**

```python
# src/swarmq/middleware.py
class RetryMiddleware:
    def __init__(self, max_retries: int = 3) -> None:
        self.max_retries = max_retries
```

Regeln:
- NUR so viel Code schreiben, wie noetig ist, damit der Test gruen wird
- Hardcodierte Werte sind erlaubt und oft richtig
- Keine zusaetzliche Funktionalitaet, die kein Test verlangt
- Keine vorausschauende Architektur ("das brauchen wir spaeter bestimmt")
- Kent Becks Strategien anwenden: Fake It, Triangulation oder Obvious Implementation

**Zyklus-Schritt 4: ALLE Tests ausfuehren und GRUENES Ergebnis verifizieren**

```bash
pytest tests/ -x
```

Regeln:
- ALLE Tests ausfuehren, nicht nur den neuen
- ALLE Tests muessen gruen sein
- Wenn ein anderer Test rot wird: Der neue Code hat eine Regression verursacht -- sofort fixen, bevor weitergemacht wird
- Kein neuer Test, solange nicht alle bestehenden Tests gruen sind

**Zyklus-Schritt 5: Refactoring (optional, aber regelmaessig)**

Regeln:
- Produktionscode UND Testcode betrachten
- Duplikation entfernen, Namen verbessern, Struktur vereinfachen
- Nach JEDEM Refactoring-Schritt: Alle Tests ausfuehren
- Wenn ein Test rot wird: Refactoring rueckgaengig machen
- Kein neues Verhalten waehrend des Refactorings einfuehren
- Mindestens nach jedem dritten Zyklus bewusst nach Refactoring-Moeglichkeiten suchen

**Zyklus-Schritt 6: Alle Tests erneut ausfuehren**

```bash
pytest tests/ -x
```

Sicherstellen, dass nach dem Refactoring noch alles gruen ist.

**Zyklus-Schritt 7: Zurueck zu Schritt 1**

Den naechsten Test aus der Test-Liste waehlen und den Zyklus wiederholen.

---

### 18.4 Entscheidungsregeln

**Welchen Test als naechstes schreiben?**
1. Den einfachsten noch nicht abgedeckten Fall
2. Den naechsten logischen Schritt in der Feature-Entwicklung
3. Einen Edge-Case, der einem waehrend der Implementierung aufgefallen ist
4. Einen Test, der eine Hardcodierung aus dem vorherigen Zyklus aufbricht (Triangulation)

**Wann ist ein Feature "fertig"?**
- Alle Tests der Test-Liste sind geschrieben und gruen
- Edge-Cases sind abgedeckt: Null/None, leere Collections, Grenzwerte, Fehlerfaelle
- Der Code ist refactored und sauber
- Man kann keinen weiteren sinnvollen fehlschlagenden Test formulieren

**Unit Test oder Integration Test?**
- **Unit Test:** Wenn die zu testende Logik isoliert von externen Systemen funktioniert
- **Integration Test:** Wenn das korrekte Zusammenspiel mit RabbitMQ, Valkey oder dem Dateisystem getestet werden muss
- **Faustregel:** Im Zweifel Unit Test. Integration Tests nur fuer I/O-Grenzen.

**Wann mocken, wann echte Objekte?**
- **Mocken:** I/O-Operationen (Netzwerk, Dateisystem), externe Services (Broker, Backend), Systemaufrufe (time, signal)
- **Nicht mocken:** Datenklassen, Value Objects, reine Funktionen, einfache Abhaengigkeiten ohne I/O
- **Faustregel:** "Mock the boundaries, not the internals"

**Wie Fixtures und Factories handhaben?**
- Fixtures fuer teure Setup-Operationen (Verbindungen, komplexe Objektgraphen)
- Factory-Funktionen fuer wiederverwendbare Objekt-Erstellung mit sinnvollen Defaults
- KEIN Over-Engineering von Test-Infrastruktur in der Anfangsphase -- bei Bedarf refactoren

---

### 18.5 Qualitaets-Gates

Diese Gates muessen zu jedem Zeitpunkt erfuellt sein:

| Gate | Beschreibung |
|---|---|
| **Alle Tests gruen** | Kein Commit, kein neues Feature, wenn Tests rot sind |
| **Kein ungetesteter Produktionscode** | Jede Zeile Produktionscode wurde durch einen fehlschlagenden Test motiviert |
| **Jeder Test hat einen klaren Zweck** | Der Testname beschreibt das getestete Verhalten |
| **Tests sind schnell** | Unit Tests < 100ms, Gesamte Suite < 30 Sekunden |
| **Tests sind deterministisch** | Kein Test darf manchmal bestehen und manchmal fehlschlagen |
| **Tests sind unabhaengig** | Kein Test darf von der Ausfuehrungsreihenfolge abhaengen |

**Coverage als Nebenprodukt:**
- Test-Coverage wird gemessen, aber NICHT als Ziel optimiert
- Hohe Coverage ist ein Nebenprodukt von konsequentem TDD, nicht umgekehrt
- 100% Coverage ist kein Qualitaetsindikator (man kann 100% Coverage mit schlechten Tests haben)
- Realistisches Ziel: 85-95% Line Coverage fuer Geschaeftslogik

---

### 18.6 Verbotene Aktionen

Ein AI Coding Agent darf NIEMALS:

1. **Produktionscode ohne fehlschlagenden Test schreiben** -- auch nicht "nur eine Hilfsfunktion", "nur ein Dataclass-Feld", "nur ein Import"
2. **Mehr als einen fehlschlagenden Test gleichzeitig haben** -- immer nur ein roter Test
3. **Tests ueberspringen oder deaktivieren** (`@pytest.mark.skip`, `pytest.mark.xfail`) -- ausser fuer bekannte, dokumentierte Bugs mit Ticket-Nummer
4. **Den Red-Schritt ueberspringen** ("Ich weiss, dass der Test fehlschlaegt, also fuehre ich ihn nicht aus") -- IMMER ausfuehren
5. **Waehrend des Refactorings neues Verhalten einfuehren** -- Refactoring aendert die Struktur, nicht das Verhalten
6. **Tests fuer private Methoden schreiben** (Methoden mit `_`-Prefix) -- die oeffentliche API testen
7. **Implementierungsdetails in Assertions verwenden** -- Verhalten testen, nicht interne Struktur
8. **Committen mit roten Tests** -- NIEMALS

---

### 18.7 Erlaubte Abkuerzungen

Der Agent DARF:

1. **"Obvious Implementation" verwenden**, wenn die Loesung trivial ist -- aber sofort zu "Fake It" zurueckwechseln, wenn der Test nicht beim ersten Versuch gruen wird
2. **Mehrere zusammenhaengende Assertions in einem Test** verwenden, wenn sie denselben Verhaltensaspekt testen
3. **Fixture-Code ohne eigene Tests** erstellen (Fixtures sind Test-Infrastruktur, nicht Produktionscode)
4. **Type-Hints ohne Tests** hinzufuegen (sie sind Dokumentation, nicht Verhalten)
5. **Mehrere Zyklen zu einem Commit** zusammenfassen (logische Feature-Inkremente)
6. **Die Test-Liste waehrend der Implementierung aendern** (hinzufuegen, streichen, umformulieren)

---

### 18.8 Template: So sieht ein TDD-Zyklus aus

```
=== ZYKLUS N: [Beschreibung des zu testenden Verhaltens] ===

--- RED ---
[Test schreiben]
[Test ausfuehren]
[Erwartetes Rot verifizieren: <Fehlertyp und -grund>]

--- GREEN ---
[Minimalen Produktionscode schreiben]
[ALLE Tests ausfuehren]
[Alle gruen: ja/nein]
[Falls nein: Regression fixen, dann weiter]

--- REFACTOR (optional) ---
[Identifizierte Verbesserung: <was und warum>]
[Refactoring durchfuehren]
[ALLE Tests ausfuehren]
[Alle gruen: ja/nein]

=== ZYKLUS N+1: [Naechstes Verhalten] ===
```

---

### 18.9 Spezialfall: Async-Code testen

Fuer SwarmQ-spezifischen async Code gelten zusaetzliche Regeln:

1. **Async Tests** verwenden `async def test_...()` mit pytest-asyncio im Auto-Mode
2. **AsyncMock** fuer async Abhaengigkeiten: `from unittest.mock import AsyncMock`
3. **Timeouts** fuer alle async Tests: `@pytest.mark.timeout(5)` oder globaler Default
4. **Keine `asyncio.sleep()` in Tests** -- stattdessen Events und Conditions verwenden
5. **Event-Loop nicht manuell verwalten** -- pytest-asyncio uebernimmt das
6. **Async Fixtures** fuer async Setup/Teardown: `@pytest.fixture` mit `async def`

---

### 18.10 Spezialfall: Bestehenden Code aendern

Wenn bestehender Code geaendert werden soll (Bug-Fix, Feature-Erweiterung):

1. **Erst den Bug reproduzieren:** Einen Test schreiben, der den Bug demonstriert (Rot)
2. **Dann den Bug fixen:** Minimaler Code, damit der Test gruen wird
3. **Dann refactoren:** Falls der Fix unsauber war
4. **Regression sicherstellen:** Der Test bleibt dauerhaft in der Suite

Fuer Feature-Erweiterungen:
1. **Bestehende Tests verifizieren:** Alle gruen? Gut.
2. **Neuen Test schreiben:** Fuer das neue Verhalten (Rot)
3. **Implementieren:** Minimaler Code (Gruen)
4. **Refactoren:** Code und Tests aufraeuemen
5. **Bestehende Tests erneut pruefen:** Keine Regressionen?

---

# Teil 8: Praktische pytest-Konfiguration

## 19. Konfiguration und Plugin-Empfehlungen

### 19.1 pyproject.toml Konfiguration

```toml
[tool.pytest.ini_options]
# Test-Discovery
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
python_classes = ["Test*"]

# Async
asyncio_mode = "auto"

# Ausgabe
console_output_style = "progress"
verbosity = 1

# Warnungen
filterwarnings = [
    "error",  # Warnungen als Fehler behandeln
    "ignore::DeprecationWarning:aio_pika.*",
]

# Marker
markers = [
    "unit: Unit Tests (schnell, isoliert)",
    "integration: Integration Tests (brauchen externe Services)",
    "e2e: End-to-End Tests (brauchen vollstaendige Infrastruktur)",
    "slow: Langsame Tests die nicht bei jedem Durchlauf laufen muessen",
]

# Timeouts
timeout = 10  # Default-Timeout fuer alle Tests: 10 Sekunden
timeout_method = "thread"

# Minimale pytest-Version
minversion = "8.0"
```

### 19.2 Plugin-Empfehlungen

| Plugin | Zweck | Prioritaet |
|---|---|---|
| `pytest-asyncio` | Async Test-Unterstuetzung | Pflicht |
| `pytest-mock` | Sauberes Mocking via `mocker` Fixture | Pflicht |
| `pytest-cov` | Coverage-Messung | Empfohlen |
| `pytest-timeout` | Test-Timeouts (gegen haengende Tests) | Pflicht |
| `pytest-xdist` | Parallele Test-Ausfuehrung | Empfohlen fuer CI |
| `pytest-randomly` | Zufaellige Test-Reihenfolge (findet Abhaengigkeiten) | Empfohlen |
| `pytest-clarity` | Bessere Diff-Ausgabe bei Failures | Optional |

### 19.3 Installation

```bash
pip install pytest pytest-asyncio pytest-mock pytest-cov pytest-timeout pytest-xdist pytest-randomly
```

Oder in `pyproject.toml`:
```toml
[project.optional-dependencies]
test = [
    "pytest>=8.0",
    "pytest-asyncio>=0.25",
    "pytest-mock>=3.14",
    "pytest-cov>=6.0",
    "pytest-timeout>=2.3",
    "pytest-xdist>=3.5",
    "pytest-randomly>=3.16",
]
```

---

## 20. Verzeichnisstruktur und Fixture-Organisation

### 20.1 Empfohlene Verzeichnisstruktur

```
swarmq_v2/
├── src/
│   └── swarmq/
│       ├── __init__.py
│       ├── task.py
│       ├── worker.py
│       ├── middleware.py
│       ├── serialization.py
│       ├── broker/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   └── rabbitmq.py
│       └── backend/
│           ├── __init__.py
│           ├── base.py
│           └── valkey.py
├── tests/
│   ├── conftest.py              ← Gemeinsame Fixtures (alle Test-Typen)
│   ├── unit/
│   │   ├── conftest.py          ← Unit-spezifische Fixtures
│   │   ├── test_task.py
│   │   ├── test_worker.py
│   │   ├── test_middleware.py
│   │   ├── test_serialization.py
│   │   └── broker/
│   │       └── test_rabbitmq.py
│   ├── integration/
│   │   ├── conftest.py          ← Integration-Fixtures (Broker/Backend-Connections)
│   │   ├── test_broker_rabbitmq.py
│   │   └── test_backend_valkey.py
│   └── e2e/
│       ├── conftest.py          ← E2E-Fixtures (komplette Infrastruktur)
│       └── test_task_lifecycle.py
├── pyproject.toml
└── docker-compose.yml           ← RabbitMQ + Valkey fuer Integration/E2E Tests
```

### 20.2 Fixture-Organisation

**`tests/conftest.py` -- Gemeinsame Fixtures:**
```python
"""Gemeinsame Fixtures fuer alle Test-Typen."""
import pytest

@pytest.fixture
def task_factory():
    """Factory fuer Task-Objekte mit sinnvollen Defaults."""
    def _create(**kwargs):
        defaults = {
            "name": "test_task",
            "queue": "default",
            "max_retries": 3,
        }
        defaults.update(kwargs)
        from swarmq.task import Task
        return Task(**defaults)
    return _create

@pytest.fixture
def sample_message():
    """Eine Beispiel-Nachricht fuer Tests."""
    return {
        "task": "send_email",
        "args": ["user@example.com"],
        "kwargs": {"subject": "Test"},
        "task_id": "test-123",
    }
```

**`tests/unit/conftest.py` -- Unit-spezifische Fixtures:**
```python
"""Fixtures fuer Unit Tests. Kein I/O, keine externen Services."""
import pytest
from unittest.mock import AsyncMock

@pytest.fixture
def mock_broker():
    """Mock-Broker fuer Unit Tests."""
    broker = AsyncMock()
    broker.publish = AsyncMock()
    broker.consume = AsyncMock(return_value=[])
    return broker

@pytest.fixture
def mock_backend():
    """Mock-Backend fuer Unit Tests."""
    backend = AsyncMock()
    backend.store_result = AsyncMock()
    backend.get_result = AsyncMock(return_value=None)
    return backend
```

**`tests/integration/conftest.py` -- Integration-Fixtures:**
```python
"""Fixtures fuer Integration Tests. Brauchen docker-compose Services."""
import pytest

@pytest.fixture(scope="session")
async def rabbitmq_broker():
    """Echte RabbitMQ-Verbindung (session-scoped fuer Performance)."""
    from swarmq.broker.rabbitmq import RabbitMQBroker
    broker = RabbitMQBroker(url="amqp://guest:guest@localhost:5672/")
    await broker.connect()
    yield broker
    await broker.disconnect()

@pytest.fixture(scope="session")
async def valkey_backend():
    """Echte Valkey-Verbindung (session-scoped fuer Performance)."""
    from swarmq.backend.valkey import ValkeyBackend
    backend = ValkeyBackend(url="valkey://localhost:6379/0")
    await backend.connect()
    yield backend
    await backend.disconnect()

@pytest.fixture
async def clean_queue(rabbitmq_broker):
    """Frische Queue fuer jeden Test."""
    import uuid
    queue_name = f"test_{uuid.uuid4().hex[:8]}"
    await rabbitmq_broker.declare_queue(queue_name)
    yield queue_name
    await rabbitmq_broker.delete_queue(queue_name)
```

### 20.3 Tests effizient ausfuehren

```bash
# Alle Unit Tests (schnell, kein Docker noetig)
pytest tests/unit/ -x

# Alle Integration Tests (brauchen docker-compose)
pytest tests/integration/ -x

# Nur einen bestimmten Test
pytest tests/unit/test_retry.py::test_backoff_is_exponential -x

# Mit Coverage
pytest tests/ --cov=swarmq --cov-report=term-missing

# Parallel (CI)
pytest tests/ -x -n auto

# Nur Tests mit bestimmtem Marker
pytest tests/ -m "unit" -x
pytest tests/ -m "not slow" -x

# Letzten fehlgeschlagenen Test erneut ausfuehren
pytest tests/ --lf

# Tests in zufaelliger Reihenfolge (findet versteckte Abhaengigkeiten)
pytest tests/ -p randomly
```

### 20.4 docker-compose fuer Integration Tests

```yaml
# docker-compose.yml
services:
  rabbitmq:
    image: rabbitmq:4-management-alpine
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "check_port_connectivity"]
      interval: 5s
      timeout: 5s
      retries: 5

  valkey:
    image: valkey/valkey:8-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "valkey-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5
```

```bash
# Services starten
docker compose up -d

# Warten bis healthy
docker compose up -d --wait

# Integration Tests ausfuehren
pytest tests/integration/ -x

# Services stoppen
docker compose down
```

---

## Zusammenfassung

Dieses Dokument definiert den vollstaendigen TDD-Prozess fuer SwarmQ. Die Kernpunkte:

1. **Der Zyklus ist heilig:** Red → Green → Refactor. Keine Abkuerzungen, keine Ausnahmen.
2. **Ein Test auf einmal:** Immer nur ein roter Test. Immer den einfachsten Fall zuerst.
3. **Minimaler Code:** Nur so viel Produktionscode wie noetig. Hardcodierung ist erlaubt.
4. **Verhalten testen, nicht Implementierung:** Tests die bei Refactoring brechen, sind schlechte Tests.
5. **Mock the boundaries:** I/O mocken, Geschaeftslogik mit echten Objekten testen.
6. **Hoere auf die Tests:** Schwierige Tests sind ein Signal fuer Design-Probleme.
7. **Abschnitt 18 ist das operative Regelwerk** fuer den AI Coding Agent -- es kann direkt als Skill-Dokument extrahiert werden.
