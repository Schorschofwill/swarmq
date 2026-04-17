---
title: "DX: Library Entrypoint vs. Design-Prototype — Convenience Gap"
type: feature-brief
status: draft
date: 2026-04-17
origin: docs/brainstorms/2026-04-16-user-documentation-requirements.md
triggered_by: "Doc-Erstentwurf index.md + getting-started.md am 2026-04-17"
---

# DX: Library Entrypoint vs. Design-Prototype — Convenience Gap

## Problem Frame

Beim Schreiben des ersten Doku-Entwurfs (`website/index.md`,
`website/getting-started.md`) ist eine Diskrepanz zwischen dem
Design-Prototyp und der tatsächlichen Library-API aufgefallen. Das
Mockup zeigt eine kürzere, "friendlier" Client-Oberfläche, als die
Library heute bietet. Die Doku wurde gegen die **echte API**
geschrieben, damit Beispiele copy-paste-runnable sind (Recherche §1
TL;DR #5). Dieser Brainstorm dokumentiert die Lücke und macht sie für
eine spätere Entscheidung sichtbar.

Dieses Dokument entscheidet **nicht**, ob die Library erweitert wird —
es trägt nur zusammen, was genau abweicht, und welche Optionen
bestehen.

## Observed Gap

| Aspekt | Mockup / README-Quickstart | Tatsächliche API (verifiziert) |
|---|---|---|
| Konstruktor | `SwarmQ(broker_url="…", backend_url="…")` | `SwarmQ(broker: RabbitMQBroker, backend: ValkeyBackend, queues=[…])` |
| Task-Identifikator | `app.schedule(Greet, who="world")` — Task-**Klasse** | `app.schedule("greet", who="world")` — Task-**Name als String** |
| Rückgabe von `schedule` | `handle` mit `await handle.wait(timeout=10)` | `task_id: str`, Ergebnis via `await app.get_result(task_id, timeout=10)` oder einzeilig via `await app.run("greet", who="world", timeout=10)` |

**Verifikationsquellen:**

- `src/swarmq/app.py:20-162` — `SwarmQ.__init__`, `schedule`,
  `get_result`, `run`.
- `src/swarmq/client.py:245-255` — `Client.schedule(task_name: str, …)
  -> str`.
- `src/swarmq/broker/rabbitmq.py` + `src/swarmq/backend/valkey.py` —
  Konstruktoren nehmen jeweils eine URL.
- `tests/e2e/test_unique_tasks.py:49-52` — reales Verwendungsmuster:
  `broker = RabbitMQBroker(url); backend = ValkeyBackend(url);
  SwarmQ(broker, backend, queues=[…])`.

## Why It Matters

Die User-Doku ist das erste, was neue Nutzer lesen. Der Design-Prototyp
zeigt, wie eine solche Library sich **anfühlen sollte** — kurz,
string-frei, ein einziges `SwarmQ(...)`-Objekt ohne
Broker/Backend-Instanziierung. Die heutige API ist explizit
(Dependency-Injection-Style: Broker und Backend sind austauschbar),
aber dafür redundanter im Quickstart.

Konkrete Reibungspunkte im Getting-Started-Beispiel:

1. Drei Imports statt einem (`SwarmQ`, `RabbitMQBroker`,
   `ValkeyBackend`).
2. Zwei Zeilen Setup statt einer (`broker = …; backend = …`).
3. Task-Namen als String statt als Klasse — Autocomplete und
   Refactoring-Tools helfen nicht.
4. Die Klasse `Greet` muss trotzdem importiert werden
   (`import myapp.tasks  # noqa: F401`), damit der Registry-Eintrag
   existiert — der explizite Import sieht seltsam neben dem
   String-Aufruf aus.

Jedes dieser Themen ist einzeln klein. Zusammen ergeben sie den
Eindruck "SwarmQ ist mächtig, aber der erste Kontakt ist umständlicher
als er sein müsste".

## Options

### 1. Nichts ändern (Status quo)

Die Doku passt zur Library. Neue Nutzer lernen die explizite Form und
verstehen früher, dass Broker und Backend austauschbare Komponenten
sind. Kein Code-Aufwand, keine Migrations­last.

**Kosten:** Quickstart bleibt länger als im Mockup gezeigt. Der
Design-Prototyp bleibt als "Wunschbild" sichtbar, erzeugt aber
anhaltenden Erwartungs-Mismatch bei Lesern, die das Mockup
parallel sehen (z.B. in Präsentationen, Blog-Posts).

### 2. Convenience-Factory hinzufügen (additiv, non-breaking)

Neue Class-Methods als Abkürzung, ohne die explizite Form zu entfernen:

```python
app = await SwarmQ.from_urls(
    broker_url="amqp://…",
    backend_url="valkey://…",
)
```

Intern baut die Factory die beiden Instanzen und gibt das `SwarmQ`-
Objekt zurück. Die explizite Form bleibt für fortgeschrittene
Anwender (Custom-Broker-Instanzen, Test-Doubles) erhalten.

**Kosten:** eine Methode, ein Test, eine Doku-Zeile. Nutzen: der
Getting-Started-Codeblock wird um 2–3 Zeilen kürzer.

### 3. Convenience-Factory + `ResultHandle`-Wrapper (additiv, non-breaking)

Option 2, plus: `schedule()` gibt weiterhin einen String zurück, aber
ein zusätzliches `app.submit(...)` (oder `app.enqueue(...)`) gibt ein
leichtgewichtiges Handle-Objekt zurück, auf dem `await handle.wait()`
möglich ist. Intern hält das Handle nur die `task_id` plus eine
Referenz auf den Client.

```python
handle = await app.submit("greet", who="world")
result = await handle.wait(timeout=10)
```

**Kosten:** eine neue Klasse, ein Test, eine Doku-Seite. Nutzen:
Mockup-konformer Flow ist möglich, ohne die String-API zu brechen.

### 4. Breaking Change: Task-Klasse als Schedule-Argument

`app.schedule(Greet, …)` statt `app.schedule("greet", …)`. Würde
Autocomplete und Refactoring-Tools einbinden.

**Kosten:** Breaking Change für alle existierenden Aufrufer. Funktioniert
nur für in-process bekannte Klassen — für Cross-Service-Enqueuing (Client
kennt die Task-Klasse nicht, nur ihren Namen) braucht es weiterhin den
String-Pfad. Praktischer Effekt: Zwei Wege, mehr API-Oberfläche, mehr
Doku.

## Recommendation (tentative)

**Option 2 zuerst, Option 3 beobachten.** Begründung:

- Option 2 ist billig und liefert den größten Einzeleffekt auf den
  Getting-Started-Codeblock (weniger Imports, weniger Setup-Zeilen).
- Option 3 fühlt sich schön an, löst aber nur eine Nuance (wait vs.
  get_result). Erst messen, wie oft Nutzer tatsächlich
  `schedule + get_result` vs. `run` wählen. Wenn `run` die
  dominierende Form ist, reicht Option 2.
- Option 4 ist teuer und löst ein Problem (Refactoring-Safety), das
  in einem Task-Queue-Kontext selten auftritt — Task-Namen sind
  typischerweise stabile Config.

Entscheidung bewusst offen — gehört in ein separates Planning-Ticket,
nicht in die Doku-Arbeit.

## Non-Goals

- Diese Phase schreibt keinen Code. Der Doku-Entwurf wurde bereits
  gegen die echte API geliefert.
- Keine Design-Prototyp-Anpassung. Der Prototyp bleibt als Design-
  Zielbild stehen; die Snippet-Abweichung wird akzeptiert, bis ein
  Folge-Plan Option 2/3 realisiert.
- Keine README-Korrektur hier. Wenn Option 2 umgesetzt wird, werden
  README und Doku in derselben PR angeglichen.

## References

- `docs/brainstorms/2026-04-16-user-documentation-requirements.md`
  — die Doku-Arbeit, die diesen Mismatch sichtbar gemacht hat.
- `docs/design-prototype/getting-started.html` — Mockup-Vorlage.
- `docs/recherche-documentation-best-practices.md` §1 TL;DR #5 — "Copy-
  paste-Runnability ist Pflicht".
- `website/getting-started.md` — der gelieferte Entwurf gegen die
  echte API.
- `src/swarmq/app.py`, `src/swarmq/client.py` — Quelle der verifizierten
  API-Signaturen.
