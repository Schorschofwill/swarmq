---
date: 2026-05-31
topic: dishka-di-tasks
---

# Optionale Dishka-Dependency-Injection für Tasks

## Summary

SwarmQ erhält eine optionale Dishka-Integration, mit der Tasks Abhängigkeiten über `FromDishka[T]`-annotierte `run`-Parameter beziehen – **ohne** dass pro Task ein `@inject`-Decorator nötig ist. Die `Task`-Basisklasse wrappt ihre Lebenszyklus-Methoden automatisch; der User baut seinen `AsyncContainer` selbst und registriert ihn global. Pro Task-Ausführung wird eine REQUEST-Scope geöffnet, die `pre_execution`/`run`/`post_execution` gemeinsam umspannt. Kern-SwarmQ bleibt ohne installiertes `dishka` voll funktionsfähig.

---

## Problem Frame

Tasks in SwarmQ sind heute Klassen mit `async def run(self, *args, **kwargs)`; pro empfangener Message erzeugt der Worker eine frische Instanz (`worker.py:1614`) und ruft `run(*args, **kwargs)` (`worker.py:1653`). Abhängigkeiten wie DB-Sessions, HTTP-Clients oder Konfiguration müssen Tasks derzeit selbst beschaffen – über Modul-Globals, manuelles Instanziieren in `run`, oder eigene Lazy-Properties. Das koppelt Geschäftslogik an Aufbau- und Teardown-Code, erschwert Tests (kein sauberes Ersetzen von Abhängigkeiten) und führt zu inkonsistentem Ressourcen-Lifecycle (wer schließt die Connection? wann?).

Andere Task-Frameworks lösen das mit DI-Containern; Dishka ist hier idiomatisch (Celery/Taskiq-Integrationen existieren), erzwingt aber normalerweise einen `@inject`-Decorator an jedem Handler. Bei vielen Tasks ist das Boilerplate, das man leicht vergisst – und das Vergessen schlägt erst zur Laufzeit fehl.

---

## Requirements

**Aktivierung & Packaging**

- R1. `dishka` ist eine optionale Dependency (Extra `swarmq[dishka]`). Kern-SwarmQ installiert und läuft ohne `dishka`.
- R2. Ist `dishka` nicht installiert, verhält sich SwarmQ exakt wie heute; es entsteht kein Import-Fehler beim normalen Betrieb.
- R3a. Ist `dishka` **nicht installiert** und ein Task-Modul referenziert `FromDishka`, schlägt der `from dishka import FromDishka`-Import bereits beim Modul-Import fehl (`ImportError`, vor jeder SwarmQ-Validierung). SwarmQ fängt diesen Fall beim Worker-Start ab und re-raised ihn mit einem klaren Hinweis (z.B. „Task nutzt `FromDishka`, installiere `swarmq[dishka]`"), statt einen rohen Traceback stehenzulassen.
- R3b. Ist `dishka` installiert, aber **kein Container registriert**, scheitert der Worker bei **Worker-Start** (vor der ersten Message): Er enumeriert `Task._registered_tasks`, erkennt `FromDishka`-Nutzung und meldet klar das fehlende `setup_dishka(...)`. Eine Validierung zur Registrierungszeit ist nicht möglich, da der Container beim Klassen-Def evtl. noch nicht existiert.

**DI-Oberfläche für Tasks**

- R4. Tasks beziehen Abhängigkeiten durch `FromDishka[T]`-annotierte Parameter an `run`, `pre_execution` **und** `post_execution` (alle drei werden automatisch gewrappt; die Mechanik ist identisch). Es ist **kein** `@inject`-Decorator pro Task erforderlich.
- R5. Mit `FromDishka` annotierte Parameter werden vom Container befüllt, alle übrigen Parameter bleiben normale, aus der Message deserialisierte `args`/`kwargs`.
- R6. Tasks ohne `FromDishka`-Parameter bleiben unverändert (kein Verhaltens- oder Performance-Unterschied).
- R7. Injizierte Parameter dürfen nicht die reservierten Scheduling-Namen (`delay`, `eta`, `unique_for`, `unique_until`) tragen; der bestehende Reserved-Param-Check (`task.py:453-482`) bleibt wirksam.

**Container-Bereitstellung**

- R8. Der User baut seinen `AsyncContainer` selbst (`make_async_container(...)`) und registriert ihn über eine globale Funktion `swarmq.setup_dishka(container)` – analog zur globalen Task-Registrierung (`Task._registered_tasks`).
- R9. Der Container wird auf Modulebene gebaut, damit er pro Worker-Subprozess durch den Modul-Re-Import frisch entsteht (spawn-sicher). Die Integration verlässt sich nicht darauf, dass ein im Parent-Prozess gebauter Container an Kinder übergeben wird.
- R10. SwarmQ liefert einen Provider namens `SwarmQProvider` aus, der den Task-Kontext per `from_context(TaskContext, scope=Scope.REQUEST)` verfügbar macht. Der User fügt `SwarmQProvider` seinem Container hinzu, wenn er den Kontext injizieren will.

**Scope-Semantik & Lebenszyklus**

- R11. `APP`-Scope entspricht der Lebensdauer des Worker-Prozesses (Singletons, einmal pro Subprozess). Der APP-Container wird beim Worker-Shutdown geschlossen (`await container.close()`), damit APP-Ressourcen finalisiert werden.
- R12. Pro Task-Ausführung öffnet der Worker eine `REQUEST`-Scope in einem `finally`-geschützten Block, der `pre_execution` → `run` → `post_execution` **neu umschließt**; diese drei teilen denselben request-scoped Container (z.B. eine in `pre_execution` bezogene Session ist dieselbe Instanz in `run`/`post_execution`). *Hinweis: Der heutige Code umschließt diese drei nicht gemeinsam (`pre_execution` liegt in eigenem try-Block vor `run`, `worker.py:1645`; `post_execution` läuft nur bei Erfolg, `worker.py:1732`) — R12 ist also neue Struktur, kein erhaltenes Verhalten.*
- R13. Die REQUEST-Scope wird unabhängig vom Ausgang (Erfolg, Exception, Timeout, Cancel) verlassen, sodass `yield`-basierte Finalizer in jedem Fall laufen. Da `post_execution` nur bei Erfolg läuft, gehört Commit-/Rollback-Logik in den `yield`-Finalizer des Providers (der die Exception beim Scope-Exit erhält), **nicht** in `post_execution`.
- R14. Der Task-Kontext ist Providern in der REQUEST-Scope über den Typ `TaskContext` zugänglich – ein frozen Dataclass mit **genau** den Feldern `task_id`, `task_name`, `queue`, `retry_count`, befüllt aus der vorhandenen `MessageContext` (`worker.py:1580`).

---

## Acceptance Examples

- AE1. **Covers R4, R5.** Gegeben ein Task mit `async def run(self, uid: int, svc: FromDishka[UserService])` und ein registrierter Container, wenn der Client `enqueue("task", 42)` aufruft, dann wird `run` mit `uid=42` und einem aus dem Container aufgelösten `svc` ausgeführt – ohne dass der Client `svc` übergibt.
- AE2. **Covers R6.** Gegeben ein Task ohne `FromDishka`-Parameter, wenn er ausgeführt wird, dann ist sein Verhalten identisch zum heutigen Stand (keine Scope wird zwingend benötigt, keine zusätzliche Auflösung).
- AE3. **Covers R3b.** Gegeben ein Task mit `FromDishka[T]` und **kein** registrierter Container, wenn der Worker startet, dann scheitert er mit einer klaren Meldung, statt die Task bis zur Ausführung anzunehmen.
- AE4. **Covers R12, R13.** Gegeben ein request-scoped Provider mit `yield` (z.B. DB-Session), wenn `run` eine Exception wirft, dann wird `post_execution` übersprungen, der `yield`-Finalizer aber dennoch aufgerufen (Scope-Exit erhält die Exception und kann z.B. zurückrollen).
- AE5. **Covers R14.** Gegeben `SwarmQProvider` ist im Container, wenn ein request-scoped Logger über `TaskContext` definiert ist, dann enthält der in `run` injizierte Logger die `task_id` der laufenden Ausführung.
- AE6. **Covers R1, R2.** Gegeben `dishka` ist nicht installiert und kein Task-Modul referenziert `FromDishka`, wenn ein Worker startet und Tasks ausführt, dann verhält sich SwarmQ identisch zum heutigen Stand und es entsteht kein Import-Fehler.
- AE7. **Covers R3a.** Gegeben `dishka` ist nicht installiert, ein Task-Modul nutzt aber `FromDishka`, wenn der Worker startet, dann meldet SwarmQ klar, dass das `swarmq[dishka]`-Extra fehlt, statt einen rohen `ImportError`-Traceback stehenzulassen.
- AE8. **Covers R8.** Gegeben der User ruft auf Modulebene `swarmq.setup_dishka(container)` auf, wenn ein Worker-Subprozess das Modul re-importiert, dann liest der Worker den global registrierten Container aus und nutzt ihn für die Injektion – obwohl der Worker eine eigene `SwarmQ`-Instanz baut.
- AE9. **Covers R7.** Gegeben ein Task definiert `async def run(self, delay: FromDishka[X])`, wenn die Klasse definiert wird, dann scheitert der Reserved-Param-Check zur Klassen-Def-Zeit (volle Signatur inkl. `FromDishka`-Parameter wird geprüft).
- AE10. **Covers R4 (Nebenläufigkeit).** Gegeben N>1 Tasks mit je eigener request-scoped Ressource laufen nebenläufig in einem Worker, wenn sie gleichzeitig ausgeführt werden, dann erhält jeder Task eine distinkte Instanz (keine Cross-Task-Leakage des Request-Containers).
- AE11. **Covers R9, R11.** Gegeben ein auf Modulebene gebauter Container, wenn mehrere Worker-Subprozesse gespawnt werden, dann besitzt jeder seinen eigenen frischen APP-Container; beim Worker-Shutdown wird `await container.close()` aufgerufen und APP-Ressourcen werden finalisiert.

---

## Success Criteria

- Ein Entwickler kann eine Abhängigkeit in einen Task injizieren, indem er einen Parameter mit `FromDishka[T]` annotiert und den Container einmal global registriert – ohne `@inject` und ohne Änderung am Enqueue-Aufruf.
- Bestehende Tasks und Deployments ohne Dishka laufen unverändert weiter; `dishka` muss nicht installiert sein.
- Request-scoped Ressourcen (Sessions/Connections) werden pro Task sauber auf- und abgebaut, auch bei Fehlern/Timeouts/Cancel.
- ce-plan kann ohne weitere Produktentscheidungen direkt in die Implementierungsplanung gehen; die verifizierte Mechanik (siehe Key Decisions) ist die Grundlage.

---

## Scope Boundaries

- Keine Custom-Scopes über `APP`/`REQUEST` hinaus (z.B. per-Retry, `SESSION`, `STEP`) – vorerst ausgeschlossen, später erweiterbar.
- Keine DI in Middleware-Hooks oder Signal-Handlern – nur die Task-Lebenszyklus-Methoden (`pre_execution`/`run`/`post_execution`).
- Keine Unterstützung für synchrone Container/Tasks – SwarmQ ist async-only.
- Kein Umbau des Worker-Bootstrappings auf ein User-`app`-Objekt (`--app myapp:app`); der Worker baut weiterhin seine eigene `SwarmQ`-Instanz, der Container kommt über die globale Registrierung.
- Keine client-seitige Signatur-Validierung – der Client inspiziert die `run`-Signatur nicht und braucht es auch nicht (siehe Key Decisions).

---

## Key Decisions

- **Auto-Wrapping in der `Task`-Basisklasse statt `@inject` pro Task**: Dishkas `wrap_injection` wird auf die Lebenszyklus-Methoden angewandt; hat eine Methode keine `FromDishka`-Parameter, gibt `wrap_injection` sie unverändert zurück (verifiziert in `dishka/integrations/base.py`). Übertragung des Celery-`DishkaTask`-Musters auf SwarmQs Klassen-API.
- **Wrappen einmal pro Klasse in `__init_subclass__`, nicht pro Instanz**: `wrap_injection` kompiliert Code (teuer), und der Worker erzeugt pro Message eine frische Instanz (`worker.py:1614`). Daher wird zur Klassen-Definitionszeit gewrappt (nach dem Reserved-Check), nicht bei Instanziierung. Die Return-Type-Erkennung der Registry bleibt intakt (Wrapper behält `return` in `__annotations__`).
- **Manuelles Scope-Management mit `manage_scope=False`**: Dishkas `manage_scope=True` öffnet pro Aufruf eine eigene Child-Scope (verifiziert in `dishka/code_tools/inject_compiler.py:120-142`) – das würde `pre`/`run`/`post` je eine separate REQUEST-Scope geben. Für die gewünschte gemeinsame Scope öffnet der Worker **eine** Scope um den ganzen Lebenszyklus und wrappt die Methoden mit `manage_scope=False`.
- **Container-Übergabe per `ContextVar` (Nebenläufigkeit kritisch)**: Der `container_getter` sieht nur `(args, kwargs)`. Der pro-Task-Request-Container wird über eine modul-level `ContextVar` übergeben. **`ContextVar.set()` muss innerhalb von `_handle_message` erfolgen** (nicht früher/modulglobal): Der Worker fährt viele Tasks nebenläufig in einem Loop (`asyncio.create_task` pro Message `worker.py:1072`, Semaphore `:129`); würde der Wert zu früh gesetzt, läsen Geschwister-Tasks denselben Container und die Request-Scope leakt (Task A bekäme Task Bs Session). `asyncio.create_task` (für `run`, `worker.py:1673`) kopiert den dann gesetzten Kontext bei Erstellung. Präzedenz: das bestehende `structlog.contextvars`-Per-Task-Muster (`worker.py:1439`). Die Aufruf-Site `task_instance.run(*args, **kwargs)` bleibt unverändert.
- **Whole-Lifecycle-Scope ist bewusst gewählt (R12)**: Ziel ist, dass eine in `pre_execution` bezogene Session/Transaktion dieselbe Instanz in `run`/`post_execution` ist. Die einfachere Alternative – Per-Methoden-Scopes (je eine REQUEST-Scope pro Methode via `manage_scope=True`) – wurde deshalb verworfen, obwohl sie R4/R5/R6 ebenfalls erfüllen würde. Konsequenz: `pre_execution`/`post_execution` werden ebenfalls gewrappt (R4), und der Worker managed die Scope manuell (`manage_scope=False`).
- **Globaler Container-Slot analog `Task._registered_tasks`**: `swarmq.setup_dishka(container)` legt den Container in einen Modul-Level-Slot; der Worker liest ihn nach `importlib.import_module` (`worker_cmd.py:27`). Der Worker nutzt das `app`-Objekt des Users nicht.
- **Task-Kontext via `from_context`**: `from_context(TaskContext, scope=Scope.REQUEST)` plus Scope-Eintritt mit `container(context={TaskContext: ctx})` (verifiziertes Taskiq-Muster). Quelle der Daten ist die vorhandene `MessageContext` (`worker.py:1580-1590`).
- **`APP → REQUEST` ohne explizites Scope-Argument**: `SESSION` ist `skip=True` (`dishka/entities/scope.py:45`), und `container()` betritt automatisch die nächste nicht-übersprungene Scope (`async_container.py:119-123`) = REQUEST.
- **Kein client-seitiges Problem mit injizierten Parametern**: Der Client serialisiert `args`/`kwargs` direkt ohne Signatur-Inspektion (`client.py:364-372`). Injizierte Parameter werden vom User schlicht nicht übergeben und nie serialisiert.

---

## Dependencies / Assumptions

- Annahme (verifiziert): `make_async_container()` baut + validiert den Graphen synchron und führt keine Provider-Factories aus → Modulebene-Konstruktion ist auch in einem Client-Prozess ohne laufenden Event-Loop sicher.
- Annahme (verifiziert): Der Worker-Subprozess re-importiert das User-Modul innerhalb seines eigenen, via `asyncio.run` gestarteten Event-Loops (`worker_cmd.py:27`); Tasks registrieren sich über `__init_subclass__`.
- Annahme (verifiziert): `pre_execution`/`post_execution` sind `async def` No-ops ohne Argumente (`task.py:333-340`); `post_execution` läuft nur nach erfolgreichem `run` (`worker.py:1732-1742`), die REQUEST-Scope-Finalisierung hingegen muss im `finally` liegen, damit sie immer greift.
- Dependency: Dishka-Version mit `wrap_injection(manage_scope=..., scope=..., provide_context=...)`-Signatur und `from_context` (aktueller `master`-Stand; konkrete Mindestversion in der Planung festnageln).

---

## Outstanding Questions

### Deferred to Planning

- [Affects R11][Technical] Exakter Einhängepunkt für `await container.close()` im Worker-Stop-Pfad (`worker.py:734-958`) inkl. Verhalten bei hartem Abbruch.
- [Affects R12, R13][Technical] Genaue Platzierung des `async with container(...)`-Blocks relativ zum bestehenden try/except/finally um `worker.py:1645-1742`, sodass Timeout-/Cancel-Pfade die Scope korrekt schließen. **Achtung:** `yield`-Finalizer, die während Cancel selbst `await`en, sind eine bekannte asyncio-Schärfe – R13s „Finalizer laufen auch bei Cancel" ist nicht automatisch gratis und muss verifiziert werden.
- [Affects R6][Technical] Das `__init_subclass__`-Wrapping muss ein echtes No-op sein, wenn `dishka` nicht installiert ist (kein Import, keine Signatur-Inspektion über den bestehenden Reserved-Check hinaus), damit dishka-freie Deployments keine Import-Zeit-Kosten tragen.
- [Affects R6][Technical] `wrap_injection` ruft `get_type_hints(run)` eager auf → run-Annotationen werden zur Klassen-Def-Zeit voll evaluiert. Forward-Refs / `TYPE_CHECKING`-only-Imports in einer `run`-Signatur könnten beim Import `NameError` werfen (existiert heute nicht, da nur `inspect.signature` läuft). Defensive Behandlung beim Wrapping festlegen.
- [Affects R1][Needs research] Mindest-Dishka-Version, die alle genutzten Parameter (`manage_scope`, `scope`, `from_context`) stabil unterstützt. **Risiko:** Bindung an einen `master`-Stand koppelt die veröffentlichte Lib-Kompatibilität an eine unveröffentlichte Upstream-API – vor Implementierungsbeginn auf ein getaggtes Release festnageln.

### Strategische Hinweise (aus Review, nicht blockierend)

- Die Kopplung der öffentlichen API an Dishka (statt eines framework-agnostischen Resolver-Hooks mit Dishka als erstem Adapter) ist eine Positionierungsentscheidung mit Adoptionswirkung für eine öffentliche Lib. Bewusst akzeptiert; bei Bedarf vor v1 erneut abwägen.
- Das globale Container-Modell (`setup_dishka`) ist für den Worker-Subprozess-Lebenszyklus passend, aber für Multi-App-/Test-Nutzung in einem Prozess unbequem (globaler Singleton, Teardown/Reset undokumentiert). Falls In-Process-Tests relevant werden, Reset-Mechanik nachziehen.
