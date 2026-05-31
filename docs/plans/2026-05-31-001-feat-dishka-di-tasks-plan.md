---
title: "feat: Optionale Dishka-Dependency-Injection für Tasks"
type: feat
status: active
date: 2026-05-31
origin: docs/brainstorms/2026-05-31-dishka-di-tasks-requirements.md
deepened: 2026-05-31
---

# feat: Optionale Dishka-Dependency-Injection für Tasks

## Summary

Eine optionale Dishka-Integration verdrahtet Dependency Injection in den Task-Lebenszyklus: `FromDishka[T]`-annotierte Parameter an `run`/`pre_execution`/`post_execution` werden aus einem user-gebauten, global registrierten `AsyncContainer` befüllt — **ohne** Per-Task-`@inject`. Die `Task`-Basisklasse wrappt ihre Lebenszyklus-Methoden einmal pro Klasse; der Worker öffnet pro Ausführung eine REQUEST-Scope, die `pre`/`run`/`post` gemeinsam umspannt und über eine `ContextVar` pro-Task-isoliert ist. Kern-SwarmQ bleibt ohne installiertes `dishka` unverändert lauffähig.

---

## Problem Frame

Tasks beschaffen Abhängigkeiten heute selbst (Modul-Globals, manuelles Instanziieren, Lazy-Properties), was Geschäftslogik an Aufbau/Teardown koppelt und den Ressourcen-Lifecycle inkonsistent macht. Die Detail-Motivation steht im Origin-Dokument (siehe Sources & References). Plan-spezifisch: Der Worker führt Tasks als **detached `asyncio.create_task`-Handler** nebenläufig in **einem** Event-Loop aus (Semaphore-begrenzt) — jede DI-Verdrahtung muss deshalb pro-Task-isoliert sein und in jedem Ausgangspfad (Erfolg/Exception/Timeout/Cancel) sauber finalisieren.

---

## Requirements

- R1. `dishka` als optionales Extra (`swarmq[dishka]`); Kern läuft ohne.
- R2. Ohne `dishka` verhält sich SwarmQ wie heute; kein Import-Fehler im Normalbetrieb.
- R3a. `dishka` fehlt + Modul nutzt `FromDishka` → `ImportError` beim Modul-Import wird beim Worker-Start abgefangen und mit klarem Hinweis (`pip install 'swarmq[dishka]'`) re-raised.
- R3b. `dishka` installiert, kein Container → klare Fehlermeldung beim Worker-Start (vor erster Message), inkl. Auflösbarkeit der injizierten Typen (siehe Key Technical Decisions).
- R4. `FromDishka`-Injektion an `run`, `pre_execution`, `post_execution`; kein `@inject` pro Task.
- R5. Nur `FromDishka`-Parameter werden injiziert; übrige bleiben message-deserialisierte `args`/`kwargs`.
- R6. Tasks ohne `FromDishka` bleiben verhaltens- und (laufzeit-)performancegleich.
- R7. Injizierte Parameter dürfen keine reservierten Namen tragen; Reserved-Check bleibt wirksam.
- R8. User baut Container selbst, registriert via `swarmq.setup_dishka(container)` (globaler Slot).
- R9. Container auf Modulebene gebaut → frisch pro Worker-Subprozess via Re-Import (spawn-sicher).
- R10. SwarmQ liefert `SwarmQProvider` mit `from_context(TaskContext, scope=Scope.REQUEST)`.
- R11. `APP`-Scope = Worker-Prozess; `await container.close()` beim Shutdown.
- R12. `REQUEST`-Scope umspannt `pre`/`run`/`post` (geteilter Container) in neuem `finally`-geschütztem Block.
- R13. REQUEST-Scope verlässt in jedem Ausgang; `yield`-Finalizer laufen immer und **erhalten die tatsächliche Exception** (siehe Key Technical Decisions: explizites Exception-Plumbing, da der Worker Exceptions intern abfängt statt sie zu re-raisen); Commit/Rollback gehört in den Finalizer, nicht in `post_execution`. Auf dem Shutdown-Cancel-Pfad ist der Scope-Exit zu shielden.
- R14. Task-Kontext über `TaskContext` (frozen Dataclass: `task_id`, `task_name`, `queue`, `retry_count`) injizierbar.

**Origin acceptance examples:** AE1 (R4,R5), AE2 (R6), AE3 (R3b), AE4 (R12,R13), AE5 (R14), AE6 (R1,R2), AE7 (R3a), AE8 (R8), AE9 (R7), AE10 (R4-Nebenläufigkeit), AE11 (R9,R11).

---

## Scope Boundaries

- Keine Custom-Scopes über `APP`/`REQUEST` hinaus (per-Retry, `SESSION`, `STEP`).
- Keine DI in Middleware-Hooks oder Signal-Handlern — nur die Task-Lebenszyklus-Methoden.
- Keine synchronen Container/Tasks — async-only.
- Kein Umbau des Worker-Bootstrappings auf ein User-`app`-Objekt.
- Keine client-seitige Signatur-Validierung (Client inspiziert `run` nicht).

### Deferred to Follow-Up Work

- `/ce-compound`-Eintrag zu `multiprocessing spawn` + Per-Worker-Modul-Re-Import (kein bestehendes Learning; Lücke laut Recherche).

---

## Context & Research

### Relevant Code and Patterns

- **Test-Harness:** `tests/unit/test_worker.py` — `_make_message`/`_default_payload` + direkter `await worker._handle_message(msg)` mit `broker`/`backend`-Fakes (`MagicMock(spec=...)`, async-Methoden als `AsyncMock`). Vorlage für AE1/AE2/AE4/AE5/AE10.
- **Lifecycle-Tests:** `test_pre_and_post_execution_called` (`test_worker.py:179`), `test_pre_execution_raises_skips_post` (`:306`), `tests/unit/test_worker_timeouts.py` (`test_soft_timeout_skips_post_execution:244`, Watchdog-Cancel `:427/:477/:525`), `test_semaphore_limits_concurrency` (`:211`).
- **ContextVar-Präzedenz:** structlog-Bind/Clear in `_handle_message` (`worker.py:1588-1594`), verifiziert via `test_handle_message_binds_contextvars` (`test_worker.py:634`) — set-während-Ausführung, cleared-danach.
- **Optionale Extras:** `metrics.py:48-58` (`_HAVE_PROMETHEUS` Flag, No-op-Fallback); `hot_reload.py:51-66` (`ConfigurationError` mit `pip install 'swarmq[reload]'`-Hinweis) — Präzedenz für R3a/R3b. Test-Trick: `patch.dict(sys.modules, {"dishka": None})` (`test_hot_reload.py:158`).
- **Registry-Isolation:** Autouse `_reset_task_registry` (`tests/conftest.py:18-24`, snapshot/restore) — Vorlage für den nötigen Container-Slot-Reset.
- **Wrap-Einhängepunkt:** `Task.__init_subclass__` (`task.py:290-294`) nach `_check_run_signature_reserved_params` (`task.py:453-482`); `run`-Aufruf `worker.py:1653`, `create_task(run_coro)` `:1673`, `post_execution` (success-only) `:1732`, Shutdown-Close-Punkt `worker.py:1053-1054`/`958-961`.
- **CLI-Bootstrap:** `run_worker_async` (`cli/worker_cmd.py:19-73`), `importlib.import_module` `:27`, Registry-Empty-Check `:28-33`. Unit-Tests: `tests/unit/test_cli.py::TestRunWorkerAsync:292-347`. Live-Multi-Subprozess: `tests/integration/test_cli_worker.py:115`.
- **Dishka-Source (verifiziert):** `wrap_injection` (`integrations/base.py` — gibt Funktion unverändert zurück ohne `FromDishka`-Deps), `inject_compiler.py:120-142` (manage_scope öffnet neue Child-Scope → wir nutzen `manage_scope=False`), `async_container.py:93-145/303-340` (`container(context=...)`, `__aexit__` reicht Exception an Finalizer), `integrations/taskiq.py:36/51` (`from_context` + `container(context={...})`-Muster).

### Institutional Learnings

- `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`: Tasks laufen detached; REQUEST-Scope **innerhalb** `_handle_message` öffnen (contextvars werden dort per Task kopiert); Cleanup mit `except BaseException`, damit Cancel/Timeout Finalizer auslösen; `connect()`-Idempotenz.
- `project_zero/docs/solutions/runtime-errors/event-system-p1-di-valkey-secrets-migration.md`: Realer Dishka/taskiq-Integration. `FromDishka[T]` ohne Provider → `NoFactoryError` **erst bei erster Ausführung**, nicht beim Start → Startup-Validierung der Auflösbarkeit nötig (R3b).
- `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`: `FIRE_AND_FORGET` ackt früh; Scope-Setup-Fehler nach Ack darf nicht still verschluckt werden → Platzierung relativ zum Ack beachten, Fehler im detached Handler loggen/routen.
- `project_zero/.../basemodule-missing-scopes-property.md`: `__init_subclass__`/Registry muss Opt-out-Subklassen tolerieren (Base-Defaults, No-op ohne DI).
- `project_zero/.../cascading-test-breaks-from-bulk-refactor.md` + R3.5-Patterns: `_MISSING`-Sentinel-Singleton zur Unterscheidung „DI nicht konfiguriert" vs. „explizit None"; Modul-State in Fixtures resetten.

### External References

- Dishka-Doku/Source (master): Celery `DishkaTask`- und Taskiq-Integration als nächstgelegene Vorlagen; siehe verifizierte Source-Pfade oben.

---

## Key Technical Decisions

- **Auto-Wrapping in `__init_subclass__`, einmal pro Klasse**: `wrap_injection` kompiliert Code (teuer); der Worker erzeugt pro Message eine frische Instanz, daher wird zur Klassen-Def-Zeit gewrappt (nach dem Reserved-Check). Ohne `FromDishka`-Parameter ist es ein No-op (Funktion unverändert). Wrapping ist **vollständiges No-op, wenn `dishka` nicht installiert ist** (guarded import, kein zusätzlicher Inspektions-Overhead über den bestehenden Reserved-Check hinaus) → erfüllt R2/R6.
- **`manage_scope=False` + manuelle REQUEST-Scope**: Dishkas `manage_scope=True` öffnet pro Aufruf eine eigene Child-Scope — das gäbe pre/run/post je eine separate Scope. Der Worker öffnet stattdessen **eine** Scope um den ganzen Lebenszyklus (R12).
- **Container-Übergabe per `ContextVar`, gesetzt vor `create_task(run_coro)`**: `container_getter` sieht nur `(args, kwargs)`; der pro-Task-Request-Container kommt über eine modul-level `ContextVar`, **innerhalb** `_handle_message` gesetzt (wie structlog-contextvars), isoliert zwischen nebenläufigen Geschwister-Tasks (`_handle_message` läuft selbst als `create_task`, `worker.py:1072`). **Sequenzkritisch:** `run` wird in einem eigenen `asyncio.create_task(run_coro)` (`worker.py:1659/1673`) ausgeführt, das den Kontext bei Erstellung kopiert — der `ContextVar.set()` muss daher **vor** dieser `create_task`-Zeile liegen (nicht nur vor `pre_execution`); das `soft_timeout`-`wait_for`-Wrapping (`worker.py:1654`) liegt innerhalb der geschedulten Coroutine und erbt den Kontext. `reset` nach Watchdog/Monitor-Cleanup (R12, AE10).
- **Scope-Platzierung relativ zum `FIRE_AND_FORGET`-Ack** *(verschärft durch Review)*: `FIRE_AND_FORGET` ackt **vor** der Ausführung (`worker.py:1576-1578`). Die REQUEST-Scope wird daher **nach** diesem Ack geöffnet (nicht davor) — sonst würde ein Scope-Setup-Fehler versuchen, eine bereits geackte Message zu nacken. Bei nicht-FF-Tasks wird ein Setup-Fehler wie ein pre_execution-Fehler geroutet; bei FF-Tasks wird er **geloggt und als Fehler gezählt** (kein Nack auf geackte Message), nie still verschluckt.
- **Exception-Plumbing zum Finalizer** *(P1-Fix aus Review)*: Der Worker fängt run/pre/post-Exceptions intern ab und re-raised sie **nicht** (`worker.py:1646-1651/1688-1742`). Ein schlichtes `async with container(...)` gäbe dem Finalizer deshalb `exception=None`, was R13s Commit/Rollback bricht. Der Scope-Exit muss die abgefangene Exception **explizit** weiterreichen: `await req.__aexit__(type(exc), exc, tb)` mit der erfassten `task_exception` (bzw. `container.close(exception=exc)`), statt nur den Context-Manager normal zu verlassen. Die bestehende ack/nack/Retry-Disposition aus `is_error`/`task_exception` bleibt davon unberührt.
- **Shutdown-Cancel: Scope-Exit shielden** *(P1-Fix aus Review)*: Soft-/Hard-Timeout canceln nur `run_task` (Kind) — `_handle_message` läuft weiter, Finalizer awaiten gefahrlos. **Beim Worker-Shutdown** wird der ganze `_handle_message`-Task gecancelt (`worker.py:998`); ein `await` in `__aexit__` (`async_container.py:320`) würde die `CancelledError` re-raisen und den Finalizer abbrechen. `except BaseException` verhindert das nicht. Der Scope-Exit ist daher auf dem Shutdown-Pfad mit `asyncio.shield` zu umgeben (Präzedenz: `_detached_releases` für Lock-Release, `worker.py:1001-1011`). Verifikation gegen den **Shutdown-Drain-Pfad** (`worker.py:987-999`), nicht nur die Watchdog-Timeout-Tests.
- **Startup-Validierung inkl. Auflösbarkeit** *(verschärft durch Review)*: R3b prüft nicht nur „Container registriert", sondern dass jeder `FromDishka`-Typ der registrierten Tasks im Container-Graph vorhanden ist. **Mechanismus:** U2 leitet die `DependencyKey`s **selbst** ab (`default_parse_dependency` + `get_type_hints(method, include_extras=True)`) und legt sie als Klassenattribut (`cls._dishka_injected_keys`) ab — `wrap_injection` exponiert sie **nicht**. U5 liest dieses Attribut und prüft gegen den Container-Graph. **Achtung Kopplung:** Die Graph-Introspektion (`container.registry`/`child_registry`/`get_factory`) ist private Dishka-API → Version-Pin ist U5-Blocker (siehe Open Questions/Risks); fällt die Introspektion auf der gepinnten Version aus, degradiert R3b auf reine Container-Präsenz.
- **Commit/Rollback im Finalizer, nicht in `post_execution`**: `post_execution` läuft nur bei Erfolg; transaktionale Logik gehört in den `yield`-Finalizer des Providers, der beim Scope-Exit die explizit weitergereichte Exception erhält (R13, siehe Exception-Plumbing oben).
- **`TaskContext` als frozen Dataclass + `SwarmQProvider`**: stabiler öffentlicher Typ (4 Felder), befüllt aus `MessageContext` (`worker.py:1580`); per `container(context={TaskContext: ctx})` in die Scope gereicht, via `from_context` aufgelöst.
- **`_MISSING`-Sentinel für den Container-Slot**: unterscheidet „nie `setup_dishka` aufgerufen" von „explizit `None`".

---

## Open Questions

### Resolved During Planning

- pre/post-Injektion: **ja**, alle drei Lebenszyklus-Methoden werden gewrappt (Origin-Entscheidung, Mechanik identisch).
- R3-Fehlerort: dishka-fehlt → Worker-Start-Catch des Import-`ImportError`; kein-Container → Worker-Start-Validierung (Registrierungszeit unmöglich).
- Validierungstiefe: Auflösbarkeit aller `FromDishka`-Typen (nicht nur Container-Präsenz).

### Resolve Before Implementation (U5-Blocker)

- **Dishka-Version-Pin**: konkretes getaggtes Release festnageln, dessen `wrap_injection`/`from_context`-Signaturen **und** Registry-Introspektions-Internals (`registry`/`child_registry`/`get_factory`) die U5-Auflösbarkeitsprüfung tragen. Bis dahin ist die stärkere R3b-Variante nicht implementierbar (Fallback: Container-Präsenz). Vorgelagerter Spike, bevor U5 startet.

### Deferred to Implementation

- **Auflösbarkeits-Mechanismus im Detail**: bevorzugt Graph-Introspektion ohne Factory-Aufruf; REQUEST-scoped Keys liegen in der Child-Registry. Exakte Traversierung gegen die gepinnte Version festnageln; Trial-`get` in Dummy-REQUEST-Scope nur als letzte Option (Factory-Seiteneffekte!).
- **Shield-vs-Drain-Timeout-Interaktion**: Der geshieldete Scope-Exit auf dem Shutdown-Pfad darf den 1s-Drain-Grace (`asyncio.wait(pending, timeout=1)`, `worker.py:999`) nicht unbegrenzt überschreiten — Verhalten beim Implementieren festlegen.
- **Scope-Boundary vs. Middleware/Retry/Result**: Middleware-`on_error`/`post_execute`, Retry-Entscheidung und Result-Storage laufen **nach** dem Scope-Exit. Falls ein Finalizer diese beeinflussen müsste, trägt die gewählte pre/run/post-Grenze das nicht — beim Implementieren bestätigen, dass das nicht gebraucht wird.
- **Genaue Methodensignaturen/Helper-Namen** im neuen Modul.

---

## High-Level Technical Design

> *Illustriert den intendierten Ansatz, ist Orientierung für das Review — keine Implementierungsspezifikation. Der umsetzende Agent behandelt dies als Kontext, nicht als zu reproduzierenden Code.*

Verdrahtung in `_handle_message` (DI-aktiver Pfad; ohne Container bleibt alles wie heute):

Das Pseudo-Modell zeigt die Reihenfolge; die genaue Verschränkung mit dem bestehenden try/except/finally und der `create_task(run_coro)`-Zeile (`worker.py:1659`) wird beim Implementieren am realen Code vorgenommen (kein erfundener `_run_lifecycle`-Helper).

```text
# innerhalb _handle_message, nach deserialize + MessageContext + structlog-bind
# und NACH dem FIRE_AND_FORGET-Ack (worker.py:1576-1578)
container = get_registered_container()        # _MISSING -> exakt heutiger Pfad, kein async with
if container is _MISSING:
    <heutiger pre/run/post-Ablauf unverändert>
else:
    ctx = TaskContext(task_id, task_name, queue, retry_count)
    req = await container(context={TaskContext: ctx}).__aenter__()   # REQUEST-Scope offen
    token = _request_container.set(req)        # VOR create_task(run_coro) (worker.py:1659)
    captured_exc = None
    try:
        <pre_execution -> run (in create_task) -> post_execution; Exceptions wie heute
         abgefangen, is_error/task_exception gesetzt; captured_exc = task_exception>
    finally:
        _request_container.reset(token)
        # Exception EXPLIZIT an die Finalizer reichen (Worker re-raised nicht):
        # auf dem Shutdown-Cancel-Pfad geshielded, sonst direkt.
        await shield_on_shutdown(req.__aexit__(type(captured_exc), captured_exc, tb))
    # ack/nack/Retry-Disposition unverändert aus is_error/task_exception
```

Wrapping (Klassen-Def-Zeit, `Task.__init_subclass__`):

```text
_check_run_signature_reserved_params(cls)           # bestehend, sieht FromDishka-Params
if dishka_available():
    for method in (run, pre_execution, post_execution):
        wrapped = wrap_injection(method, is_async=True, manage_scope=False,
                                 container_getter=lambda a,k: _request_container.get())
        record_injected_keys(cls, method)           # für Startup-Auflösbarkeits-Check
        setattr(cls, name, wrapped)                  # No-op wenn keine FromDishka-Params
```

---

## Implementation Units

### U1. Dishka-Modul-Fundament + Packaging + Test-Isolation

**Goal:** Optionales Extra, das neue Modul mit öffentlichem Typ/Provider/Registrierung, der ContextVar-/Container-Slot-Mechanismus und die Test-Isolationsfixture.

**Requirements:** R1, R2, R8, R10, R14

**Dependencies:** None

**Files:**
- Modify: `pyproject.toml` (Extra `dishka`; `dishka` zusätzlich ins `test`-Extra)
- Create: `src/swarmq/dishka.py` (`TaskContext` frozen Dataclass; `SwarmQProvider` mit `from_context(TaskContext, scope=Scope.REQUEST)`; `setup_dishka(container)`; `get_registered_container()`/`_reset_container()`; modul-level `_container = _MISSING`; `_MISSING`-Sentinel; `_request_container: ContextVar`; guarded `dishka`-Import + `_HAVE_DISHKA`-Flag)
- Modify: `src/swarmq/__init__.py` (Export `setup_dishka`, `SwarmQProvider`, `TaskContext` — eager wenn dishka-frei importierbar, sonst lazy via `_LAZY_SURFACE`)
- Modify: `tests/conftest.py` (autouse `_reset_dishka_container`, snapshot/restore analog `_reset_task_registry`)
- Test: `tests/unit/test_dishka.py`

**Approach:**
- Guarded Import: `try: from dishka import ... ; _HAVE_DISHKA=True except ImportError: _HAVE_DISHKA=False`. Bei fehlendem dishka bleiben `SwarmQProvider`/`from_context` unreferenziert; `TaskContext`/`setup_dishka`/Slot funktionieren ohne dishka (reine Python-Objekte).
- `setup_dishka` setzt den Modul-Slot; `get_registered_container()` gibt `_MISSING` zurück, wenn nie gesetzt.
- ContextVar wird hier definiert, in U3 gesetzt.

**Patterns to follow:** `metrics.py:48-58` (Flag+Fallback); `tests/conftest.py:18-24` (Reset-Fixture); `__init__.py:57-92` (`_LAZY_SURFACE`).

**Test scenarios:**
- Happy path: `setup_dishka(c)` dann `get_registered_container() is c`.
- Edge case: ohne `setup_dishka` → `get_registered_container() is _MISSING` (nicht `None`).
- Edge case: `TaskContext` ist frozen, hat genau `task_id`/`task_name`/`queue`/`retry_count` (Covers AE5-Vorbereitung).
- Happy path: `SwarmQProvider` ist exportiert/importierbar und deklariert `from_context(TaskContext, scope=Scope.REQUEST)` (Covers R10 — explizite Export-Verifikation).
- Integration: `_reset_dishka_container`-Fixture isoliert — ein in Test A registrierter Container ist in Test B nicht sichtbar.
- Error path: `from swarmq.dishka import _HAVE_DISHKA` ist `False` unter `patch.dict(sys.modules, {"dishka": None})`; Modul-Import wirft trotzdem nicht; Kern-Symbole (`TaskContext`, `setup_dishka`) bleiben nutzbar (Covers AE6, R2).

**Verification:** Modul importiert mit und ohne installiertes dishka fehlerfrei; Slot-Roundtrip und Fixture-Isolation grün.

---

### U2. Auto-Wrapping der Lebenszyklus-Methoden in der Task-Basisklasse

**Goal:** `run`/`pre_execution`/`post_execution` werden zur Klassen-Def-Zeit injektionsfähig gemacht, ohne `@inject`, ohne Verhalten für Nicht-DI-Tasks zu ändern.

**Requirements:** R4, R5, R6, R7

**Dependencies:** U1

**Files:**
- Modify: `src/swarmq/task.py` (`__init_subclass__`: nach Reserved-Check die drei Methoden via `wrap_injection(manage_scope=False, is_async=True, container_getter=…ContextVar)` wrappen; injizierte `DependencyKey`s pro Klasse merken für U5; No-op wenn `_HAVE_DISHKA` False)
- Test: `tests/unit/test_task.py`

**Approach:**
- `container_getter = lambda args, kwargs: _request_container.get()` — resolved gegen den in U3 gesetzten REQUEST-Container.
- `wrap_injection` gibt Methoden ohne `FromDishka`-Param unverändert zurück → kein Effekt für R6.
- Bound-Method-Mechanik: Klassenattribut wird ersetzt; `self` landet in `*args` und wird korrekt durchgereicht.
- Wrapping **nach** `_check_run_signature_reserved_params`, damit `delay: FromDishka[X]` weiterhin scheitert (R7).
- **DependencyKeys selbst ableiten:** `wrap_injection` exponiert die injizierten `DependencyKey`s **nicht** (nur `__dishka_orig_func__`/`__dishka_injected__`). U2 berechnet sie unabhängig via `default_parse_dependency` + `get_type_hints(method, include_extras=True)` pro Lebenszyklus-Methode und legt sie als Klassenattribut `cls._dishka_injected_keys` ab — Datenvertrag für U5.
- **Forward-Ref-/`TYPE_CHECKING`-Entscheidung:** `wrap_injection` ruft `get_type_hints` eager zur Klassen-Def-Zeit (`base.py:114`) — heute läuft dort nur `inspect.signature` (keine Annotationsauflösung). Default-Entscheidung: **injizierte Lebenszyklus-Parameter müssen importzeit-auflösbar annotiert sein** (kein `TYPE_CHECKING`-only-Import / kein Forward-Ref auf später definierte Typen); ein `NameError` wird klar der Task-Klasse attribuiert. „Defensives" stilles Überspringen der Injektion ist **ausgeschlossen** (würde R4 verletzen). Alternative (falls Forward-Refs gebraucht werden): Wrapping auf Registrierungszeit verschieben, wo `get_annotations(eval_str=True)` bereits erfolgreich läuft — siehe Open Questions.

**Execution note:** Test-first — zuerst ein fehlschlagender Test, dass ein `FromDishka`-Param aus dem ContextVar-Container befüllt wird.

**Patterns to follow:** `task.py:290-294` (`__init_subclass__`), `task.py:453-482` (Reserved-Check), Dishka `integrations/celery.py` `DishkaTask` (Wrap-im-Basisklassen-Muster).

**Test scenarios:**
- Happy path: Task mit `run(self, x, svc: FromDishka[S])` → nach Set des ContextVar-Containers liefert Aufruf `svc` aus dem Container, `x` aus args (Covers AE1).
- Edge case: Task ohne `FromDishka` → `run` ist unverändert (Identität/Verhalten), keine Scope nötig (Covers AE2).
- Edge case: `run(self, delay: FromDishka[X])` → `ValueError` beim Klassen-Def (Reserved-Check, Covers AE9).
- Edge case: Return-Type-Erkennung der Registry überlebt das Wrapping (`inspect.get_annotations(run)` liefert weiterhin den Rückgabetyp).
- Edge case: `cls._dishka_injected_keys` enthält genau die `FromDishka`-Typen über `run`/`pre_execution`/`post_execution` (Datenvertrag für U5).
- Error path: ein nicht zur Klassen-Def-Zeit auflösbarer Annotations-Typ an einem injizierten Param → klarer, der Task-Klasse attribuierter `NameError` (kein stilles Überspringen).
- Error path: `_HAVE_DISHKA` False → `__init_subclass__` wrappt nicht, keine zusätzliche Inspektion (Covers AE6-Aspekt).

**Verification:** Injektion funktioniert für annotierte Params; Nicht-DI-Tasks unverändert; Reserved-Check unverändert wirksam.

---

### U3. Worker-REQUEST-Scope um den Lebenszyklus

**Goal:** Der Worker öffnet pro Task eine REQUEST-Scope, die `pre`/`run`/`post` umspannt, pro-Task-isoliert (ContextVar) und in jedem Ausgang finalisiert.

**Requirements:** R12, R13, R14, R5

**Dependencies:** U1, U2

**Files:**
- Modify: `src/swarmq/worker.py` (`_handle_message`: bei registriertem Container `async with container(context={TaskContext: ctx})`, ContextVar setzen/zurücksetzen, Block um pre/run/post; `_MISSING` → bestehender Pfad)
- Test: `tests/unit/test_worker.py`, `tests/unit/test_worker_timeouts.py`

**Approach:**
- Scope-Eintritt **innerhalb** `_handle_message` (detached-Task-Kontext → ContextVar pro Task kopiert) und **vor** `pre_execution`, innerhalb des bestehenden Error-Envelopes (Ack-Ordering).
- `TaskContext` aus `MessageContext`-Feldern (`worker.py:1580`).
- `finally`/`async with`-Exit mit `except BaseException`-Disziplin, damit Cancel/Timeout die Finalizer auslösen.
- Aufruf-Site `task_instance.run(*args, **kwargs)` bleibt unverändert.

**Execution note:** Charakterisierung zuerst — bestehende Lifecycle-/Timeout-Tests müssen ohne Container grün bleiben (Regressionsschutz), bevor der DI-Pfad ergänzt wird.

**Technical design:** siehe High-Level Technical Design (Pseudo-Code `_handle_message`).

**Patterns to follow:** structlog-Bind/Clear `worker.py:1588-1594`; `test_handle_message_binds_contextvars:634`; Lifecycle-/Timeout-Tests `test_worker.py:179/306`, `test_worker_timeouts.py:244/427`.

**Test scenarios:**
- Happy path: pre→run→post teilen denselben request-scoped Wert (z.B. dieselbe Session-Instanz in allen drei) (Covers AE4-Aspekt, AE5).
- Integration: request-scoped `yield`-Provider; `run` wirft → `post_execution` übersprungen, Finalizer läuft dennoch (Rollback) (Covers AE4).
- Integration: Soft-Timeout/Hard-Timeout/User-Cancel (nur `run_task` gecancelt) → Finalizer erhält die Exception, Scope sauber geschlossen, Disposition unverändert (Covers AE4; spiegelt `test_worker_timeouts.py`).
- Integration: **Finalizer erhält die echte Exception** — `run` wirft `ValueError`, der `yield`-Provider beobachtet beim Scope-Exit `ValueError` (nicht `None`) und rollt zurück (deckt das P1-Exception-Plumbing ab).
- Integration: **Shutdown-Cancel** — Task in Ausführung, Worker-Shutdown cancelt `_handle_message`; der geshieldete Scope-Exit-Finalizer läuft vollständig (z.B. `await session.rollback()`), bevor der Handler endet (gegen Shutdown-Drain-Pfad `worker.py:987-999`, nicht nur Watchdog-Tests).
- Edge case: **`FIRE_AND_FORGET` + Scope-Setup-Fehler** — Container wirft beim `__aenter__`; die bereits geackte Message wird **nicht** genackt, der Fehler wird geloggt/als Fehler gezählt.
- Edge case (Nebenläufigkeit): N>1 Tasks mit je eigener request-scoped Ressource via `asyncio.gather` → distinkte Instanzen, kein Leak (Covers AE10; spiegelt `test_semaphore_limits_concurrency`).
- Edge case: injizierter Logger enthält `task_id` während `run`, ContextVar nach Rückkehr zurückgesetzt (spiegelt `test_handle_message_binds_contextvars`).
- Edge case: kein Container registriert → exakt heutiger Codepfad (kein `async with`).

**Verification:** Alle bestehenden Worker-/Timeout-Tests grün; neue DI-Lifecycle- und Nebenläufigkeits-Tests grün.

---

### U4. APP-Container-Lebenszyklus (Read + Close)

**Goal:** Der Worker nutzt den global registrierten Container als APP-Scope und schließt ihn beim Shutdown.

**Requirements:** R9, R11

**Dependencies:** U1, U3

**Files:**
- Modify: `src/swarmq/worker.py` (Container beim Start aus dem Slot lesen; `await container.close()` im Shutdown-Pfad)
- Test: `tests/unit/test_worker.py`

**Approach:**
- Close im Shutdown einhängen, **nach** dem bestehenden In-Flight-Drain (`_execute_shutdown` Schritt 4, `worker.py:1052-1054`), damit laufende REQUEST-Scopes ihre Finalizer abschließen, bevor APP-Ressourcen abgebaut werden. Genau **einen** Einhängepunkt wählen (nicht zusätzlich in `stop()` `:958-961`), bzw. `close()` idempotent gegen Doppelaufruf machen.
- Nur wenn ein Container registriert ist; kein Container → kein zusätzlicher Schritt (R2/R6).
- Container entsteht über den Modul-Re-Import pro Subprozess (U-übergreifend; hier nur Read+Close, nicht Bau).

**Test scenarios:**
- Happy path: bei registriertem Container ruft der Shutdown `container.close()` genau einmal (AsyncMock-Container) (Covers AE11-Unit-Aspekt).
- Edge case: kein Container → kein `close`-Aufruf, Shutdown unverändert (Covers AE6).
- Error path: `close()` wirft → Shutdown bricht nicht hart ab (Fehler geloggt, restliche Teardown-Schritte laufen).

**Verification:** Shutdown schließt den Container idempotent; Pfad ohne Container unverändert.

---

### U5. Startup-Validierung & Fehlermeldungen (R3a/R3b)

**Goal:** Klare, frühe Fehler statt roher Tracebacks bzw. später `NoFactoryError`.

**Requirements:** R3a, R3b

**Dependencies:** U1, U2

**Files:**
- Modify: `src/swarmq/cli/worker_cmd.py` (R3a: `import_module`-`ImportError` abfangen + mit `pip install 'swarmq[dishka]'`-Hinweis als `ConfigurationError` re-raisen, wenn die Ursache ein fehlendes `dishka` ist; R3b: nach Registry-Enumeration prüfen, dass bei `FromDishka`-Nutzung ein Container registriert ist und alle injizierten Typen auflösbar sind)
- Test: `tests/unit/test_cli.py`

**Approach:**
- R3a: nur den dishka-bezogenen `ImportError` umformulieren (Ursache erkennen), andere `ImportError` unverändert propagieren.
- R3b: die in U2 berechneten `cls._dishka_injected_keys` aller registrierten Tasks gegen den Container-Graph prüfen. Bevorzugt **Graph-Introspektion ohne Factory-Aufruf** (`container.registry`/`child_registry`/`get_factory` — Achtung: REQUEST-scoped Provider liegen in der Child-Registry, nicht der APP-Registry). Diese Pfade sind **private Dishka-API** → der Version-Pin (siehe unten) ist ein **Blocker** für U5, und ein Test muss die Introspektion gegen die gepinnte Version festnageln, damit ein Upstream-Bruch in CI laut scheitert. Schlägt die Introspektion auf der gepinnten Version fehl, **degradiert R3b dokumentiert auf reine Container-Präsenz** (Typ-Mismatch dann erst zur Laufzeit).
- Validierung läuft an der Stelle der bestehenden Registry-Empty-Prüfung (`worker_cmd.py:28`).

**Patterns to follow:** `hot_reload.py:51-66` (`ConfigurationError` + Extra-Hinweis); `test_cli.py::TestRunWorkerAsync:308/326`.

**Test scenarios:**
- Error path: dishka fehlt + Modul nutzt `FromDishka` → `ConfigurationError` mit Extra-Hinweis statt rohem `ImportError` (Covers AE7; `patch.dict(sys.modules, {"dishka": None})`).
- Error path: dishka da, kein `setup_dishka` → `ConfigurationError` über fehlenden Container vor erster Message (Covers AE3, AE8-Aspekt).
- Error path: Container da, aber ein `FromDishka`-Typ ohne Provider → klare Meldung beim Start (nicht erst zur Laufzeit).
- Happy path: Container korrekt registriert, alle Typen auflösbar → Start ohne Fehler (Covers AE8).
- Edge case: anderer `ImportError` (nicht dishka) propagiert unverändert.

**Verification:** Alle drei Fehlerklassen liefern frühe, klare Meldungen; valider Fall startet normal.

---

### U6. Integration: Multi-Subprozess & Round-Trip mit Injektion

**Goal:** Nachweis, dass jeder Worker-Subprozess einen frischen APP-Container baut, Injektion end-to-end funktioniert und der Container beim sauberen Shutdown geschlossen wird.

**Requirements:** R8, R9, R11

**Dependencies:** U1, U3, U4, U5

**Files:**
- Modify/Create: `tests/integration/test_cli_worker.py` (bzw. neue Integrationsdatei) — Beispielmodul mit `make_async_container` auf Modulebene + `setup_dishka` + Task mit `FromDishka`
- Test: dieselbe Datei (integration-Marker, Docker-gated via `tests/integration/conftest.py::check_docker`)

**Approach:**
- Real-CLI via `subprocess.Popen` (Muster `test_cli_worker.py:63/115`), echte RabbitMQ+Valkey.
- Enqueue → Task läuft mit injizierter Abhängigkeit → Ergebnis verifiziert → `SIGTERM` → sauberer Exit (impliziert `container.close()`).

**Execution note:** Integration-gated; überspringt automatisch ohne Docker.

**Test scenarios:**
- Integration: Round-Trip — Task mit `FromDishka`-Dep wird über echten Worker ausgeführt, Ergebnis korrekt (Covers AE8, AE1 live).
- Integration: SIGTERM nach Ausführung → Prozess endet sauber, kein Hängen (impliziter `container.close()`) (Covers AE11).

**Verification:** Integrationslauf grün mit Docker; sauberer Worker-Exit.

---

## System-Wide Impact

- **Interaction graph:** `_handle_message` (zentral), `Task.__init_subclass__` (alle Task-Subklassen), `run_worker_async`/CLI-Bootstrap, Worker-Shutdown. Middleware/Signals unberührt.
- **Error propagation:** Scope-Setup-Fehler routen wie pre_execution-Fehler (Error-Result, bestehende Disposition); Finalizer-Fehler beim Scope-Exit werden geloggt; DI-Fehler im detached Handler dürfen nicht still verschluckt werden (Ack-Ordering-Learning).
- **State lifecycle risks:** Globaler Container-Slot (Test-Leakage → Reset-Fixture); ContextVar-Leakage zwischen nebenläufigen Tasks (Set-in-`_handle_message`); REQUEST-Finalizer in Cancel/Timeout-Pfaden.
- **API surface parity:** Neue öffentliche Symbole `setup_dishka`/`SwarmQProvider`/`TaskContext`; `Task`-API unverändert für Nicht-DI-Nutzer.
- **Integration coverage:** AE4 (Finalizer bei Exception/Timeout/Cancel), AE10 (Nebenläufigkeit), AE11 (Multi-Subprozess) — Unit-Mocks allein beweisen das nicht; daher U3-Lifecycle/Concurrency-Tests + U6-Integration.
- **Unchanged invariants:** Ohne registrierten Container und ohne installiertes dishka ist jeder Codepfad identisch zum heutigen Stand (R2, R6); Reserved-Param-Check, Serialisierung und Client bleiben unverändert.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| ContextVar zu früh/zu spät gesetzt → Cross-Task-Leak bzw. unsichtbar in `run` | Set strikt in `_handle_message` **vor** `create_task(run_coro)` (`worker.py:1659`); AE10-Concurrency-Test + Set-während/cleared-danach-Test als Gate |
| Worker swallowt Exceptions → Finalizer erhält `None`, R13-Rollback bricht | Scope-Exit treibt die erfasste Exception explizit (`__aexit__(type, exc, tb)`); Test „Finalizer sieht echte Exception" |
| Shutdown-Cancel des Handlers bricht awaitende Finalizer ab | `asyncio.shield` um Scope-Exit auf Shutdown-Pfad (Präzedenz `_detached_releases`); Test gegen Shutdown-Drain-Pfad |
| `FIRE_AND_FORGET`-Ack vor Scope → Setup-Fehler nackt geackte Message | Scope erst **nach** dem FF-Ack öffnen; Setup-Fehler loggen statt nacken; dedizierter Test |
| U2/U5-Vertrag: `wrap_injection` exponiert Keys nicht | U2 leitet Keys via `default_parse_dependency` ab, legt `cls._dishka_injected_keys` an; U5 liest Attribut |
| U5-Auflösbarkeit hängt an privater Dishka-API | Version-Pin als U5-Blocker; CI-Test gegen gepinnte Version; sonst Degradierung auf Container-Präsenz |
| Eager `get_type_hints` beim Wrappen → `NameError` bei Forward-Refs/`TYPE_CHECKING` | Annotationen injizierter Params müssen importzeit-auflösbar sein (dokumentiert); kein stilles Skip; Alternative Registrierungszeit-Wrapping |
| Globaler Slot leakt zwischen Tests | Autouse `_reset_dishka_container`-Fixture (U1) |

---

## Documentation / Operational Notes

- README/Docs: Abschnitt „Optional: Dependency Injection mit Dishka" (Container bauen, `setup_dishka`, `SwarmQProvider`, `FromDishka`-Beispiel, Scope-Modell).
- `pyproject.toml`-Extra `swarmq[dishka]` dokumentieren.
- Nach Landung: `/ce-compound`-Eintrag zu spawn + Per-Worker-Modul-Re-Import (Recherche-Lücke).

---

## Sources & References

- **Origin document:** [docs/brainstorms/2026-05-31-dishka-di-tasks-requirements.md](docs/brainstorms/2026-05-31-dishka-di-tasks-requirements.md)
- Learnings: `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`, `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`, `project_zero/docs/solutions/runtime-errors/event-system-p1-di-valkey-secrets-migration.md`
- Related code: `src/swarmq/worker.py`, `src/swarmq/task.py`, `src/swarmq/cli/worker_cmd.py`, `src/swarmq/metrics.py`, `src/swarmq/hot_reload.py`, `tests/conftest.py`, `tests/unit/test_worker.py`
- External: Dishka-Source (master) — `integrations/base.py`, `code_tools/inject_compiler.py`, `async_container.py`, `integrations/taskiq.py`, `integrations/celery.py`
