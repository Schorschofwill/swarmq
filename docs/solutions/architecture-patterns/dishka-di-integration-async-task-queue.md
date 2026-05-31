---
title: Integrating Dishka Dependency Injection into an Async Multiprocess Task Queue
date: 2026-05-31
category: docs/solutions/architecture-patterns
module: swarmq.dishka / swarmq.worker / swarmq.task
problem_type: architecture_pattern
component: background_job
severity: medium
applies_when:
  - Integrating a dependency-injection container into an async task/worker queue
  - "Tasks need per-execution dependencies injected via FromDishka[T] without a per-task @inject decorator"
  - Worker processes are spawned as subprocesses that re-import user modules and must rebuild the container fresh
  - "DI is an optional extra (swarmq[dishka]) and the import path must stay safe when dishka is absent"
  - A REQUEST scope must be opened/closed around each task and the APP container closed on worker shutdown
tags:
  - dependency-injection
  - dishka
  - async-task-queue
  - request-scope
  - multiprocess
  - contextvars
  - task-lifecycle
  - optional-dependency
related_components:
  - swarmq.task
  - swarmq.worker
  - swarmq.dishka
---

# Integrating Dishka Dependency Injection into an Async Multiprocess Task Queue

## Context

SwarmQ is an async, multiprocess task queue: a parent process spawns worker subprocesses via `multiprocessing` with the `'spawn'` start method; each worker runs an asyncio loop that pulls messages off RabbitMQ and, per message, launches the task lifecycle (`pre_execution` → `run` → `post_execution`) as a **detached `asyncio.create_task` under a concurrency semaphore**. Many tasks run concurrently in one process, interleaved on a single loop.

Dependency injection (DB sessions, HTTP clients, repositories) belongs in a task queue for the same reason it belongs in a web framework: tasks should declare what they need (`svc: FromDishka[Svc]`) rather than hand-wire construction, and per-execution resources (a DB transaction scoped to one task run) need deterministic commit-on-success / rollback-on-error.

The integration is non-obvious because three properties of the runtime each break the naive approach:

- **Concurrency** — a single per-process container or a thread-local can't isolate the per-task REQUEST scope; concurrent tasks would share/leak each other's injected resources.
- **Detached tasks** — the REQUEST container must be visible inside the `create_task`-launched coroutine, which means context must be established *before* the task is created (asyncio copies the `contextvars.Context` at task-creation time).
- **Spawn** — the container can't be pickled and shipped to a subprocess; it must be rebuilt per process, and must be constructible *without* a running loop (the parent imports the module too).

Dishka fits (APP/REQUEST scopes, `from_context`, `wrap_injection`), but each property dictates a specific, load-bearing way of wiring it in.

## Guidance

**1. Auto-wrap lifecycle methods in `Task.__init_subclass__` — no `@inject` per task.**
`wrap_injection(manage_scope=False)` is applied to `run`/`pre_execution`/`post_execution` at class-definition time, so users never decorate methods. `wrap_injection` returns the function unchanged when no parameter is a `FromDishka`, so non-DI tasks are untouched. Run it *after* any existing signature checks (e.g. reserved-param validation), so those still see the raw signature.

```python
for name in ("run", "pre_execution", "post_execution"):
    func = cls.__dict__.get(name)
    if func is None or is_dishka_injected(func):
        continue
    # ... only when it declares FromDishka params (see #2) ...
    setattr(cls, name, wrap_injection(
        func=func, is_async=True, manage_scope=False,
        container_getter=_get_request_container,
    ))
```

**2. Gate the eager `get_type_hints` behind a cheap raw-annotation pre-check.**
Calling `get_type_hints(func)` at class-def time on *every* task would raise `NameError` for existing tasks whose annotations are forward-refs or `TYPE_CHECKING`-only — a regression for code that has nothing to do with DI, triggered the moment dishka is installed. First do a cheap check on the raw annotations (string form *and* already-evaluated `Annotated` form); only genuine DI methods pay the `get_type_hints` cost (and get a strict, attributed error if their `FromDishka` annotation can't resolve).

```python
def _raw_mentions_fromdishka(param, raw):
    if isinstance(raw, str):                       # PEP 563 string annotation
        return "FromDishka" in raw
    return default_parse_dependency(param, raw) is not None  # already-Annotated
```

**3. `wrap_injection` does not expose the injected keys — derive them independently.**
For startup validation you need the `DependencyKey`s, but `wrap_injection`'s result doesn't surface them (only `__dishka_injected__` / `__dishka_orig_func__`). Re-derive via `default_parse_dependency` over resolved hints and stash on the class.

```python
hints = get_type_hints(func, include_extras=True)
method_keys = [dep for pname, param in sig.parameters.items()
               if (hint := hints.get(pname)) is not None
               and (dep := default_parse_dependency(param, hint)) is not None]
cls._dishka_injected_keys = collected   # consumed by worker-start validation
```

**4. `manage_scope=False` + a module-level `ContextVar` carrying the per-task REQUEST container; `set()` BEFORE `create_task`.**
The wrapper reads the container from a `ContextVar` instead of managing its own scope. The worker must `request_container.set(...)` *before* `asyncio.create_task(run_coro)`, because the run task copies the current `Context` at creation — set it after and the injected method sees an empty var. This is what isolates concurrent tasks. Precedent in the same worker: `structlog.contextvars`.

```python
request_container: ContextVar[AsyncContainer] = ContextVar("swarmq_request_container")

def _get_request_container(args, kwargs) -> AsyncContainer:
    try:
        return request_container.get()
    except LookupError:
        raise RuntimeError("No active SwarmQ request scope; FromDishka only "
                           "works when the task runs through a worker.") from None
```

**5. Drive `__aexit__` with the captured exception explicitly — the worker swallows task errors.**
The worker catches task exceptions internally (sets `is_error`, does *not* re-raise). A plain `async with container(...)` would therefore see `__aexit__(None, None, None)` even on failure, and any `yield`-based provider finalizer would *commit* instead of rolling back. Dishka forwards the passed exception into `yield`-finalizers via `agen.asend(exception)`, so pass the captured exception. On user-cancel (no `task_exception`), pass `CancelledError` so finalizers treat it as an abort.

```python
_di_exc = asyncio.CancelledError() if is_cancelled else task_exception
_di_et  = type(_di_exc) if _di_exc is not None else None
_di_tb  = _di_exc.__traceback__ if _di_exc is not None else None
await _di_scope.__aexit__(_di_et, _di_exc, _di_tb)   # scheduled + shielded, see #7
```

**6. `AsyncContainer.__aenter__` is a no-op (`return self`); the child scope is created in `__call__(context=...)`.**
So there is no "`__aenter__` failure" hazard to defend against, and the idiomatic way to inject per-request data is `from_context` + `container(context={TaskContext: ctx})`.

```python
_di_scope   = _di_container(context={TaskContext: TaskContext(
    task_id=task_id, task_name=task_name, queue=queue, retry_count=retry_count)})
_di_request = await _di_scope.__aenter__()
_di_token   = request_container.set(_di_request)
```

**7. Tear the scope down with the detached-shield pattern.**
A worker-shutdown cancel can land inside the handler's `finally`. If `__aexit__` were awaited directly, the cancel would abort the finalizer mid-commit/rollback. Schedule `__aexit__` as its own tracked task and `await asyncio.shield(...)`; the shutdown drain awaits those tracked tasks before closing the backend. (SwarmQ already uses this exact pattern for lock release.)

```python
_di_exit = asyncio.create_task(_di_scope.__aexit__(_di_et, _di_exc, _di_tb))
self._detached_releases.add(_di_exit)
_di_exit.add_done_callback(self._detached_releases.discard)
try:
    await asyncio.shield(_di_exit)
except asyncio.CancelledError:
    raise   # _di_exit survives (shielded) and is drained at shutdown
```

**8. APP scope = worker process; close the container at shutdown after the in-flight drain.**
APP-scoped resources (pools etc.) live for the process. Close *after* in-flight tasks and the detached-release set have drained, so live REQUEST scopes are already finalized. A close failure is logged, not raised, so it cannot abort shutdown.

**9. Validate provider resolvability at worker start, not at first task run.**
Dishka defers `NoFactoryError` to resolution time (mid-task — possibly days later for a rare/cron task). Walk the registry graph up front to catch a missing provider at startup. The introspection uses private dishka internals, so guard it: if the surface changes across versions, degrade to container-presence-only and **log** the degradation rather than blocking a valid worker.

```python
reg = container.registry
while reg is not None:
    if reg.get_factory(key) is not None:   # key from cls._dishka_injected_keys
        return True
    reg = reg.child_registry
return False   # -> ConfigurationError at start, naming task -> missing type
```

Note: `DependencyKey`s derived in #3 already carry the correct component (`DEFAULT_COMPONENT`, the empty string `''`, not `None`), so they can be passed to `get_factory` directly.

**10. `spawn` re-imports the user module per subprocess — a module-level container is naturally spawn-safe.**
A module-level `make_async_container()` + `setup_dishka()` rebuilds the container fresh in every worker subprocess on re-import; nothing is pickled across the process boundary. `make_async_container` constructs *without* a running loop (factories are lazy), so it is safe at import time in both the parent and the worker. The worker reads the container from a global slot (mirroring how tasks register into a global list) — it does not, and need not, use the user's `app` instance.

## Why This Matters

- **#1 / #2 wrong → import-time regression.** Eager `get_type_hints` on every task (or requiring `@inject`) breaks existing tasks with forward-ref / `TYPE_CHECKING` annotations the moment dishka is installed — a silent, broad regression unrelated to DI.
- **#4 wrong → cross-task leakage.** A non-`ContextVar` container, or `set()` *after* `create_task`, means concurrent tasks either share one REQUEST scope or see no scope at all — the worst class of bug: intermittent, data-corrupting, load-dependent.
- **#5 wrong → silent commit-on-error.** Because the worker swallows task exceptions, a plain `async with` finalizer always commits — failed tasks persist partial writes with no error surfaced.
- **#7 wrong → finalizers aborted on shutdown.** A shutdown cancel mid-`__aexit__` leaves transactions half-finalized and connections leaked, exactly when the system is already under stress.
- **#9 wrong → runtime `NoFactoryError`.** A misconfigured container starts fine and fails on the *first* task of that type instead of failing the worker at boot.
- **#3 wrong → no startup validation possible; #6 wrong → wasted defensive code around a no-op; #8 / #10 wrong → leaked pools or an attempt to ship an unpicklable container across `spawn`.**

## When to Apply

Integrating any IoC/DI container into an async worker or task-execution loop — Celery-style queues, message consumers, job runners, cron executors — especially when tasks run as detached coroutines, the loop swallows task exceptions internally, workers are spawned subprocesses, and you want per-execution (REQUEST-scoped) resources with deterministic commit/rollback. The specific dishka APIs (`wrap_injection`, `from_context`, `manage_scope=False`, registry walk) map onto any container offering scope + context + injection-wrapping primitives.

## Examples

**Before — manual dependency wiring inside the task:**

```python
class SendInvoice(Task):
    task_info = TaskInfo(name="send_invoice", queues=["billing"])

    async def run(self, invoice_id: str):
        session = await SessionFactory.create()
        try:
            await InvoiceService(session).send(invoice_id)
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
```

**After — declare the dependency; the framework scopes and finalizes it:**

```python
from dishka import FromDishka

class SendInvoice(Task):
    task_info = TaskInfo(name="send_invoice", queues=["billing"])

    async def run(self, invoice_id: str, svc: FromDishka[InvoiceService]):
        await svc.send(invoice_id)
        # commit-on-success / rollback-on-error happens in the provider's
        # yield-finalizer, driven by the worker (Guidance #5)
```

**Module-level container + `setup_dishka` (spawn-safe, Guidance #10) — runs in every worker subprocess on re-import:**

```python
# tasks.py  (the module passed as `swarmq worker --module tasks`)
from collections.abc import AsyncIterator
from dishka import Provider, Scope, make_async_container, provide
from swarmq import Task, TaskInfo, setup_dishka, SwarmQProvider, TaskContext

class AppProvider(Provider):
    scope = Scope.REQUEST

    @provide
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with SessionFactory() as s:
            yield s            # post-yield runs on __aexit__ with the task's exc
            await s.commit()   # only reached when exc is None (success)

    @provide
    def invoice_service(self, s: AsyncSession, ctx: TaskContext) -> InvoiceService:
        return InvoiceService(s, task_id=ctx.task_id)

# Built at module level — no running loop needed (factories are lazy);
# rebuilt fresh in each spawned worker because spawn re-imports this module.
# SwarmQProvider exposes the per-task TaskContext at REQUEST scope.
container = make_async_container(AppProvider(), SwarmQProvider())
setup_dishka(container)
```

## Related

- `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md` — the worker's detached-task dispatch, semaphore throttling, and `__aenter__`/`__aexit__` partial-cleanup discipline that DI scopes must hook into.
- `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md` — the `_handle_message` pipeline-stage ordering and FIRE_AND_FORGET ack disposition that constrain where a per-message DI scope boundary may live.
- `project_zero` repo: `docs/solutions/runtime-errors/event-system-p1-di-valkey-secrets-migration.md` — prior art on Dishka+taskiq DI wiring failure modes (unregistered-provider `NoFactoryError`, wrong DI tokens, `dishka.integrations.*` `inject` import). Different integration surface (taskiq/FastAPI), complements rather than supersedes; its "Background Tasks (DI Wiring)" checklist is portable.
- Dishka source (pinned `>=1.10.1`): `integrations/base.py` (`wrap_injection`), `code_tools/inject_compiler.py` (`manage_scope` semantics), `async_container.py` (`__call__`/`__aexit__`/exception forwarding), `integrations/taskiq.py` & `integrations/celery.py` (closest official analogues).
