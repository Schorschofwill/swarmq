"""DI-Annotationen unter PEP 563 und PEP 649 (Plan 2026-10-12-1105, MR 7, P2/E9).

Bewusst ohne ``from __future__ import annotations``: die PEP-649-Fälle
brauchen ausgewertete (lazy) Annotationen in diesem Modul. Die PEP-563-Fälle
liegen als Moduldateien unter ``tests/unit/fixtures/dishka_modules/`` und
werden je Test unter eindeutigem Namen frisch geladen.

Test-Liste:
- S3 (Spike): Verhalten von typing/dishka, auf das T5 baut
  - S3a Key mit ForwardRef (Alias und FromComponent) ist nicht None,
    ``wrap_injection`` wirft dafür NameError
  - S3b PEP 649: ``FromDishka[Later] | None`` ist ein einziger ForwardRef
  - S3c kaputte String-Annotation ergibt SyntaxError
- 1 PEP 563, ``Annotated[Svc, FromComponent("c")]`` → gewrappt, Key mit Komponente
- 2 PEP 563, Alias ``Depends[Svc]`` → gewrappt
- 3 PEP 563, Nicht-DI ``x: Undefined`` → kein Fehler, nicht gewrappt
- 4 PEP 563, Nicht-DI mit TYPE_CHECKING-Import → kein Fehler
- 4a PEP 563, Nicht-DI ``x: "list[int"`` → kein Fehler, nicht gewrappt
- 5 String mit FromDishka und unauflösbarem Typ → NameError mit Hinweis
- 5a ``svc: FromDishka[Svc], x: OnlyTC`` → NameError mit Hinweis
- 6 Forward-Ref auf später definierte Klasse (Alias, FromComponent) →
  Definition ok, nicht gewrappt; Start → ConfigurationError (Task, Parameter)
- 6a Alias auflösbar plus ``x: OnlyTC`` → Start nennt ``x``
- 7 unauflösbar ohne DI-Key → auch beim Start kein Fehler (DI-Prüfung und
  Registrierung beim Bau der App, N2)
- 7a ``Depends[Svc] | None`` → beim Start als wirkungslos abgelehnt
- 8 PEP 649, ``Annotated[Svc, FromComponent("c")]`` → gewrappt
- 8a PEP 649, ``Depends[Later]`` → NameError mit Hinweis
- 8b PEP 649, ``FromDishka[Later] | None`` → NameError mit Hinweis
- 13 frisch geladenes Modul (CPU-Kind) wrappt ebenso
"""

import annotationlib
import importlib.util
import inspect
import pathlib
import types
import typing
from typing import Annotated, Any

import pytest
from dishka import AsyncContainer, FromComponent, FromDishka
from dishka.integrations.base import default_parse_dependency, is_dishka_injected, wrap_injection

from swarmq.dishka import validate_registered_tasks
from swarmq.exceptions import ConfigurationError
from swarmq.registry import TaskRegistry

Depends = FromDishka  # type: ignore[type-arg]  # Phoenix-Muster: Alias ohne Typargument
_FWD = annotationlib.Format.FORWARDREF


# --- Spike S3 ---------------------------------------------------------------


def _string_annotated(**annotations: str):
    """Funktion mit PEP-563-artigen String-Annotationen in diesem Modul, in
    dem ``Later`` (noch) nicht existiert -- wie bei der Klassendefinition."""

    async def run(self, g, h) -> None: ...

    run.__annotations__ = dict(annotations)
    return run


def _no_container(_args: tuple[Any, ...], _kwargs: dict[str, Any]) -> AsyncContainer:
    raise AssertionError("wird im Spike nie aufgerufen")


def test_s3a_key_with_forward_ref_is_not_none_and_wrap_injection_raises():
    func = _string_annotated(g="Depends[Later]", h='Annotated[Later, FromComponent("c")]')
    namespace = {"Depends": Depends, "Annotated": Annotated, "FromComponent": FromComponent}
    hints = typing.get_type_hints(func, localns=namespace, include_extras=True, format=_FWD)
    sig = inspect.signature(func, annotation_format=_FWD)

    for pname in ("g", "h"):
        dep = default_parse_dependency(sig.parameters[pname], hints[pname])
        assert dep is not None
        assert isinstance(dep.type_hint, typing.ForwardRef)
        assert dep.type_hint.__forward_arg__ == "Later"
    with pytest.raises(NameError, match="Later"):
        wrap_injection(func=func, is_async=True, manage_scope=False, container_getter=_no_container)


def test_s3b_pep649_optional_fromdishka_with_forward_ref_is_one_forward_ref():
    async def run(self, o: FromDishka[Later] | None) -> None: ...  # type: ignore[name-defined]  # noqa: F821 - Later absichtlich undefiniert

    ann = annotationlib.get_annotations(run, format=_FWD)
    assert isinstance(ann["o"], typing.ForwardRef)
    assert ann["o"].__forward_arg__ == "FromDishka[Later] | None"


def test_s3c_broken_string_annotation_raises_syntax_error():
    func = _string_annotated(g="list[int", h="int")
    with pytest.raises(SyntaxError):
        typing.get_type_hints(func, include_extras=True, format=_FWD)


# --- Moduldateien unter PEP 563 --------------------------------------------

_MODULE_DIR = pathlib.Path(__file__).parent / "fixtures" / "dishka_modules"


def _load(stem: str, request: pytest.FixtureRequest) -> types.ModuleType:
    """Lädt ``dishka_modules/<stem>.py`` frisch unter einem je Test eindeutigen
    Namen (wie ein Kindprozess, der das Modul neu importiert)."""
    return _load_as(stem, f"{stem}__{request.node.name}")


def _load_as(stem: str, name: str) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(name, _MODULE_DIR / f"{stem}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pep563_annotated_from_component_is_wrapped(request):
    mod = _load("dimod_pep563_component", request)

    assert is_dishka_injected(mod.ComponentTask.run)
    keys = mod.ComponentTask._dishka_injected_keys
    assert [(k.type_hint, k.component) for k in keys] == [(mod.Svc, "c")]


def test_pep563_alias_of_fromdishka_is_wrapped(request):
    mod = _load("dimod_pep563_alias", request)

    assert is_dishka_injected(mod.AliasTask.run)
    assert [k.type_hint for k in mod.AliasTask._dishka_injected_keys] == [mod.Svc]


def test_pep563_task_without_di_and_unresolvable_name_defines_unwrapped(request):
    mod = _load("dimod_pep563_plain_undefined", request)

    assert not is_dishka_injected(mod.PlainUndefinedTask.run)
    assert not hasattr(mod.PlainUndefinedTask, "_dishka_injected_keys")


def test_pep563_task_without_di_and_type_checking_import_defines(request):
    mod = _load("dimod_pep563_plain_type_checking", request)

    assert not is_dishka_injected(mod.PlainTypeCheckingTask.run)


def test_pep563_task_without_di_and_broken_string_annotation_defines_unwrapped(request):
    mod = _load("dimod_pep563_plain_broken", request)

    assert not is_dishka_injected(mod.PlainBrokenTask.run)


def test_pep563_fromdishka_text_with_unresolvable_type_keeps_hinted_name_error(request):
    with pytest.raises(NameError, match="must be importable at class-definition time"):
        _load("dimod_pep563_fromdishka_undefined", request)


def test_pep563_fromdishka_with_type_checking_only_param_keeps_hinted_name_error(request):
    with pytest.raises(NameError, match="must be importable at class-definition time"):
        _load("dimod_pep563_fromdishka_type_checking", request)


def test_pep563_di_key_with_forward_ref_defines_unwrapped(request):
    mod = _load("dimod_pep563_forward_ref", request)

    assert not is_dishka_injected(mod.AliasForwardTask.run)
    assert not is_dishka_injected(mod.ComponentForwardTask.run)


@pytest.mark.parametrize(
    ("task_attr", "task_name"),
    [("AliasForwardTask", "dimod_alias_forward"), ("ComponentForwardTask", "dimod_component_forward")],
)
def test_pep563_di_key_with_forward_ref_fails_worker_start(request, task_attr, task_name):
    mod = _load("dimod_pep563_forward_ref", request)

    with pytest.raises(ConfigurationError) as excinfo:
        validate_registered_tasks([getattr(mod, task_attr)])
    message = str(excinfo.value)
    assert task_name in message
    assert "run.later" in message
    assert "class definition" in message


@pytest.mark.parametrize(
    ("stem", "task_attr"),
    [
        ("dimod_pep563_plain_undefined", "PlainUndefinedTask"),
        ("dimod_pep563_plain_broken", "PlainBrokenTask"),
        ("dimod_pep563_plain_type_checking", "PlainTypeCheckingTask"),
    ],
)
def test_pep563_task_without_di_key_passes_worker_start(request, stem, task_attr):
    """Test 7 (Grenze, Abschnitt 7): unauflösbar und ohne erkennbaren DI-Key ->
    kein Fehler, keine Warnung. Ebenso eine kaputte String-Annotation. N2:
    auch die Registrierung beim Bau der App (``SwarmQ(...)``) scheitert nicht."""
    mod = _load(stem, request)
    task = getattr(mod, task_attr)

    validate_registered_tasks([task])
    TaskRegistry().register(task)


def test_pep563_alias_with_unresolvable_non_di_param_names_that_param_at_start(request):
    mod = _load("dimod_pep563_alias_type_checking", request)
    assert not is_dishka_injected(mod.AliasTypeCheckingTask.run)

    with pytest.raises(ConfigurationError) as excinfo:
        validate_registered_tasks([mod.AliasTypeCheckingTask])
    message = str(excinfo.value)
    assert "dimod_alias_type_checking" in message
    assert "run.x is unresolvable" in message


def test_pep563_optional_alias_is_rejected_as_ineffective_at_start(request):
    """Test 7a: bewusst neu (Changelog) -- wie ``FromDishka[X] | None``."""
    mod = _load("dimod_pep563_alias_optional", request)

    with pytest.raises(ConfigurationError, match=r"never inject: dimod_alias_optional: run\.svc"):
        validate_registered_tasks([mod.AliasOptionalTask])


# --- Moduldateien unter PEP 649 (ohne Future-Import) ----------------------


def test_pep649_annotated_from_component_is_wrapped(request):
    mod = _load("dimod_pep649_component", request)

    assert is_dishka_injected(mod.Pep649ComponentTask.run)
    keys = mod.Pep649ComponentTask._dishka_injected_keys
    assert [(k.type_hint, k.component) for k in keys] == [(mod.Svc, "c")]


def test_pep649_alias_with_forward_ref_raises_hinted_name_error_at_definition(request):
    with pytest.raises(NameError, match="must be importable at class-definition time"):
        _load("dimod_pep649_alias_forward", request)


def test_pep649_optional_fromdishka_forward_ref_raises_hinted_name_error_at_definition(request):
    with pytest.raises(NameError, match="must be importable at class-definition time"):
        _load("dimod_pep649_optional_forward", request)


def test_fresh_reimport_wraps_again_like_a_cpu_child(request):
    """Test 13: das CPU-Kind importiert das Modul neu; das Wrappen bei der
    Klassendefinition gilt dort ebenso (jede Ladung ist eigenständig)."""
    parent = _load("dimod_pep563_component", request)
    child = _load_as("dimod_pep563_component", f"child__{request.node.name}")

    assert child.ComponentTask is not parent.ComponentTask
    assert is_dishka_injected(child.ComponentTask.run)
    assert [(k.type_hint, k.component) for k in child.ComponentTask._dishka_injected_keys] == [(child.Svc, "c")]
