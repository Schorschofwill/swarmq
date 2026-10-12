"""Property-based tests for canonical_encode + compute_dedup_hash.

E2E-Hardening Phase 3 U3.2. Pins the contract that the dedup-hash
producer-side SETNX relies on in R3.5 Client.schedule and
Scheduler._fire_one:

1. Determinism — same (task_name, args, kwargs) always hash identically.
2. Permutation invariance — dict kwarg ordering is irrelevant.
3. Collision rate — distinct inputs hit distinct cells (sha256 floor).
4. NaN / Inf rejection — unstable JSON representations are refused even
   when buried inside nested dicts/lists.
5. Unicode normalisation drift — NFC and NFD are intentionally treated
   as distinct inputs (documented caller-responsibility behaviour).

Run via ``uv run --extra test pytest tests/property -m property``.
"""

from __future__ import annotations

import itertools
import math
import unicodedata
from typing import Any

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from swarmq.serialization import canonical_encode, compute_dedup_hash

# ---------------------------------------------------------------------------
# Shared JSON-compatible strategies
# ---------------------------------------------------------------------------

json_atoms = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-1_000_000, max_value=1_000_000),
    st.floats(allow_nan=False, allow_infinity=False, width=64),
    st.text(max_size=20),
)

json_value = st.recursive(
    json_atoms,
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.dictionaries(st.text(max_size=10), children, max_size=5),
    ),
    max_leaves=20,
)

# args must be a list/tuple of json_values; kwargs is a str->json_value dict.
args_strategy = st.lists(json_value, max_size=5)
kwargs_strategy = st.dictionaries(st.text(max_size=10), json_value, max_size=5)


# ---------------------------------------------------------------------------
# 1. Determinism
# ---------------------------------------------------------------------------


@given(args=args_strategy, kwargs=kwargs_strategy)
def test_determinism_same_args_same_hash(args: list[Any], kwargs: dict[str, Any]) -> None:
    """Same inputs → same hash, every invocation.

    This is the bedrock of SETNX-based dedup: if hash(x) is not stable,
    two concurrent Client.schedule(x) calls map to different Valkey
    cells and both tasks fire.
    """
    h1 = compute_dedup_hash("my.task", tuple(args), kwargs)
    h2 = compute_dedup_hash("my.task", tuple(args), kwargs)
    assert h1 == h2
    assert len(h1) == 64  # sha256-hex


# ---------------------------------------------------------------------------
# 2. Permutation invariance for dict kwargs
# ---------------------------------------------------------------------------


@given(
    kwargs=st.dictionaries(
        st.text(min_size=1, max_size=10),
        json_atoms,
        min_size=2,
        max_size=5,
    )
)
def test_permutation_invariance_for_dict_kwargs(kwargs: dict[str, Any]) -> None:
    """Callsite kwarg-insertion-order must not change the hash.

    Python dicts preserve insertion order; we iterate a few
    permutations of the key order and rebuild the dict. All must
    produce the same hash.
    """
    assume(len(kwargs) >= 2)
    keys = list(kwargs.keys())
    # Try up to 6 permutations (keeps runtime bounded for size-5 dicts).
    perms = list(itertools.islice(itertools.permutations(keys), 6))
    hashes = {compute_dedup_hash("t", (), {k: kwargs[k] for k in perm}) for perm in perms}
    assert len(hashes) == 1, f"Hash changed across kwarg permutations: {hashes}"


# ---------------------------------------------------------------------------
# 3. Collision rate below threshold
# ---------------------------------------------------------------------------


def test_collision_rate_below_threshold() -> None:
    """Across 1000 distinct inputs, collisions must be < 0.5%.

    We build the 1000 inputs deterministically via itertools.product
    (hypothesis cannot cheaply guarantee set-level uniqueness at this
    scale). sha256 over structured JSON should give zero collisions
    in practice — the 0.5% threshold is generous for CI-flake tolerance.
    """
    inputs = []
    for i, (a, b, c) in enumerate(itertools.product(range(10), range(10), range(10))):
        inputs.append(((a, b, c), {"idx": i}))
    assert len(inputs) == 1000

    hashes = {compute_dedup_hash("collision.test", a, k) for a, k in inputs}
    unique = len(hashes)
    collisions = len(inputs) - unique
    rate = collisions / len(inputs)
    assert rate < 0.005, f"Collision rate {rate:.4f} exceeds 0.5% ({collisions} collisions in {len(inputs)} inputs)"


# ---------------------------------------------------------------------------
# 4. NaN / Inf rejection at nested positions
# ---------------------------------------------------------------------------


def _inject_bad_float(container: object, bad: float, depth: int = 0) -> object:
    """Return a new structure with `bad` spliced into a nested position.

    Rebuilds dict/list containers recursively so the injected value
    is reachable via _canonicalize's recursion.
    """
    if depth >= 3:
        return bad  # bottomed out — caller wraps this
    if isinstance(container, dict):
        new: dict[str, Any] = {str(k): v for k, v in container.items()}
        new["__injected__"] = bad
        return new
    if isinstance(container, list):
        return [*container, bad]
    return bad


nested_container = st.recursive(
    st.one_of(
        st.lists(json_atoms, max_size=3), st.dictionaries(st.text(min_size=1, max_size=5), json_atoms, max_size=3)
    ),
    lambda children: st.one_of(
        st.lists(children, max_size=3),
        st.dictionaries(st.text(min_size=1, max_size=5), children, max_size=3),
    ),
    max_leaves=8,
)


@given(
    container=nested_container,
    bad=st.sampled_from([math.nan, math.inf, -math.inf]),
)
@settings(max_examples=200)
def test_nan_inf_rejection_in_nested_position(container: list[Any] | dict[str, Any], bad: float) -> None:
    """NaN / ±Inf buried in any nested position must raise TypeError.

    Guards ADV-003: msgspec encodes NaN→null and Inf→Infinity,
    either colliding with None or breaking cross-version stability.
    """
    # Wrap so the bad float is definitely reached by the canonicalizer.
    poisoned = [container, _inject_bad_float(container, bad)]
    with pytest.raises(TypeError, match="NaN"):
        canonical_encode(tuple(poisoned), {})


# ---------------------------------------------------------------------------
# 5. Unicode NFC vs NFD drift — documented behaviour
# ---------------------------------------------------------------------------


@given(
    # Text drawn from the Latin Extended range plus combining marks so
    # that NFC and NFD forms actually differ. Filter to inputs whose
    # two normalisation forms are distinct byte sequences.
    text=st.text(
        alphabet=st.characters(
            whitelist_categories=("Ll", "Lu", "Mn"),
            min_codepoint=0x00C0,
            max_codepoint=0x024F,
        ),
        min_size=1,
        max_size=10,
    ),
)
def test_unicode_normalisation_documented_drift(text: str) -> None:
    """NFC and NFD forms of the same grapheme cluster hash differently.

    This is documented, intentional behaviour: canonical_encode does
    NOT normalise unicode — callers must pre-normalise if they want
    semantic equivalence across input sources. Pinning the current
    behaviour here catches accidental changes that would silently
    deduplicate semantically-equivalent-but-byte-distinct strings.
    """
    nfc = unicodedata.normalize("NFC", text)
    nfd = unicodedata.normalize("NFD", text)
    assume(nfc != nfd)  # skip inputs where the two forms coincide

    h_nfc = compute_dedup_hash("task", (nfc,), {})
    h_nfd = compute_dedup_hash("task", (nfd,), {})
    assert h_nfc != h_nfd, (
        "NFC and NFD produced the same hash — this would silently "
        "dedup semantically-equivalent-but-byte-distinct inputs. "
        "If this is now intended, update the docstring contract."
    )
