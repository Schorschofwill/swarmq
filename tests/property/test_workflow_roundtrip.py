"""Property tests: workflow topology to_dict/from_dict roundtrip (U1, R1).

Any valid topology must survive ``from_dict(to_dict(x))`` byte-identically
at the dict level. Opt-in under ``-m property``.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from swarmq.workflow import (
    DEFAULT_MAX_GROUP_SIZE,
    Chain,
    Chord,
    Group,
    Signature,
    chain,
    chord,
    group,
    sig,
)

json_atoms = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-1_000_000, max_value=1_000_000),
    st.text(max_size=20),
)
json_value = st.recursive(
    json_atoms,
    lambda children: st.one_of(
        st.lists(children, max_size=4),
        st.dictionaries(st.text(max_size=8), children, max_size=4),
    ),
    max_leaves=10,
)
args_strategy = st.lists(json_value, max_size=4)
kwargs_strategy = st.dictionaries(st.text(min_size=1, max_size=8), json_value, max_size=4)
task_names = st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12)


@st.composite
def signatures(draw) -> Signature:
    return sig(
        draw(task_names),
        *draw(args_strategy),
        immutable=draw(st.booleans()),
        **draw(kwargs_strategy),
    )


@st.composite
def groups(draw) -> Group:
    members = draw(st.lists(signatures(), min_size=1, max_size=5))
    return group(*members)


# Flat-DAG: a chain step may be a signature, a group, or a chord; depth
# stays well under the limit so construction always succeeds.
@st.composite
def chains(draw) -> Chain:
    steps = draw(
        st.lists(
            st.one_of(
                signatures(),
                groups(),
                st.builds(chord, groups(), signatures()),
            ),
            min_size=1,
            max_size=5,
        )
    )
    return chain(*steps)


@given(signatures())
def test_signature_roundtrip(s: Signature):
    assert Signature.from_dict(s.to_dict()).to_dict() == s.to_dict()


@given(groups())
def test_group_roundtrip(g: Group):
    assert Group.from_dict(g.to_dict()).to_dict() == g.to_dict()


@given(chains())
def test_chain_roundtrip(c: Chain):
    assert Chain.from_dict(c.to_dict()).to_dict() == c.to_dict()


@given(st.builds(chord, groups(), signatures()))
def test_chord_roundtrip(ch: Chord):
    assert Chord.from_dict(ch.to_dict()).to_dict() == ch.to_dict()


def test_group_size_strategy_within_limit():
    # Guard: the strategies never approach the size cap, so roundtrip tests
    # exercise valid topologies only.
    assert DEFAULT_MAX_GROUP_SIZE >= 1000
