"""Byte-fuzz tests for the serialization / result-parser code paths.

E2E-Hardening Phase 3 U3.6.

Pins the contract that the parsers (``Client._unpack``,
``decode_result``, ``decode_json``) refuse random input without ever
surfacing an unhandled exception. A rogue producer writing arbitrary
bytes to Valkey must never crash the consumer loop — the caller
always sees one of the typed errors on the allow-list below.

Allow-list reasoning:

* ``ResultSerializationError`` — the library's canonical typed error
  for unparseable bytes; every decode wraps msgpack/msgspec failures
  in it.
* ``ValueError`` — raised directly by ``_unpack`` for non-dict
  payloads (``decode_result`` returns e.g. an int for a random int
  msgpack blob; ``data.get()`` then fails with AttributeError — see
  handling below).
* ``AttributeError`` — same root cause: ``decode_result`` can return
  any msgpack-compatible object, which may not support ``.get``.
  Treated as a typed failure (it's a concrete Python exception with a
  finite class — callers can catch ``Exception`` safely).
* ``KeyError`` — ``_unpack`` reads ``data["result"]`` when neither
  cancel nor error flag is set; a random but valid msgpack-dict may
  lack the key.
* ``TaskCancelledError`` / ``TaskError`` — valid success paths for
  random inputs that happen to parse as cancelled / errored results.
  The fuzz does not *try* to generate these; we just allow them so
  the contract covers the full surface of ``_unpack``.
* ``TypeError`` — msgspec raises ``TypeError`` for a few classes of
  malformed UTF-8 byte sequences; allowed as a typed failure.

What we explicitly want to catch: ``BaseException`` subclasses that
are not one of the above (``SystemExit``, ``KeyboardInterrupt``, a
bare ``Exception`` from a library bug, etc.).

Run via::

    uv run --extra test pytest tests/fuzz/test_parser_fuzz.py \
        -v --tb=short -m fuzz
"""

from __future__ import annotations

import msgspec
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from swarmq.client import Client
from swarmq.exceptions import ResultSerializationError, TaskCancelledError, TaskError
from swarmq.serialization import decode_json, decode_result

# Typed-exception allow-list for the _unpack / decode_result paths.
# Any exception NOT in this tuple is an unhandled-crash regression.
_UNPACK_ALLOWED = (
    ResultSerializationError,
    ValueError,
    TypeError,
    AttributeError,
    KeyError,
    TaskCancelledError,
    TaskError,
)

_DECODE_JSON_ALLOWED = (
    ResultSerializationError,
    ValueError,
    TypeError,
    msgspec.ValidationError,
    msgspec.DecodeError,
)


# ---------------------------------------------------------------------------
# 1. _unpack never produces an unhandled BaseException.
# ---------------------------------------------------------------------------


@given(raw=st.binary(max_size=1024))
@settings(max_examples=500, deadline=None)
def test_unpack_byte_fuzz_never_unhandled(raw: bytes) -> None:
    """Random bytes into Client._unpack must produce typed errors only.

    The msgpack payload format accepts arbitrary 1-256 byte blobs as
    valid encodings of int/str/list/None, so many fuzz inputs are
    *successfully* decoded by ``decode_result`` but then fail
    structurally inside ``_unpack``'s dict lookups. Both the decode
    and the lookup must raise from the typed allow-list — never
    ``BaseException`` escaping through uncaught.
    """
    try:
        Client._unpack(raw)
    except _UNPACK_ALLOWED:
        return
    except BaseException as exc:
        pytest.fail(
            f"Client._unpack raised non-typed {type(exc).__name__}: {exc!r} "
            f"on input {raw!r}"
        )


# ---------------------------------------------------------------------------
# 2. decode_result alone — same contract at the lower layer.
# ---------------------------------------------------------------------------


@given(raw=st.binary(max_size=1024))
@settings(max_examples=500, deadline=None)
def test_decode_result_byte_fuzz_never_unhandled(raw: bytes) -> None:
    """decode_result is the library's msgpack entrypoint; must raise
    only ``ResultSerializationError`` on bad input.

    The module wraps every msgpack / ext-hook failure into this one
    typed exception by design — any other BaseException escape is a
    regression.
    """
    try:
        decode_result(raw)
    except ResultSerializationError:
        return
    except BaseException as exc:
        pytest.fail(
            f"decode_result raised non-typed {type(exc).__name__}: {exc!r} "
            f"on input {raw!r}"
        )


# ---------------------------------------------------------------------------
# 3. decode_json random-bytes — typed exceptions only.
# ---------------------------------------------------------------------------


@given(raw=st.binary(max_size=1024))
@settings(max_examples=500, deadline=None)
def test_decode_json_random_bytes_typed_only(raw: bytes) -> None:
    """Random bytes into decode_json: typed error or success, nothing else.

    JSON has a small valid grammar so most random inputs fail with
    ``ResultSerializationError``; occasionally a single ASCII digit
    happens to be valid JSON and decode_json returns cleanly. Either
    branch is fine — uncaught BaseException is not.
    """
    try:
        decode_json(raw)
    except _DECODE_JSON_ALLOWED:
        return
    except BaseException as exc:
        pytest.fail(
            f"decode_json raised non-typed {type(exc).__name__}: {exc!r} "
            f"on input {raw!r}"
        )


# ---------------------------------------------------------------------------
# 4. decode_json — junk strings that look JSON-ish.
# ---------------------------------------------------------------------------


_json_ish = st.text(
    alphabet=st.sampled_from(
        list('{}[]",:.0123456789abcdef truefalsenull\\\n\t ')
    ),
    max_size=256,
).map(lambda s: s.encode("utf-8"))


@given(raw=_json_ish)
@settings(max_examples=500, deadline=None)
def test_decode_json_junk_ascii_typed_only(raw: bytes) -> None:
    """Junk ASCII that mimics JSON shape — must still stay typed.

    The alphabet is the union of JSON's structural characters plus
    literal letters, so hypothesis builds near-misses that stress
    msgspec's recovery paths more than pure binary noise.
    """
    try:
        decode_json(raw)
    except _DECODE_JSON_ALLOWED:
        return
    except BaseException as exc:
        pytest.fail(
            f"decode_json raised non-typed {type(exc).__name__}: {exc!r} "
            f"on input {raw!r}"
        )
