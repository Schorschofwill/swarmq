# Lokaler Teil-Stub fuer msgpack 1.x (liefert keine Typinformationen).
# Deckt nur die von swarmq und seinen Tests genutzte Oberflaeche ab.
from collections.abc import Callable
from typing import Any, NamedTuple

class ExtType(NamedTuple):
    code: int
    data: bytes

class UnpackException(Exception): ...
class BufferFull(UnpackException): ...
class OutOfData(UnpackException): ...
class FormatError(ValueError, UnpackException): ...
class StackError(ValueError, UnpackException): ...

UnpackValueError = ValueError

class ExtraData(UnpackValueError):
    unpacked: Any
    extra: bytes

def packb(
    o: Any,
    *,
    default: Callable[[Any], Any] | None = ...,
    use_single_float: bool = ...,
    use_bin_type: bool = ...,
    strict_types: bool = ...,
    datetime: bool = ...,
    unicode_errors: str | None = ...,
) -> bytes: ...
def unpackb(
    packed: bytes | bytearray | memoryview,
    *,
    raw: bool = ...,
    use_list: bool = ...,
    timestamp: int = ...,
    strict_map_key: bool = ...,
    object_hook: Callable[[dict[Any, Any]], Any] | None = ...,
    object_pairs_hook: Callable[[list[tuple[Any, Any]]], Any] | None = ...,
    list_hook: Callable[[list[Any]], Any] | None = ...,
    unicode_errors: str | None = ...,
    ext_hook: Callable[[int, bytes], Any] = ...,
    max_str_len: int = ...,
    max_bin_len: int = ...,
    max_array_len: int = ...,
    max_map_len: int = ...,
    max_ext_len: int = ...,
) -> Any: ...

loads = unpackb
dumps = packb
