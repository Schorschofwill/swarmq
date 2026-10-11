# Lokaler Teil-Stub fuer croniter 6.x (liefert keine Typinformationen).
# Deckt nur die von swarmq und seinen Tests genutzte Oberflaeche ab.
import datetime as dt
from typing import overload

class CroniterError(ValueError): ...
class CroniterBadTypeRangeError(TypeError): ...
class CroniterBadCronError(CroniterError): ...
class CroniterUnsupportedSyntaxError(CroniterBadCronError): ...
class CroniterBadDateError(CroniterError): ...
class CroniterNotAlphaError(CroniterBadCronError): ...

class croniter:  # noqa: N801 - Name der Bibliothek
    tzinfo: dt.tzinfo | None
    def __init__(
        self,
        expr_format: str,
        start_time: dt.datetime | float | None = None,
        ret_type: type = ...,
        day_or: bool = True,
        max_years_between_matches: int | None = None,
        is_prev: bool = False,
        hash_id: bytes | str | None = None,
        implement_cron_bug: bool = False,
        second_at_beginning: bool = False,
        expand_from_start_time: bool = False,
    ) -> None: ...
    @overload
    def get_next(
        self, ret_type: type[dt.datetime], start_time: dt.datetime | float | None = None, update_current: bool = True
    ) -> dt.datetime: ...
    @overload
    def get_next(
        self,
        ret_type: type[float] | None = None,
        start_time: dt.datetime | float | None = None,
        update_current: bool = True,
    ) -> float: ...
    @overload
    def get_prev(
        self, ret_type: type[dt.datetime], start_time: dt.datetime | float | None = None, update_current: bool = True
    ) -> dt.datetime: ...
    @overload
    def get_prev(
        self,
        ret_type: type[float] | None = None,
        start_time: dt.datetime | float | None = None,
        update_current: bool = True,
    ) -> float: ...
    @classmethod
    def is_valid(
        cls,
        expression: str,
        hash_id: bytes | str | None = None,
        encoding: str = "UTF-8",
        second_at_beginning: bool = False,
        strict: bool = False,
        strict_year: bool | None = None,
    ) -> bool: ...
