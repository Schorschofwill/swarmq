"""``as_backend_error``: Einordnung eines Treiberfehlers an der Backend-Grenze.

Ein Ausfall (``BackendConnectionError``) heilt durch Warten -- die Gates
stellen dann mit Backoff zurueck. Eine Fehlerantwort, die Warten nicht heilt
(``BackendResponseError``: WRONGTYPE, Skriptfehler), nimmt den Requeue-Weg
zum Zustelllimit.

Test-Liste (viertes Review 0.9.0):
- [x] OOM (``OutOfMemoryError``, maxmemory erreicht) ist ein Ausfall
- [x] READONLY (Failover), TRYAGAIN, CLUSTERDOWN, MASTERDOWN (Klassen) sind
      Ausfaelle
- [x] LOADING (``BusyLoadingError``, Valkey laedt nach Neustart) ist ein
      Ausfall
- [x] dieselben Antworten ohne eigene Klasse (``ResponseError`` mit
      Fehlerpraefix, z. B. MASTERDOWN ausserhalb des Cluster-Parsers, MISCONF)
      sind Ausfaelle
- [x] WRONGTYPE und Skriptfehler bleiben ``BackendResponseError``
- [x] BUSY (langes Skript) und NOREPLICAS (``min-replicas-to-write``) sind
      Ausfaelle (Review 0.13.1)
"""

from __future__ import annotations

import pytest
import valkey.exceptions as vx

from swarmq.backend.valkey.connection import as_backend_error
from swarmq.exceptions import BackendConnectionError, BackendResponseError

pytestmark = pytest.mark.timeout(5)


def _is_outage(error: BaseException) -> bool:
    return not isinstance(error, BackendResponseError) and isinstance(error, BackendConnectionError)


@pytest.mark.parametrize(
    "exc",
    [
        vx.OutOfMemoryError("command not allowed when used memory > 'maxmemory'."),
        vx.ReadOnlyError("You can't write against a read only replica."),
        vx.TryAgainError("Multiple keys request during rehashing of slot"),
        vx.ClusterDownError("The cluster is down"),
        vx.MasterDownError("Link with MASTER is down and replica-serve-stale-data is set to 'no'."),
        vx.BusyLoadingError("Valkey is loading the dataset in memory"),
    ],
)
def test_self_healing_error_replies_are_outages(exc):
    assert _is_outage(as_backend_error(str(exc), exc))


@pytest.mark.parametrize(
    "reply",
    [
        "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
        "MISCONF Errors writing to the AOF file: No space left on device",
        "CLUSTERDOWN The cluster is down",
        "TRYAGAIN Multiple keys request during rehashing of slot",
        "LOADING Valkey is loading the dataset in memory",
        "READONLY You can't write against a read only replica.",
        "OOM command not allowed when used memory > 'maxmemory'.",
    ],
)
def test_self_healing_replies_without_own_class_are_outages(reply):
    """Der Standalone-Parser kennt MASTERDOWN, MISCONF, CLUSTERDOWN und
    TRYAGAIN nicht: sie kommen als einfacher ``ResponseError`` mit dem
    Fehlerpraefix im Text an."""
    exc = vx.ResponseError(reply)

    assert _is_outage(as_backend_error(str(exc), exc))


@pytest.mark.entscheidung("E-079")
@pytest.mark.parametrize(
    "reply",
    [
        "BUSY Valkey is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
        "NOREPLICAS Not enough good replicas to write.",
    ],
    ids=["busy", "noreplicas"],
)
def test_busy_script_and_missing_replicas_are_outages(reply):
    """Review 0.13.1: ein langes Skript (``BUSY``) und zu wenige Replikate
    (``NOREPLICAS``, ``min-replicas-to-write``) heilen durch Warten wie
    ``OOM``. Seit 0.13.1 markiert der FAF-Pfad eine Fehlerantwort sofort als
    ``failed``; eine falsche Einstufung schlüge dort sofort durch."""
    exc = vx.ResponseError(reply)

    assert _is_outage(as_backend_error(str(exc), exc))


@pytest.mark.parametrize(
    "exc",
    [
        vx.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        vx.ResponseError("ERR user_script:1: Script attempted to access nonexistent global variable"),
        vx.ResponseError("BUSYKEY Target key name already exists."),
        vx.ResponseError(""),
        vx.DataError("Invalid input of type: 'dict'"),
    ],
)
def test_other_error_replies_stay_response_errors(exc):
    """Warten heilt sie nicht: Requeue-Weg zum Zustelllimit."""
    assert isinstance(as_backend_error(str(exc), exc), BackendResponseError)
