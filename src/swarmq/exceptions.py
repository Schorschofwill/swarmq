"""SwarmQ exception hierarchy."""

import math


class SwarmQError(Exception):
    pass


class ConfigurationError(SwarmQError):
    pass


#: Migration einer Queue-Familie (Review MR 8, P2-3): ``{queue}.DQ`` und die
#: Delay-Stufen ``{queue}.DQ.*`` (BZ-01, Review MR 10, P2-5) enthalten Retries
#: und Termine, sie werden nie einfach geloescht.
MIGRATION_HINT = (
    "migrate the queue: stop its producers, let its workers drain the queue, its .DQ and its .DQ.* tiers "
    "(the .DQ and its .DQ.* tiers hold pending retries and deferred tasks: wait for them or move them "
    "with a shovel, never just delete them), stop the workers, move or keep the .XQ, then delete the "
    "queue family and start the workers with the new setting."
)


class QueueArgumentsMismatch(ConfigurationError):
    """Eine vorhandene Queue hat andere Argumente als die Konfiguration, und
    swarmq kann sie nicht uebernehmen (BZ-15), etwa ein geaenderter
    ``SWARMQ_QUEUE_TYPE``. RabbitMQ aendert die Argumente einer bestehenden
    Queue nie; ein Neustart aendert daran nichts, darum Exit 78 statt einer
    Neustartschleife. ``x-delivery-limit``, ``x-dead-letter-strategy`` und
    ``x-overflow`` werden dagegen uebernommen.
    """

    def __init__(self, queue: str, argument: str, declared: object, existing: object, hint: str = "") -> None:
        self.queue = queue
        self.argument = argument
        self.declared = declared
        self.existing = existing
        hint = hint or f"Restore the previous setting, or {MIGRATION_HINT}"
        message = (
            f"queue {queue!r}: argument {argument!r} differs from the existing queue "
            f"(declared {declared!r}, existing {existing!r}); RabbitMQ never changes the arguments "
            f"of an existing queue. {hint}"
        )
        super().__init__(message)


class LoggingConfigError(ConfigurationError):
    """Eine Logging-Konfigurationsdatei ist nicht ladbar oder ungueltig.

    Plan 2026-10-06-0831, U6 (R14, AE4). Traegt den Pfad der Datei; die
    Ursache steht in ``__cause__``. Als ``ConfigurationError`` endet sie in
    den Einstiegspunkten mit Exit 78.
    """

    def __init__(self, path: str, cause: BaseException) -> None:
        super().__init__(f"invalid logging config {path}: {type(cause).__name__}: {cause}")
        self.path = path
        self.__cause__ = cause


class BrokerConnectionError(SwarmQError):
    pass


class PublishIndeterminate(BrokerConnectionError):
    """Ein Publish ist gesendet, aber unbestaetigt gescheitert; ob er ankam, ist unbekannt.

    Zwei Wege: Die Frist laeuft waehrend des Wartens auf das Publisher-Confirm
    ab, oder die Verbindung reisst in dieser Phase (B24). Der Broker kann die
    Nachricht also bereits haben. Das unterscheidet diesen Fall von jedem
    anderen Publish-Fehler, bei dem nichts gesendet wurde.

    Absichtlich eine Unterklasse von :class:`BrokerConnectionError`: Alle
    bestehenden ``except BrokerConnectionError``-Stellen — insbesondere die
    eng gefassten Handler im Ablauf-Gate — fangen sie unveraendert weiter.
    Wer die Unterscheidung braucht, fragt gezielt danach; der Client tut das,
    um die Dedup-Sperre eines ``unique_for``-Tasks NICHT freizugeben
    (Plan 2026-08-28-001, R11).
    """


class SettleFailedAfterPublish(BrokerConnectionError):
    """``retry_later``/``dead_letter``: die Kopie ist veroeffentlicht
    (Publisher-Confirm), nur das Bestaetigen des Originals scheiterte.

    Der Broker stellt das Original erneut zu, die Kopie ist trotzdem
    unterwegs; der Worker wertet das als erfolgten Retry, Dead-Letter bzw.
    erfolgte Rueckstellung (E-043). Der Ausfuehrungsschutz (Plan
    2026-10-08-1541, KTD5) setzt nur bei der Retry-Kopie (hoeherer
    Retry-Zaehler) den Superseded- und bei der DLQ-Kopie den Done-Marker wie
    nach Erfolg, damit die Neuzustellung des Originals uebersprungen wird.
    Bei einer Rueckstellungs-Kopie (Rate-Limit, Lock, Ausfuehrungsschutz,
    Backend-Ausfall, unbekannter Task, Schedfire; gleicher Retry-Zaehler)
    setzt er keinen Marker: Kopie und neu zugestelltes Original laufen beide
    durch die Gates, der Ausfuehrungsschutz serialisiert sie. Nicht bei
    ``FIRE_AND_FORGET``, ohne ``ExecutionGuardStore`` und mit
    ``SWARMQ_EXECUTION_GUARD=false``: dann koennen Kopie und Original beide
    laufen. Unterklasse von :class:`BrokerConnectionError`: bestehende
    Handler fangen sie weiter.
    """


class MessageHeadersTooLarge(BrokerConnectionError):
    """Die Header einer Nachricht passen auch nach dem Kuerzen langer Texte
    nicht in die Haelfte von ``frame_max``; der Publish wurde nicht
    versucht (BZ-17). Ein zu grosser Content-Header schloesse sonst die
    ganze Verbindung, und alle unbestaetigten Nachbar-Zustellungen kaemen
    zurueck. Unterklasse von :class:`BrokerConnectionError` (Q14).
    """


class UnroutableMessage(BrokerConnectionError):
    """Der Broker hat die Nachricht per ``basic.return`` zurueckgewiesen:
    keine Queue mit diesem Namen (BZ-03). Nichts wurde zugestellt.

    Typisch ist ein Producer, der vor dem ersten Worker einer neuen Queue
    sendet, oder ein Prioritaets-Publish an ``{queue}.high``/``.low`` ohne
    ``--priorities``. Vor 0.12 bestaetigte der Broker solche Nachrichten
    und verwarf sie still. Unterklasse von :class:`BrokerConnectionError`
    (Q14): bestehende Handler fangen sie weiter.
    """


class BrokerPublishBlocked(BrokerConnectionError):
    """Die Publish-Verbindung war bis zur Publish-Frist vom Broker blockiert
    (``connection.blocked``, Ressourcenalarm: Speicher oder Platte; BZ-16).
    Nichts wurde gesendet; ein spaeterer Versuch ist sicher. Ein Alarm
    trifft jeden Prozess am Broker gleich, deshalb zaehlt er nicht fuer den
    Ruecktritt des Leaders. Unterklasse von :class:`BrokerConnectionError`
    (Q14): bestehende Handler fangen sie weiter.
    """


class PublishRejected(BrokerConnectionError):
    """Der Broker hat den Publish fuer diese Queue ueber alle Versuche
    abgelehnt: negatives Publisher-Confirm (``basic.nack``), etwa eine volle
    Queue mit ``x-overflow=reject-publish`` oder eine Quorum-Queue ohne
    Quorum (Review MR 15, P2-2). Die Verbindung ist gesund; der Fehler haengt
    an der Queue, deshalb zaehlt er nicht fuer den Ruecktritt des Leaders.
    Unterklasse von :class:`BrokerConnectionError` (Q14): bestehende Handler
    fangen sie weiter.
    """


class BackendConnectionError(SwarmQError):
    pass


class BackendResponseError(BackendConnectionError):
    """Das Backend war erreichbar, hat die Operation aber mit einem Fehler
    beantwortet (z. B. ``WRONGTYPE``, Skriptfehler).

    Fehlerantworten, die durch Warten heilen (OOM, READONLY, LOADING,
    TRYAGAIN, CLUSTERDOWN, MASTERDOWN, MISCONF), sind KEIN
    ``BackendResponseError``, sondern ein einfacher
    ``BackendConnectionError`` (``as_backend_error``).

    Kein Ausfall, der sich von selbst erholt: die Gates vor der Ausfuehrung
    stellen dafuer nicht mit Backoff zurueck, sondern nehmen den alten Weg
    (Requeue an derselben Stelle, das Zustelllimit fuehrt eine dauerhaft
    scheiternde Nachricht ins Dead-Letter-Ziel). Absichtlich eine
    Unterklasse von :class:`BackendConnectionError` (wie
    :class:`PublishIndeterminate`): bestehende ``except
    BackendConnectionError``-Stellen fangen sie unveraendert weiter.
    """


def is_backend_outage(exc: BaseException) -> bool:
    """Ausfall des Backends (Verbindung, Timeout) -- nicht eine
    Fehlerantwort (``BackendResponseError``) und kein anderer Fehler."""
    return isinstance(exc, BackendConnectionError) and not isinstance(exc, BackendResponseError)


class TaskNotFoundError(SwarmQError):
    pass


class RedeliveryLimitError(SwarmQError):
    """Fehlertyp einer DLQ-Kopie, deren Zustellungen immer wieder per
    ``redeliver`` (``nack(requeue=True)``) zurueckgegeben wurden, bis
    ``x-acquired-count`` (RabbitMQ 4.3) das Zustelllimit erreichte (BZ-29).
    Wird nicht geworfen; der Name steht als ``error_type`` in
    ``task_sent_to_dlq`` und am Anfang von ``x-swarmq-traceback``.
    """


class LockWaitExceeded(SwarmQError):
    """Fehlertyp einer DLQ-Kopie, deren Wartezeit auf einen Lock endete
    (Plan 0.12.0, MR 13, BZ-05): entweder hielt derselbe Halter laenger als
    seine TTL + 60 s (Stillstand), oder die Obergrenze
    ``SWARMQ_LOCK_WAIT_MAX_S`` ab dem ersten Konflikt war erreicht. Wird
    nicht geworfen; der Name steht als ``error_type`` in ``task_sent_to_dlq``
    und am Anfang von ``x-swarmq-traceback``.
    """


class TaskError(SwarmQError):
    """Ein Task endete mit einem Fehlerergebnis (bzw. ein Workflow scheiterte).

    ``error_class`` (P9, beratend) nennt die voll qualifizierte Klasse der
    ausloesenden Exception (``modul.qualname``, z. B.
    ``builtins.ValueError``), im CPU-Modus den Namen aus dem Rechenkind.
    ``None``, wenn das Ergebnis keine Klasse traegt: Ergebnisse aelterer
    Worker und vom Worker selbst erzeugte Fehler (harter Timeout, Drain,
    verlorenes Rechenkind, Ablauf).
    """

    def __init__(self, *args: object, error_class: str | None = None) -> None:
        super().__init__(*args)
        self.error_class = error_class


class TaskCancelledError(TaskError):
    def __init__(self, task_id: str | None = None) -> None:
        self.task_id = task_id
        msg = f"task {task_id} was cancelled" if task_id else "task was cancelled"
        super().__init__(msg)


class ResultNotReadyError(SwarmQError):
    pass


class WorkflowValidationError(SwarmQError):
    """Raised at workflow construction when a topology violates the
    flat-DAG depth limit, the group-size limit, or embeds a
    FIRE_AND_FORGET task as a step (R8, R9).

    Also raised at **run time**, during group/chord fan-in, when the member
    result list cannot be assembled completely — a member step marker is
    missing from the workflow state. Delivering a shorter list than the group
    has members would be an undetectable wrong answer, so the workflow fails
    instead. The worker seam marks the workflow failed and ``handle.get()``
    raises."""


class WorkflowNotFound(SwarmQError):
    """Raised by ``client.workflow(workflow_id)`` when no workflow state
    exists for the id (never started, or expired) — R14."""


class WorkflowStateConflict(SwarmQError):
    """Raised when a workflow-state Compare-and-Set lost every one of its
    conflict rounds (KTD11, R23) — the advance is not finished and the step
    message must be redelivered. Each round is won by at least one writer,
    so this needs more concurrently writing branches than the limit
    (``WORKFLOW_CAS_MAX_ROUNDS``) allows; the redelivery may re-publish
    deterministic step ids, which step idempotency absorbs."""


class WorkflowResultExpired(SwarmQError):
    """Raised when a member/step result needed for fan-in or final
    retrieval has expired before the workflow completed — R6. Distinct
    from 'never written' so callers never see silent ``None`` holes."""


class ResultExpiredError(SwarmQError):
    """Currently never raised; a stable hook for later telling an expired
    result apart from one that is not ready yet. After the result TTL,
    ``get_result`` raises ``ResultNotReadyError``."""


class ResultSerializationError(SwarmQError):
    pass


class Retry(SwarmQError):
    def __init__(self, delay: float | None = None) -> None:
        # E-060 (F3-03): ``inf``/``nan`` gehen nicht an den Broker; eine
        # negative Wartezeit (etwa ``reset_at - time.time()``) wiederholt
        # sofort und zaehlt normal. Hier, damit ``delay_ms``, ``on_retry``,
        # ``TASK_RETRYING`` und das Log denselben Wert sehen (KTD6).
        if delay is not None:
            if not math.isfinite(delay):
                msg = f"Retry delay must be finite, got {delay!r}"
                raise ValueError(msg)
            if delay < 0:
                delay = 0.0
        self.delay = delay
        super().__init__()


class SoftTimeoutError(SwarmQError):
    pass


class NoRetry(SwarmQError):
    def __init__(self, reason: str = "") -> None:
        self.reason = reason
        super().__init__(reason)
