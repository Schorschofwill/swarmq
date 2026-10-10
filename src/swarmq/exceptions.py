"""SwarmQ exception hierarchy."""


class SwarmQError(Exception):
    pass


class ConfigurationError(SwarmQError):
    pass


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
        hint = hint or (
            "Restore the previous setting, or migrate the queue: stop its workers, "
            "drain and delete the queue family, then start the workers again."
        )
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
    """Ein Publish hat seine Frist gerissen; ob er ankam, ist unbekannt.

    Die Frist laeuft waehrend des Wartens auf das Publisher-Confirm ab — der
    Broker kann die Nachricht also bereits haben. Das unterscheidet diesen
    Fall von jedem anderen Publish-Fehler, bei dem nichts gesendet wurde.

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
    unterwegs. Der Ausfuehrungsschutz (Plan 2026-10-08-1541, KTD5) setzt
    deshalb seinen Superseded- bzw. Done-Marker wie nach Erfolg, damit die
    Neuzustellung des Originals uebersprungen wird. Unterklasse von
    :class:`BrokerConnectionError`: bestehende Handler fangen sie weiter.
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


class TaskNotFoundError(SwarmQError):
    pass


class RedeliveryLimitError(SwarmQError):
    """Fehlertyp einer DLQ-Kopie, deren Zustellungen immer wieder per
    ``redeliver`` (``nack(requeue=True)``) zurueckgegeben wurden, bis
    ``x-acquired-count`` (RabbitMQ 4.3) das Zustelllimit erreichte (BZ-29).
    Wird nicht geworfen; der Name steht als ``error_type`` in
    ``task_sent_to_dlq`` und am Anfang von ``x-swarmq-traceback``.
    """


class TaskError(SwarmQError):
    pass


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
    pass


class ResultSerializationError(SwarmQError):
    pass


class Retry(SwarmQError):
    def __init__(self, delay: float | None = None) -> None:
        self.delay = delay
        super().__init__()


class SoftTimeoutError(SwarmQError):
    pass


class NoRetry(SwarmQError):
    def __init__(self, reason: str = "") -> None:
        self.reason = reason
        super().__init__(reason)
