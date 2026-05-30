"""SwarmQ exception hierarchy."""


class SwarmQError(Exception):
    pass


class ConfigurationError(SwarmQError):
    pass


class BrokerConnectionError(SwarmQError):
    pass


class BackendConnectionError(SwarmQError):
    pass


class TaskNotFoundError(SwarmQError):
    pass


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
    FIRE_AND_FORGET task as a step (R8, R9)."""


class WorkflowNotFound(SwarmQError):
    """Raised by ``client.workflow(workflow_id)`` when no workflow state
    exists for the id (never started, or expired) — R14."""


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
