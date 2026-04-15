"""SwarmQ configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from swarmq.exceptions import ConfigurationError


@dataclass
class SwarmQConfig:
    broker_url: str = ""
    backend_url: str = ""
    result_ttl: int = 86400
    queues: list[str] = field(default_factory=lambda: ["default"])
    concurrency: int = 10
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        env_broker = os.environ.get("SWARMQ_BROKER_URL", "")
        if env_broker:
            self.broker_url = env_broker

        env_backend = os.environ.get("SWARMQ_BACKEND_URL", "")
        if env_backend:
            self.backend_url = env_backend

        env_ttl = os.environ.get("SWARMQ_RESULT_TTL", "")
        if env_ttl:
            self.result_ttl = int(env_ttl)

        env_queues = os.environ.get("SWARMQ_QUEUES", "")
        if env_queues:
            self.queues = [q.strip() for q in env_queues.split(",") if q.strip()]

        env_concurrency = os.environ.get("SWARMQ_CONCURRENCY", "")
        if env_concurrency:
            self.concurrency = int(env_concurrency)

        env_log_level = os.environ.get("SWARMQ_LOG_LEVEL", "")
        if env_log_level:
            self.log_level = env_log_level

        # Validation
        if not self.broker_url:
            msg = "broker_url must not be empty"
            raise ConfigurationError(msg)

        if not self.backend_url:
            msg = "backend_url must not be empty"
            raise ConfigurationError(msg)

        if self.result_ttl < 0:
            msg = f"result_ttl must be >= 0, got {self.result_ttl}"
            raise ConfigurationError(msg)

        if self.concurrency < 1:
            msg = f"concurrency must be >= 1, got {self.concurrency}"
            raise ConfigurationError(msg)

        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level not in valid_levels:
            msg = f"log_level must be one of {sorted(valid_levels)}, got {self.log_level!r}"
            raise ConfigurationError(msg)
