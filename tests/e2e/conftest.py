"""E2E conftest — fixtures for real RabbitMQ + Valkey roundtrip tests."""

from __future__ import annotations

from tests.support.services import (  # noqa: F401 — Fixtures in den Namensraum (Plan U4)
    check_docker,
    rabbitmq_url,
    valkey_url,
)

# Task registry reset is handled by root conftest.py (_reset_task_registry).
