"""``swarmq check`` gegen den Test-Valkey (Plan 0.12.0, MR 14, BZ-04).

Nur lesend (``CONFIG GET``, ``INFO``); der Test-Valkey laeuft mit
``noeviction`` ohne Speichergrenze. Die Faelle mit verdraengender Policy
stehen als Unit-Tests (``tests/unit/test_cli_env_check.py``) und als
Chaos-Test mit eigenem Wegwerf-Container (``tests/chaos/test_valkey_eviction_policy.py``).

Test-Liste:
- [x] Test-Valkey: Exit 0, Policy ``noeviction``, Version >= 8, Werte gelesen
"""

from __future__ import annotations

import json

import pytest

from swarmq.cli import main

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


def test_check_reads_the_test_valkey(
    valkey_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SWARMQ_BACKEND_URL", valkey_url)

    code = main(["check", "--format", "json"])

    report = json.loads(capsys.readouterr().out)
    assert code == 0
    valkey_section = report["valkey"]
    assert valkey_section["maxmemory_policy"] == "noeviction"
    assert valkey_section["eviction"] == "safe"
    assert int(valkey_section["version"].split(".")[0]) >= 8
    assert isinstance(valkey_section["used_memory"], int)
    assert valkey_section["aof_enabled"] in (True, False)
    assert valkey_section["cluster_enabled"] is False
