"""CLI integration tests for R4.6 / R4.7 (--processes, --autoscale)."""

from __future__ import annotations

import argparse
import os
from unittest.mock import patch

import pytest

from swarmq.cli import _autoscale_range, _build_parser, _watch_dir_path, main


class TestAutoscaleRangeParser:
    def test_valid_range(self):
        assert _autoscale_range("2,5") == (2, 5)
        assert _autoscale_range(" 1 , 10 ") == (1, 10)

    def test_rejects_single_value(self):
        with pytest.raises(argparse.ArgumentTypeError, match="MIN,MAX"):
            _autoscale_range("5")

    def test_rejects_non_integer(self):
        with pytest.raises(argparse.ArgumentTypeError, match="integers"):
            _autoscale_range("a,b")

    def test_rejects_min_below_one(self):
        with pytest.raises(
            argparse.ArgumentTypeError, match="1 <= MIN <= MAX",
        ):
            _autoscale_range("0,3")

    def test_rejects_max_below_min(self):
        with pytest.raises(
            argparse.ArgumentTypeError, match="1 <= MIN <= MAX",
        ):
            _autoscale_range("5,2")


class TestWorkerParserFlags:
    def test_processes_default_is_one(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x"])
        assert args.processes == 1

    def test_processes_accepts_value(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x", "--processes", "4"])
        assert args.processes == 4

    def test_autoscale_parses_to_tuple(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["worker", "--module=x", "--autoscale", "2,10"],
        )
        assert args.autoscale == (2, 10)

    def test_autoscale_default_is_none(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x"])
        assert args.autoscale is None


class TestWatchDirPathType:
    """R5.3 Unit 1: --watch-dir validator."""

    def test_existing_directory_accepted(self, tmp_path):
        result = _watch_dir_path(str(tmp_path))
        # Resolved to absolute path.
        assert result == str(tmp_path.resolve())

    def test_nonexistent_path_rejected(self, tmp_path):
        missing = tmp_path / "does-not-exist"
        with pytest.raises(argparse.ArgumentTypeError, match="not an existing directory"):
            _watch_dir_path(str(missing))

    def test_file_rejected(self, tmp_path):
        f = tmp_path / "not_a_dir.txt"
        f.write_text("x")
        with pytest.raises(argparse.ArgumentTypeError, match="not an existing directory"):
            _watch_dir_path(str(f))


class TestReloadFlag:
    """R5.3 Unit 1: --reload + --watch-dir argparse layer."""

    def test_reload_default_is_false(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x"])
        assert args.reload is False

    def test_watch_dir_default_is_none(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x"])
        assert args.watch_dir is None

    def test_reload_flag_sets_true(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x", "--reload"])
        assert args.reload is True

    def test_watch_dir_repeatable(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        parser = _build_parser()
        args = parser.parse_args([
            "worker", "--module=x",
            "--reload",
            "--watch-dir", str(d1),
            "--watch-dir", str(d2),
        ])
        assert args.watch_dir == [str(d1.resolve()), str(d2.resolve())]

    def test_watch_dir_invalid_path_exits(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "worker", "--module=x",
                "--watch-dir", "/definitely/not/existing/path/xyz",
            ])


class TestMainRouting:
    """Verify that --processes>1 or --autoscale routes to the supervisor."""

    def _env(self):
        return {
            "SWARMQ_BROKER_URL": "amqp://x",
            "SWARMQ_BACKEND_URL": "valkey://x",
        }

    def test_default_routes_to_supervisor(self):
        """R20 (Plan 2026-09-03-001): auch --processes 1 laeuft unter dem Supervisor."""
        with patch.dict(os.environ, self._env(), clear=False), \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0) as rmp:
            rc = main(["worker", "--module=ignore.me"])
            rmp.assert_called_once()
            ns = rmp.call_args[0][0]
            assert ns.processes == 1
            assert ns.concurrency == 10  # aufgeloest, nicht None
            assert rc == 0

    def test_processes_gt_one_routes_to_supervisor(self):
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 return_value=0,
             ) as rmp:
            rc = main(["worker", "--module=ignore.me", "--processes", "3"])
            rmp.assert_called_once()
            ns = rmp.call_args.args[0]
            assert ns.processes == 3
            assert rc == 0

    def test_check_reload_validates_without_supervisor(self, capsys):
        """R5.3 ce-review #23: --check --reload validates watchfiles +
        watch-dirs and exits 0, without starting the supervisor."""
        import json

        with patch(
            "swarmq.process_manager.run.run_multi_process_worker",
        ) as rmp:
            rc = main([
                "worker", "--module=json",
                "--reload", "--check",
            ])
        assert rc == 0
        rmp.assert_not_called()  # supervisor NOT started
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["ok"] is True
        assert payload["reload"] is True
        assert len(payload["watch_dirs"]) == 1

    def test_check_reports_missing_module(self, capsys):
        """--check returns 1 with JSON error when --module unresolvable."""
        import json

        rc = main([
            "worker", "--module=nope.xyz.unresolvable",
            "--check",
        ])
        assert rc == 1
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["ok"] is False
        assert "nope.xyz.unresolvable" in payload["error"]

    def test_check_reports_missing_watchfiles(self, capsys):
        """R5.3 ce-review pass 2 TG-05: --check --reload without the
        [reload] extra emits JSON with error + exit 1, not a raw
        traceback."""
        import json

        with patch.dict("sys.modules", {"watchfiles": None}):
            rc = main(["worker", "--module=json", "--reload", "--check"])
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["error_type"] == "ConfigurationError"
        assert "swarmq[reload]" in payload["error"]
        # Stable field-set even on error path.
        assert payload["watch_dirs"] is None
        assert payload["module_origin"] is None

    def test_check_reports_unexpected_exception(self, capsys):
        """R5.3 ce-review pass 2 #3 / P1: non-ConfigurationError
        exceptions must render as JSON on stdout, not escape as a
        Python traceback."""
        import json

        with patch(
            "swarmq.process_manager.check._validate_module_importable",
            side_effect=PermissionError("simulated .pth lockout"),
        ):
            rc = main(["worker", "--module=json", "--check"])
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["error_type"] == "PermissionError"
        assert "simulated .pth lockout" in payload["error"]

    def test_check_argparse_error_emits_json(self, capsys):
        """R5.3 ce-review pass 2 #2 / AC-06 / CLI-02: when --check is
        requested but argparse itself rejects the invocation, the JSON
        contract must still hold — agents cannot branch on a missing
        stdout payload."""
        import json

        rc = main([
            "worker", "--module=json",
            "--watch-dir", "/definitely/not/existing/path/xyz",
            "--check",
        ])
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["error_type"] == "ArgparseError"
        assert "watch-dir" in payload["error"].lower() or \
               "not an existing directory" in payload["error"]

    def test_check_warns_on_reload_burst_combo(self, capsys):
        """R5.3 ce-review pass 2 #25: --reload + --burst emits a
        structured warning in the JSON payload."""
        import json

        rc = main([
            "worker", "--module=json", "--reload", "--burst", "--check",
        ])
        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is True
        assert any(
            "reload_burst_conflict" in w for w in payload["warnings"]
        )

    def test_check_rejects_proc_watch_dir(self, capsys):
        """R5.3 ce-review pass 2 #22 / ADV-01: pseudo-filesystem paths
        are rejected at argparse time, surfacing through the --check
        JSON error contract."""
        import json

        rc = main([
            "worker", "--module=json", "--reload",
            "--watch-dir", "/proc",
            "--check",
        ])
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["error_type"] == "ArgparseError"
        assert "/proc" in payload["error"]

    def test_check_without_reload_validates_module_only(self, capsys):
        """--check alone (no --reload) still validates --module.

        R5.3 ce-review P2-P3 pass #1: payload has stable field-set —
        every key is always present; ``watch_dirs`` is ``None`` when
        ``--reload`` is not set.
        """
        import json

        rc = main(["worker", "--module=json", "--check"])
        assert rc == 0
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["ok"] is True
        assert payload["reload"] is False
        # Stable field-set: watch_dirs is always present, None when unused.
        assert "watch_dirs" in payload
        assert payload["watch_dirs"] is None
        assert payload["error"] is None
        assert payload["warnings"] == []

    def test_reload_alone_routes_to_supervisor(self):
        """R5.3 ce-review #1: --reload (without --processes or --autoscale)
        must activate the multi-process supervisor — otherwise the flag is
        a silent no-op and R-1 is not met."""
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 return_value=0,
             ) as rmp:
            rc = main(["worker", "--module=ignore.me", "--reload"])
            rmp.assert_called_once()
            ns = rmp.call_args.args[0]
            assert ns.reload is True
            assert ns.processes == 1  # default stays 1; supervisor wraps it
            assert rc == 0

    def test_autoscale_routes_to_supervisor(self):
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 return_value=0,
             ) as rmp:
            main(["worker", "--module=ignore.me", "--autoscale", "2,5"])
            rmp.assert_called_once()
            ns = rmp.call_args.args[0]
            assert ns.autoscale == (2, 5)

    def test_supervisor_surfaces_config_error(self):
        from swarmq.exceptions import ConfigurationError

        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 side_effect=ConfigurationError("boom"),
             ):
            rc = main(["worker", "--module=x", "--processes", "2"])
            assert rc == 1




