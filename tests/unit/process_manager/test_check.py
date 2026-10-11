"""Check-Tests (Plan U21) — verbatim: Modul-Importbarkeit
(``--check``-JSON-Kontrakt läuft in ``test_cli_worker.py`` mit)."""

from __future__ import annotations

import pytest

from swarmq.exceptions import ConfigurationError
from swarmq.process_manager.check import check_worker_config


class TestValidateModuleImportable:
    """ce-review Unit 2: find_spec pre-flight check direct tests."""

    def test_existing_module_accepted(self):
        from swarmq.process_manager.check import _validate_module_importable

        _validate_module_importable("swarmq.cli")  # no raise

    def test_nonexistent_module_rejected(self):
        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager.check import _validate_module_importable

        with pytest.raises(ConfigurationError, match="cannot be imported|no loadable"):
            _validate_module_importable("swarmq_does_not_exist_xyz")

    def test_malformed_dotted_raises_value_error_wrapped(self):
        """find_spec raises ValueError on some malformed inputs. The
        pre-flight wraps these as ConfigurationError (not bare ValueError)."""
        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager.check import _validate_module_importable

        # Leading-dot style triggers ValueError on most Python versions.
        with pytest.raises(ConfigurationError):
            _validate_module_importable(".leading.dot")

    def test_namespace_package_without_init_rejected(self, tmp_path, monkeypatch):
        """ce-review F-05: namespace package (no __init__.py) has
        spec.origin=None and will fail in import_module — we reject it
        upfront."""
        import sys as _sys

        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager.check import _validate_module_importable

        # Create a namespace package on PYTHONPATH.
        ns_dir = tmp_path / "my_namespace_pkg"
        ns_dir.mkdir()
        # NO __init__.py → namespace package → spec.origin is None
        monkeypatch.syspath_prepend(str(tmp_path))
        # Sanity: find_spec returns a spec but origin is None.
        import importlib.util as _iu

        spec = _iu.find_spec("my_namespace_pkg")
        assert spec is not None
        assert spec.origin is None

        with pytest.raises(ConfigurationError, match="no loadable"):
            _validate_module_importable("my_namespace_pkg")
        # Cleanup sys.modules so other tests aren't polluted.
        _sys.modules.pop("my_namespace_pkg", None)


class TestConfigureHookPreflight:
    """U5 (R11/KTD5): ``--check`` prueft Syntax und Auffindbarkeit des
    Hook-Moduls -- ohne es zu importieren.

    Ein Import im Supervisor liesse Anwendungscode im Elternprozess laufen
    (und im Forkserver-Preload). Das Attribut wird deshalb erst im Worker
    aufgeloest; ein Tippfehler im Modulnamen faellt aber schon hier auf,
    statt erst in fuenf Kindern."""

    def _check(self, **kwargs):
        defaults = {"module": "swarmq", "reload": False, "watch_dir": None}
        return check_worker_config(**{**defaults, **kwargs})

    def test_resolvable_module_passes_and_is_reported(self):
        result = self._check(configure="swarmq.cli:main")
        assert result["configure"] == "swarmq.cli:main"

    def test_unresolvable_module_is_rejected(self):
        with pytest.raises(ConfigurationError, match="definitely_no_hook_module_xyz"):
            self._check(configure="definitely_no_hook_module_xyz:setup")

    def test_malformed_spec_is_rejected(self):
        with pytest.raises(ConfigurationError, match="MODULE:CALLABLE"):
            self._check(configure="swarmq.cli")

    def test_missing_attribute_is_not_checked_here(self):
        """Das Attribut loest erst der Worker auf -- ein ``getattr`` hier
        braeuchte den Import."""
        result = self._check(configure="swarmq.cli:no_such_callable")
        assert result["configure"] == "swarmq.cli:no_such_callable"

    def test_absent_hook_reports_none(self):
        assert self._check()["configure"] is None


class TestHeadroomPreflight:
    """Kontrollkanal U3 (R1, R3): ``--max-processes`` gegen Startwerte und CPU-Zuteilung.

    Test-Liste:
    - Spielraum unter --processes wird abgelehnt.
    - Spielraum unter der Autoscale-Obergrenze wird abgelehnt.
    - Spielraum ab den Startwerten wird angenommen.
    - Rechen-Pool ohne ausdrueckliche Nebenlaeufigkeit: Spielraum ueber der CPU-Zuteilung abgelehnt.
    - Rechen-Pool mit ausdruecklicher Nebenlaeufigkeit: Produkt ueber der CPU-Zuteilung abgelehnt.
    """

    def test_headroom_below_processes_is_rejected(self):
        with pytest.raises(ConfigurationError, match="--max-processes 4"):
            check_worker_config(module="json", reload=False, watch_dir=None, processes=8, max_processes=4)

    def test_headroom_below_autoscale_upper_bound_is_rejected(self):
        with pytest.raises(ConfigurationError, match="autoscale upper bound 8"):
            check_worker_config(
                module="json",
                reload=False,
                watch_dir=None,
                processes=1,
                autoscale=(2, 8),
                max_processes=4,
            )

    def test_headroom_covering_the_start_values_is_accepted(self):
        result = check_worker_config(
            module="json",
            reload=False,
            watch_dir=None,
            processes=4,
            autoscale=(2, 8),
            max_processes=8,
        )
        assert result["module"] == "json"

    @pytest.fixture
    def eight_cpus(self, monkeypatch):
        monkeypatch.setattr("swarmq.slots._affinity_count", lambda: 8)
        monkeypatch.setattr("swarmq.slots._cgroup_quota", lambda _path: None)

    def test_cpu_pool_without_explicit_concurrency_rejects_headroom_above_the_cpus(self, eight_cpus):
        with pytest.raises(ConfigurationError, match="exceeds the 8 CPUs"):
            check_worker_config(module="json", reload=False, watch_dir=None, pool="cpu", processes=2, max_processes=9)
        check_worker_config(module="json", reload=False, watch_dir=None, pool="cpu", processes=2, max_processes=8)

    def test_cpu_pool_with_explicit_concurrency_rejects_a_product_above_the_cpus(self, eight_cpus):
        with pytest.raises(ConfigurationError, match="3 x --max-processes 3 = 9"):
            check_worker_config(
                module="json",
                reload=False,
                watch_dir=None,
                pool="cpu",
                processes=1,
                concurrency=3,
                max_processes=3,
            )
        check_worker_config(
            module="json",
            reload=False,
            watch_dir=None,
            pool="cpu",
            processes=1,
            concurrency=2,
            max_processes=4,
        )

    def test_check_payload_slots_follow_the_headroom(self, eight_cpus):
        result = check_worker_config(
            module="json",
            reload=False,
            watch_dir=None,
            pool="cpu",
            processes=2,
            max_processes=8,
        )
        assert result["slots"] == 1
