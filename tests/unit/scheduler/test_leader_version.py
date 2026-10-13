"""Leader-Version (Plan 2026-10-08-1541, U13, KTD11): Versionsvergleich und
die Beobachtung des Leader-Locks durch Nicht-Leader."""

# Test list (U13, KTD11):
# 1. ``is_outdated``: Leader ohne Versionsangabe (0.9.0) ist veraltet.
# 2. Gleiche Version ist nicht veraltet.
# 3. Version unter der Mindestversion ist veraltet, numerisch verglichen
#    (0.9.0 < 0.10.0).
# 4. Neuere Version ist nicht veraltet.
# 4b. KTD11 (Plan 2026-10-09-2314, MR 5): die Mindestversion ist 0.12; ein
#     0.11-Leader wird gemeldet (einmal pro Wechsel), ein 0.12.0.dev0 nicht.
# 5. Nicht lesbare Leader-Version (``unknown``) gilt nicht als veraltet;
#    Suffixe wie ``.dev1``/``rc1``
#    zählen nach dem Release-Teil.
# 5b. ``0.12`` und ``0.12.0`` sind gleich.
# 6. ``LeaderVersionWatch``: ein veralteter Leader wird erst gemeldet, wenn
#    er bei zwei aufeinanderfolgenden Beobachtungen derselbe ist (zwischen
#    Lock-Erwerb und Versions-Key eines 0.10.0-Leaders liegt ein Roundtrip).
# 7. ... und danach nicht noch einmal (einmal pro Wechsel).
# 7b. Lock mit gleicher Version: keine Meldung.
# 8. Ein neuer veralteter Leader (anderes Owner-Token) wird wieder gemeldet.
# 9. Ein aktueller Leader nach einem gemeldeten veralteten liefert „clear“.
# 10. Kein Leader: keine Meldung, Bestätigung beginnt von vorn.
# 11. ``swarmq_version`` liefert die Version aus den Paket-Metadaten.

from __future__ import annotations

import importlib.metadata

import pytest

from swarmq.scheduler.leader_version import LeaderVersionWatch, is_outdated, swarmq_version


class TestIsOutdated:
    def test_missing_leader_version_is_outdated(self) -> None:
        assert is_outdated(None) is True

    @pytest.mark.entscheidung("E-029")
    def test_floor_version_is_current(self) -> None:
        assert is_outdated("0.12.0") is False

    def test_version_below_floor_is_outdated_numerically(self) -> None:
        assert is_outdated("0.9.0") is True

    def test_newer_version_is_current(self) -> None:
        assert is_outdated("0.13.0") is False

    def test_unreadable_leader_version_is_not_outdated(self) -> None:
        assert is_outdated("unknown") is False

    def test_suffixes_compare_by_release(self) -> None:
        assert is_outdated("0.12.0rc1") is False
        assert is_outdated("0.12.0.dev0") is False
        assert is_outdated("0.9.9rc1") is True

    def test_trailing_zero_components_compare_equal(self) -> None:
        assert is_outdated("0.12") is False


class TestLeaderVersionWatch:
    def test_outdated_leader_is_reported_on_second_observation(self) -> None:
        watch = LeaderVersionWatch()
        assert watch.observe("old:1:aa", None) is None
        assert watch.observe("old:1:aa", None) == "outdated"

    def test_outdated_leader_is_reported_once(self) -> None:
        watch = LeaderVersionWatch()
        observed = [watch.observe("old:1:aa", None) for _ in range(4)]
        assert observed == [None, "outdated", None, None]

    def test_current_leader_is_never_reported(self) -> None:
        watch = LeaderVersionWatch()
        observed = [watch.observe("new:1:aa", "0.12.0") for _ in range(3)]
        assert observed == [None, None, None]

    @pytest.mark.entscheidung("E-029")
    def test_leader_below_minimum_version_is_reported_once(self) -> None:
        """KTD11 (Plan 2026-10-09-2314, MR 5): ein 0.11-Leader kennt die
        Korrekturen ab 0.12 nicht und wird gemeldet, einmal pro Wechsel."""
        watch = LeaderVersionWatch()
        observed = [watch.observe("old:1:aa", "0.11.0") for _ in range(3)]
        assert observed == [None, "outdated", None]

    def test_new_outdated_leader_is_reported_again(self) -> None:
        watch = LeaderVersionWatch()
        first = [watch.observe("old:1:aa", None) for _ in range(2)]
        second = [watch.observe("old:2:bb", "0.9.0") for _ in range(3)]
        assert (first, second) == ([None, "outdated"], [None, "outdated", None])

    def test_current_leader_after_reported_outdated_one_clears(self) -> None:
        watch = LeaderVersionWatch()
        [watch.observe("old:1:aa", None) for _ in range(2)]
        observed = [watch.observe("new:1:bb", "0.12.0") for _ in range(2)]
        assert observed == ["current", None]

    def test_no_leader_restarts_the_confirmation(self) -> None:
        watch = LeaderVersionWatch()
        observed = [
            watch.observe("old:1:aa", None),
            watch.observe(None, None),
            watch.observe("old:1:aa", None),
            watch.observe("old:1:aa", None),
        ]
        assert observed == [None, None, None, "outdated"]


class TestSwarmqVersion:
    def test_version_comes_from_package_metadata(self) -> None:
        assert swarmq_version() == importlib.metadata.version("swarmq")

    def test_missing_metadata_yields_unknown(self, monkeypatch) -> None:
        def _missing(_name: str) -> str:
            raise importlib.metadata.PackageNotFoundError("swarmq")

        monkeypatch.setattr(importlib.metadata, "version", _missing)
        assert swarmq_version() == "unknown"
