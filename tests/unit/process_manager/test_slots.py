"""Slot-Tabelle des Supervisors (Plan 2026-09-08-001, U1/KTD1).

Test-Liste:
- [x] frische Tabelle: alle Slots frei
- [x] refresh nach Haltern: 0 → frei, 1 → aktiv, >= 2 → Übergang
- [x] lock: gesperrt bleibt gesperrt, auch nach refresh
- [x] smallest_free überspringt aktiv/Übergang/gesperrt; None wenn nichts frei
- [x] occupied zählt aktiv und Übergang je einmal
"""

from __future__ import annotations

from swarmq.process_manager.slots import SlotState, SlotTable


class TestSlotTable:
    def test_fresh_table_has_all_slots_free(self):
        table = SlotTable(3)
        assert len(table) == 3
        assert [table.state(s) for s in range(3)] == [SlotState.FREE] * 3

    def test_refresh_derives_state_from_holder_count(self):
        table = SlotTable(2)
        table.refresh(1, holders=1)
        assert table.state(1) is SlotState.ACTIVE
        table.refresh(1, holders=2)
        assert table.state(1) is SlotState.TRANSITION
        table.refresh(1, holders=0)
        assert table.state(1) is SlotState.FREE
        assert table.state(0) is SlotState.FREE  # unberührt

    def test_locked_slot_stays_locked_across_refresh(self):
        table = SlotTable(3)
        table.refresh(2, holders=1)
        table.lock(2)
        assert table.state(2) is SlotState.LOCKED
        table.refresh(2, holders=0)
        assert table.state(2) is SlotState.LOCKED
        assert table.locked() == 1

    def test_smallest_free_skips_held_and_locked_slots(self):
        table = SlotTable(4)
        table.refresh(0, holders=1)
        table.lock(1)
        table.refresh(2, holders=2)
        assert table.smallest_free() == 3
        table.refresh(3, holders=1)
        assert table.smallest_free() is None

    def test_occupied_counts_active_and_transition_slots_once(self):
        table = SlotTable(4)
        table.refresh(0, holders=1)
        table.refresh(1, holders=2)  # zwei Prozesse, ein Slot
        table.lock(2)
        assert table.occupied() == 2
