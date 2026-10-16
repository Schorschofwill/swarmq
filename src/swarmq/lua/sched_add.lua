-- sched_add.lua
--
-- Atomic insert-if-absent of a schedule (F3-01, plan 2026-10-15-1138 KTD4):
-- HSETNX on the definition and, on success, ZADD into the fire index in
-- one EVAL. valkey-py resends a command after a lost reply; with two
-- separate commands the retry saw HSETNX = 0 and skipped the ZADD, so the
-- entry never fired. Now a stored definition always has its index entry,
-- and a retry returns 0 without touching definition or score. Telling
-- "own insert" from "conflict" stays in Python (E-058).
--
-- KEYS[1] sq:{sched}:defs
-- KEYS[2] sq:{sched}:zset
-- ARGV[1] schedule_name
-- ARGV[2] def_json
-- ARGV[3] fire_ts_ms ("" -> no fire-index entry, e.g. disabled)
--
-- Returns:
--   1 -> inserted (and indexed when ARGV[3] is set)
--   0 -> an entry with that name already existed (no-op)

if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 0 then
    return 0
end
if ARGV[3] ~= '' then
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
end
return 1
