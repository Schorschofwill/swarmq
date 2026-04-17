-- sched_disable.lua
--
-- Atomic disable: mark the hash entry as disabled AND remove it from
-- the zset in one EVAL. The decode+encode step happens in Python (we
-- depend on msgspec's exact JSON shape for ScheduleDefinition), but
-- the write-pair HSET+ZREM no longer has a TOCTOU window for
-- concurrent remove/add operations against the same schedule_name
-- (plan Unit 8 / M-14).
--
-- KEYS[1] sq:sched:defs
-- KEYS[2] sq:sched:zset
-- ARGV[1] schedule_name
-- ARGV[2] updated_def_json (already has disabled=true set by caller)
--
-- Returns:
--   1 -> entry existed and was disabled
--   0 -> no entry existed under that name (no-op)

if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
