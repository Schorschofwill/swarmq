-- sched_replan.lua
--
-- Atomic healing of an enabled schedule without a fire-index score (Review
-- #4): the leader read the definition, found it unchanged and active, and
-- wants to plan it again. A `schedule remove` (HSET disabled + ZREM), a
-- permanent remove, a takeover or a concurrent re-plan landing after that
-- read must win; so the write only happens while the hash still holds
-- exactly the bytes that were read and the member has no score.
--
-- KEYS[1] sq:{sched}:defs
-- KEYS[2] sq:{sched}:zset
-- ARGV[1] schedule_name
-- ARGV[2] expected_def_json (the payload the caller read)
-- ARGV[3] healed_def_json   (written in place of ARGV[2])
-- ARGV[4] fire_ts_ms        (score to ZADD)
--
-- Returns:
--   1 -> definition written and member planned
--   0 -> definition changed/removed or member already scored; no-op

if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then
    return 0
end
if redis.call('ZSCORE', KEYS[2], ARGV[1]) then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[2], tonumber(ARGV[4]), ARGV[1])
return 1
