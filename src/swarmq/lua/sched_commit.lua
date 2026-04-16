-- sched_commit.lua
--
-- Atomically commit a scheduler fire-event: ZREM current member, ZADD
-- next-fire (or skip for one-shots), HSET updated def (or HDEL for
-- expired one-shots).
--
-- Owner-check guard (plan Review-Finding P1-2): a leader whose lock
-- expired mid-batch (GC pause, network blip) must NOT be able to
-- retroactively overwrite the new leader's state. The script reads the
-- lock key first and returns 0 without mutation when the caller's token
-- does not match the current holder. This makes split-brain commits
-- impossible at the storage boundary.
--
-- KEYS[1] sq:sched:zset
-- KEYS[2] sq:sched:defs
-- KEYS[3] sq:lock:scheduler:leader  (the leader lock key)
-- ARGV[1] schedule_name         (member to commit)
-- ARGV[2] next_fire_ts_ms       (int; -1 means one-shot, skip ZADD)
-- ARGV[3] updated_def_json      (bytes; "" means delete def via HDEL)
-- ARGV[4] owner_token           (caller's identity — must match KEYS[3])
--
-- Returns:
--   1 -> commit applied (zset/defs mutated according to ARGV)
--   0 -> owner mismatch; no state touched

local owner = redis.call('GET', KEYS[3])
if owner ~= ARGV[4] then
    return 0
end

redis.call('ZREM', KEYS[1], ARGV[1])
local next_ts = tonumber(ARGV[2])
if next_ts >= 0 then
    redis.call('ZADD', KEYS[1], next_ts, ARGV[1])
end

if ARGV[3] == '' then
    redis.call('HDEL', KEYS[2], ARGV[1])
else
    redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
end

return 1
