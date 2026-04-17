-- unique_release.lua
--
-- Atomically release a SwarmQ dedup flag with owner-check.
--
-- Used by R3.5 Unique Tasks. Called at Worker terminal paths
-- (completion-mode default) and at Message-Consume (start-mode).
--
-- Owner-token check protects against the TTL-expiry-race: when
-- `unique_for` is shorter than the task runtime, the flag TTL-expires
-- while the first task is still running. A second task.SETNX with the
-- same dedup_hash then succeeds and holds the flag with a DIFFERENT
-- task_id. If the first task later triggers release via a bare DEL,
-- it would wipe the second task's flag and admit a third duplicate.
-- The GET-and-compare protects against this — we DEL only when the
-- stored value matches the caller's task_id.
--
-- KEYS[1] dedup key        ("sq:unique:{sha256_hex}")
-- ARGV[1] task_id          (owner-token — the calling task's id)
--
-- Returns:
--   1 -> released by owner (key existed, value matched, DEL succeeded)
--   0 -> nothing released (foreign owner, TTL-expired, or never set)

local dedup_key = KEYS[1]
local task_id = ARGV[1]

if redis.call('GET', dedup_key) == task_id then
    redis.call('DEL', dedup_key)
    return 1
end
return 0
