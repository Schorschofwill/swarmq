-- sched_poll.lua
--
-- Read-only scheduler poll. Returns due schedules with their score and
-- JSON-encoded definition so the Python loop can decide what to fire.
--
-- No ZREM/ZADD here — Publish-before-ZREM ordering (plan CD10) requires
-- the commit phase to be a separate atomic step. See sched_commit.lua.
--
-- Orphan handling: when a zset member has no matching def in the hash
-- (e.g. HDEL raced with ZADD), skip it silently — the next poll will
-- either see a fresh def or the commit-script will be called with an
-- empty def_json which cleans up the orphan zset entry.
--
-- KEYS[1] sq:{sched}:zset
-- KEYS[2] sq:{sched}:defs
-- ARGV[1] now_ms   (int)
-- ARGV[2] limit    (int)
--
-- Returns: array of [member, score, def_json] tuples.

local due = redis.call(
    'ZRANGEBYSCORE', KEYS[1],
    '-inf', ARGV[1],
    'LIMIT', 0, tonumber(ARGV[2])
)

local result = {}
for i, member in ipairs(due) do
    local score = redis.call('ZSCORE', KEYS[1], member)
    local def = redis.call('HGET', KEYS[2], member)
    if def then
        table.insert(result, {member, score, def})
    end
    -- orphan zset entry (def deleted but zset not cleaned) -> skip
end
return result
