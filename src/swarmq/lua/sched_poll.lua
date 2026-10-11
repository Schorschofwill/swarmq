-- sched_poll.lua
--
-- Scheduler poll. Returns due schedules with their score and JSON-encoded
-- definition so the Python loop can decide what to fire.
--
-- No ZADD and no commit here — Publish-before-ZREM ordering (plan CD10)
-- requires the commit phase to be a separate atomic step. See
-- sched_commit.lua.
--
-- Index hygiene (BZ-28): the poll reads the due range page by page and
-- removes entries that can never fire from the fire index, so they cannot
-- fill the LIMIT and block due crons behind them:
--   * orphans: zset member without a def in the hash (e.g. HDEL raced
--     with ZADD);
--   * disabled defs (`"disabled": true`) that still carry a score, e.g.
--     written before 0.10, which planned disabled entries. Since 0.10 a
--     disabled def has no fire-index entry, so dropping it changes nothing.
--     A def cjson cannot decode is returned; Python parks it.
-- The hash is never touched. One call drops at most MAX_DROPS entries so a
-- huge backlog cannot block Valkey; the rest goes on the next poll.
--
-- KEYS[1] sq:{sched}:zset
-- KEYS[2] sq:{sched}:defs
-- ARGV[1] now_ms   (int)
-- ARGV[2] limit    (int)
--
-- Returns: array of [member, score, def_json] tuples (at most `limit`).

local function is_disabled(def)
    local ok, decoded = pcall(cjson.decode, def)
    return ok and type(decoded) == 'table' and decoded['disabled'] == true
end

local MAX_DROPS = 1000
local limit = tonumber(ARGV[2])
local result = {}
local offset = 0
local drops = 0

while #result < limit and drops < MAX_DROPS do
    local page = redis.call(
        'ZRANGEBYSCORE', KEYS[1],
        '-inf', ARGV[1],
        'LIMIT', offset, limit
    )
    if #page == 0 then
        break
    end
    for _, member in ipairs(page) do
        local def = redis.call('HGET', KEYS[2], member)
        if def and not is_disabled(def) then
            if #result < limit then
                local score = redis.call('ZSCORE', KEYS[1], member)
                table.insert(result, {member, score, def})
            end
            offset = offset + 1
        elseif drops < MAX_DROPS then
            redis.call('ZREM', KEYS[1], member)
            drops = drops + 1
        else
            break
        end
    end
    if #page < limit then
        break
    end
end
return result
