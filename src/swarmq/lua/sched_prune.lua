-- sched_prune.lua
--
-- Remove one static cron whose reports went silent (Plan 2026-10-08-1541,
-- KTD8). Server clock (TIME); the check and the removal run in one step, so
-- a report or a change that lands concurrently keeps the cron.
--
-- KEYS[1] sq:{sched}:defs                (hash; name -> definition json)
-- KEYS[2] sq:{sched}:zset                (fire index)
-- KEYS[3] sq:{sched}:report:last_seen    (hash; "<name>|<fingerprint>" -> ms)
-- KEYS[4] sq:{sched}:disabled            (set; names disabled when removed)
-- ARGV[1] name
-- ARGV[2] expected definition json (as read by the leader)
-- ARGV[3] stale_after_ms (int), or "" for a cron that was never reported
--
-- Removes the cron only when its definition is still ARGV[2] and
-- - with ARGV[3]: at least one report of the name exists, all older than
--   ARGV[3];
-- - with "": no report of the name exists (schedule prune --orphans).
-- ARGV[4] "1" remembers the name in KEYS[4] (the cron was disabled), so a
--         later report recreates it disabled.
--
-- Returns: 1 when removed, else 0.

local name = ARGV[1]
if redis.call('HGET', KEYS[1], name) ~= ARGV[2] then
    return 0
end

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- Fields are "<name>|<fingerprint>"; the fingerprint carries no "|".
local prefix = name .. '|'
local newest = nil
local entries = redis.call('HGETALL', KEYS[3])
for i = 1, #entries, 2 do
    local field = entries[i]
    if string.sub(field, 1, #prefix) == prefix
        and not string.find(field, '|', #prefix + 1, true) then
        local seen = tonumber(entries[i + 1])
        if newest == nil or seen > newest then
            newest = seen
        end
    end
end

if ARGV[3] == '' then
    if newest ~= nil then
        return 0
    end
elseif newest == nil or newest >= now_ms - tonumber(ARGV[3]) then
    return 0
end
redis.call('HDEL', KEYS[1], name)
redis.call('ZREM', KEYS[2], name)
if ARGV[4] == '1' then
    redis.call('SADD', KEYS[4], name)
end
return 1
