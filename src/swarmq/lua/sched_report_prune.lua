-- sched_report_prune.lua
--
-- Delete cron reports whose last_seen is older than the given age
-- (Plan 2026-10-08-1541, KTD10: reports older than 7 days). Server clock
-- (TIME), and the check plus delete run in one step, so a report that is
-- refreshed concurrently is never deleted.
--
-- KEYS[1] sq:{sched}:report:first_seen
-- KEYS[2] sq:{sched}:report:last_seen
-- KEYS[3] sq:{sched}:report:def
-- ARGV[1] max_age_ms (int)
--
-- Returns: number of deleted reports.

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local cutoff = now_ms - tonumber(ARGV[1])

local entries = redis.call('HGETALL', KEYS[2])
local deleted = 0
for i = 1, #entries, 2 do
    if tonumber(entries[i + 1]) < cutoff then
        redis.call('HDEL', KEYS[1], entries[i])
        redis.call('HDEL', KEYS[2], entries[i])
        redis.call('HDEL', KEYS[3], entries[i])
        deleted = deleted + 1
    end
end
return deleted
