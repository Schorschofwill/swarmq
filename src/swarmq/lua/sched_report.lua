-- sched_report.lua
--
-- Record one worker's report of a static cron (Plan 2026-10-08-1541, KTD10).
-- Times come from the server clock (TIME), so clock skew between workers
-- cannot change which definition the leader picks.
--
-- KEYS[1] sq:{sched}:report:first_seen   (hash; field -> ms)
-- KEYS[2] sq:{sched}:report:last_seen    (hash; field -> ms)
-- KEYS[3] sq:{sched}:report:def          (hash; field -> definition json)
-- ARGV[1] field             ("<schedule_name>|<fingerprint>")
-- ARGV[2] definition json   (the full desired definition)
-- ARGV[3] live_window_ms    (int; a fingerprint whose last_seen is older
--                            is dead and counts as new when it returns)
--
-- first_seen is only written when the field is missing or the fingerprint
-- was dead, so a revert A -> B -> A makes A the newest definition again.
--
-- Returns: first_seen_ms of the field after the call.

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local field = ARGV[1]
local window = tonumber(ARGV[3])

local first = redis.call('HGET', KEYS[1], field)
local last = redis.call('HGET', KEYS[2], field)
if not first or not last or tonumber(last) < now_ms - window then
    first = now_ms
    redis.call('HSET', KEYS[1], field, now_ms)
end
redis.call('HSET', KEYS[2], field, now_ms)
redis.call('HSET', KEYS[3], field, ARGV[2])
return tonumber(first)
