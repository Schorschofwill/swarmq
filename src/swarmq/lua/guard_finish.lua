-- guard_finish.lua
--
-- Abschluss einer Ausfuehrung im Ausfuehrungsschutz (Plan 2026-10-08-1541,
-- KTD5): den eigenen Running-Marker loeschen und je nach Ausgang den Done-
-- Marker setzen.
--
-- KEYS[1] Running-Marker ("sq:guard:{<identity>}:run")
-- KEYS[2] Zustand        ("sq:guard:{<identity>}:state", Hash)
-- ARGV[1] Owner-Token
-- ARGV[2] Ausgang: "done", "superseded" oder "release" (nur freigeben)
-- ARGV[3] TTL des Zustands in ms
-- ARGV[4] Retry-Zaehler der Retry-Kopie (nur "superseded")
--
-- Returns 1

if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
end
if redis.call('HGET', KEYS[2], 'lease') == ARGV[1] then
    redis.call('HDEL', KEYS[2], 'lease')
end
if ARGV[2] == 'done' then
    redis.call('HSET', KEYS[2], 'done', '1')
    redis.call('PEXPIRE', KEYS[2], ARGV[3])
elseif ARGV[2] == 'superseded' then
    local current = tonumber(redis.call('HGET', KEYS[2], 'sup'))
    if not current or tonumber(ARGV[4]) > current then
        redis.call('HSET', KEYS[2], 'sup', ARGV[4])
    end
    redis.call('PEXPIRE', KEYS[2], ARGV[3])
end
return 1
