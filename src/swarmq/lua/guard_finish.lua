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
-- ARGV[3] TTL des Zustands in ms -- verlaengert nur, verkuerzt nie (BZ-12:
--         ein Claim mit ``running`` hat den Zustand auf den Rueckstand
--         gesetzt, damit die zurueckgestellte Kopie den Marker noch sieht)
-- ARGV[4] Retry-Zaehler der Retry-Kopie (nur "superseded")
--
-- Returns 1

-- Vor dem HDEL lesen: ein leer gewordener Hash verschwindet samt TTL, und
-- das folgende HSET legte ihn ohne Frist neu an.
local remaining = redis.call('PTTL', KEYS[2])
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
end
if redis.call('HGET', KEYS[2], 'lease') == ARGV[1] then
    redis.call('HDEL', KEYS[2], 'lease')
end
local function extend_state()
    local ttl = tonumber(ARGV[3])
    if remaining > ttl then
        ttl = remaining
    end
    redis.call('PEXPIRE', KEYS[2], ttl)
end
if ARGV[2] == 'done' then
    redis.call('HSET', KEYS[2], 'done', '1')
    extend_state()
elseif ARGV[2] == 'superseded' then
    local current = tonumber(redis.call('HGET', KEYS[2], 'sup'))
    if not current or tonumber(ARGV[4]) > current then
        redis.call('HSET', KEYS[2], 'sup', ARGV[4])
    end
    extend_state()
end
return 1
