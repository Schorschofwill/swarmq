-- guard_claim.lua
--
-- Claim des Ausfuehrungsschutzes (Plan 2026-10-08-1541, KTD5).
--
-- Der Zustand haelt neben Done- und Superseded-Marker die ``lease``: das
-- Owner-Token des letzten Claims, den noch kein Abschluss (guard_finish.lua)
-- freigegeben hat. Fehlt der Running-Marker, steht die Lease aber noch, ist
-- die Ausfuehrung ohne Freigabe abgelaufen (Waise): ``orphans`` zaehlt hoch.
--
-- KEYS[1] Running-Marker ("sq:guard:{<identity>}:run")
-- KEYS[2] Zustand        ("sq:guard:{<identity>}:state", Hash)
-- ARGV[1] Owner-Token
-- ARGV[2] TTL des Running-Markers in ms
-- ARGV[3] Retry-Zaehler der Zustellung
-- ARGV[4] TTL des Zustands in ms
-- ARGV[5] Waisen-Grenze: ab so vielen Waisen ``orphan_limit`` (der Marker
--         gehoert trotzdem ARGV[1], damit das folgende Dead-Letter ihn
--         abschliesst)
-- ARGV[6] Rueckstand in ms (Plan 0.12.0, MR 12, BZ-12): meldet der Claim
--         ``running``, wird die Zustellung zurueckgestellt und kann hinter
--         einem Rueckstand zurueckkommen -- der Zustand (und mit ihm der
--         kommende Done-Marker) lebt mindestens so lange; 0 = unveraendert
--
-- Returns {status, orphans}

if redis.call('HGET', KEYS[2], 'done') then
    return {'done', 0}
end
local superseded = tonumber(redis.call('HGET', KEYS[2], 'sup'))
if superseded and tonumber(ARGV[3]) < superseded then
    return {'superseded', 0}
end
local orphans = tonumber(redis.call('HGET', KEYS[2], 'orphans')) or 0
local holder = redis.call('GET', KEYS[1])
if holder == ARGV[1] then
    -- Wiederholung desselben Claims (Transport-Retry nach verlorener
    -- Antwort): idempotent, keine weitere Waise.
    if orphans >= tonumber(ARGV[5]) then
        return {'orphan_limit', orphans}
    end
    return {'claimed', orphans}
end
if holder then
    local backlog = tonumber(ARGV[6]) or 0
    if backlog > 0 and backlog > redis.call('PTTL', KEYS[2]) then
        redis.call('PEXPIRE', KEYS[2], backlog)
    end
    return {'running', 0}
end
if redis.call('HEXISTS', KEYS[2], 'lease') == 1 then
    orphans = redis.call('HINCRBY', KEYS[2], 'orphans', 1)
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('HSET', KEYS[2], 'lease', ARGV[1])
-- Nie verkuerzen: ein Claim mit ``running`` kann den Zustand schon auf den
-- Rueckstand gesetzt haben (BZ-12).
if tonumber(ARGV[4]) > redis.call('PTTL', KEYS[2]) then
    redis.call('PEXPIRE', KEYS[2], ARGV[4])
end
if orphans >= tonumber(ARGV[5]) then
    return {'orphan_limit', orphans}
end
return {'claimed', orphans}
