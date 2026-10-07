-- sched_fire_claim.lua
--
-- In-Flight-Claim eines Schedule-Fires mit Owner-Token und Uebernahme.
--
-- Der Worker beansprucht einen Fire vor dem Lauf (kurze TTL). Wird die
-- Zustellung nicht ausgefuehrt (Zurueckstellung bei Backend-Ausfall, Retry,
-- Requeue), traegt die Kopie das Token als Header. Scheitert die Freigabe
-- (Valkey weg), erkennt die Kopie den Claim an diesem Token als ihren und
-- uebernimmt ihn, statt sich als Duplikat zu bestaetigen. Die Uebernahme
-- rotiert den Owner: eine Broker-Redelivery derselben Kopie (altes Token)
-- bleibt ein Duplikat.
--
-- KEYS[1] In-Flight-Key   ("sq:sched:inflight:<schedfire_key>")
-- ARGV[1] Owner-Token     (neuer Wert des Claims)
-- ARGV[2] TTL in ms
-- ARGV[3] Uebernahme-Token ("" = keine Uebernahme)
--
-- Returns:
--   1 -> Claim gehoert jetzt ARGV[1] (frei gewesen oder uebernommen)
--   0 -> fremder Claim (Duplikat)

local current = redis.call('GET', KEYS[1])
if (not current) or (ARGV[3] ~= '' and current == ARGV[3]) then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
end
return 0
