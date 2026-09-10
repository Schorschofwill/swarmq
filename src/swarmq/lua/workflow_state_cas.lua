-- workflow_state_cas.lua
--
-- Compare-and-Set des Workflow-Zustands (Plan 2026-09-08-001, KTD11).
--
-- ``advance()`` ist ein Read-Modify-Write: zwischen Lesen und Schreiben hat
-- der Advancer bereits publiziert und den Chord-Zaehler bewegt. Ein blindes
-- SET ueberschrieb bis 0.5.x den Schritt-Marker eines nebenlaeufigen
-- Advancers (verlorenes Zustands-Update). Dieses Skript schreibt nur, wenn
-- die ``version`` des Bestands der erwarteten entspricht; den Merge macht
-- der Client (R23), nicht das Skript.
--
-- Das Skript DEKODIERT nur (die Version), es KODIERT nie: Die Payload wird
-- byteweise geschrieben. ``cjson.encode`` wuerde Fliesskommazahlen in den
-- Task-Argumenten der Topologie verfaelschen.
--
-- KEYS[1] Zustandsschluessel ("sq:wf:{workflow_id}")
-- ARGV[1] erwartete Version (fehlt ``version`` im Bestand, gilt 0)
-- ARGV[2] Payload (Bytes, unveraendert geschrieben)
-- ARGV[3] TTL in Millisekunden
--
-- Rueckgabe:
--   1 -> geschrieben (Version passte)
--   0 -> nicht geschrieben (Version verschieden oder Schluessel fehlt)

local current = redis.call('GET', KEYS[1])
if not current then
    return 0
end

local version = 0
local doc = cjson.decode(current)
if type(doc) == 'table' and doc['version'] ~= nil then
    version = tonumber(doc['version']) or 0
end

if version ~= tonumber(ARGV[1]) then
    return 0
end

redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
