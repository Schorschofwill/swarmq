-- lock_release.lua
--
-- Atomically release a SwarmQ task lock.
--
-- Owner-token check protects against foreign release: a worker that
-- inherited the lock via TTL expiry (the previous owner crashed) cannot
-- have its lock yanked by the original owner if the original wakes up.
-- The check + DEL (or ZREM) runs inside one EVAL so there is no TOCTOU
-- window.
--
-- Three modes selected by ARGV[1]:
--   __force__ literal -> Admin override: unconditional DEL (mutex) or
--                        full key DEL (semaphore). Reserved for Phase
--                        3b.3 (Cancellation) and Phase 3c
--                        (`swarmq locks force-release`). Returns 2 so
--                        callers can distinguish forced vs owned release.
--   max_concurrency==1 -> Mutex: GET-and-compare, DEL only on owner match.
--   max_concurrency >1 -> Semaphore: ZREM owner_token (no GET needed —
--                          ZREM returns 0 cleanly when the token isn't
--                          a member, so the foreign-release case is the
--                          natural no-op).
--
-- KEYS[1] lock key         ("sq:lock:{lock_name}")
-- ARGV[1] owner_token      (string; uuid4().hex OR the literal "__force__")
-- ARGV[2] max_concurrency  (int, >= 1; ignored when ARGV[1] == "__force__")
--
-- Returns:
--   1 -> released by owner (or admin DEL of an existing key in mutex mode)
--   0 -> nothing released (foreign owner, key did not exist, or token not
--        a semaphore member)
--   2 -> released via __force__ admin override

local lock_key = KEYS[1]
local owner_token = ARGV[1]
local max_concurrency = tonumber(ARGV[2])

-- Admin force-release path. Unconditional DEL — the key is gone whether
-- it was a mutex string or a semaphore SortedSet. Returns 2 so callers
-- can log it differently from a normal owned release.
if owner_token == "__force__" then
    redis.call('DEL', lock_key)
    return 2
end

if max_concurrency == 1 then
    -- Mutex: only release if we are the recorded owner.
    if redis.call('GET', lock_key) == owner_token then
        redis.call('DEL', lock_key)
        return 1
    end
    return 0
end

-- Semaphore: ZREM is owner-safe by construction — it returns the number
-- of removed members, which is 1 when the token was held by us and 0
-- otherwise. No separate GET-and-compare needed.
local removed = redis.call('ZREM', lock_key, owner_token)
if removed == 1 then
    return 1
end
return 0
