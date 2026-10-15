-- lock_acquire.lua
--
-- Atomically acquire a SwarmQ task lock.
--
-- Two modes selected by ARGV[3] (max_concurrency):
--   max_concurrency == 1  -> Mutex: simple SET NX PX with owner-token value
--   max_concurrency  > 1  -> Semaphore: SortedSet of owner-tokens scored by
--                            acquire-timestamp (ms). Expired members are
--                            cleaned up in the same EVAL via
--                            ZREMRANGEBYSCORE before the count check, so the
--                            lock never gets wedged by stale entries from
--                            crashed workers.
--
-- KEYS[1] lock key   ("sq:lock:{lock_name}")
-- ARGV[1] owner_token       (string; uuid4().hex from caller)
-- ARGV[2] ttl_ms            (int, > 0; (hard_timeout + 30) seconds in ms)
-- ARGV[3] max_concurrency   (int, >= 1; 1 = mutex, >1 = semaphore)
-- ARGV[4] now_ms            (int; server-side TIME from caller, mirrors
--                            sliding_window_counter.lua's pattern so
--                            wall-clock skew between Lua and worker doesn't
--                            corrupt expiry math)
--
-- Returns: 1 (acquired) | 0 (conflict)
--
-- Edge cases:
--   ttl_ms <= 0           -> return 0 (refuse — programming error in caller)
--   max_concurrency < 1   -> return 0 (refuse — programming error in caller)
--   owner_token already   -> 1 without touching the TTL (mutex: GET equals
--   held                     the token; semaphore: live ZSCORE). valkey-py
--                            re-sends EVALSHA with identical ARGV after a lost
--                            reply (FS-06b); the token is a fresh uuid4 per
--                            acquire, so only that retry can hit it. The
--                            scheduler leader (stable token) re-wins its own
--                            key at once instead of waiting for the TTL.

local lock_key = KEYS[1]
local owner_token = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local max_concurrency = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

if ttl_ms <= 0 or max_concurrency < 1 then
    return 0
end

if max_concurrency == 1 then
    -- Mutex: single-holder string key with owner_token as value.
    -- Own token already in the key -> acquired (FS-06b): the client's
    -- transport retry after a lost reply re-sends this call; the TTL stays.
    if redis.call('GET', lock_key) == owner_token then
        return 1
    end
    local ok = redis.call('SET', lock_key, owner_token, 'NX', 'PX', ttl_ms)
    if ok then
        return 1
    end
    return 0
end

-- Semaphore: SortedSet, score = acquire-time-ms, member = owner_token.
-- Step 1: clean up members that expired (score < now - ttl).
redis.call('ZREMRANGEBYSCORE', lock_key, '-inf', '(' .. (now_ms - ttl_ms))

-- Step 1b: own token still live -> acquired (FS-06b), before the count so
-- a full set including the own token does not refuse the transport retry.
-- Score and TTL stay as they are.
if redis.call('ZSCORE', lock_key, owner_token) then
    return 1
end

-- Step 2: count remaining live holders.
local held = redis.call('ZCARD', lock_key)
if held >= max_concurrency then
    return 0
end

-- Step 3: claim a slot and refresh the whole-set TTL so the key dies
-- naturally once every holder has released or expired. Never shorten it
-- (Plan 0.12.0, MR 13, Rev. 3 F6): an acquirer with a short TTL must not
-- expire the key under a holder with a longer one. Members still expire
-- by score (step 1) at the TTL of whoever acquires next.
redis.call('ZADD', lock_key, now_ms, owner_token)
if redis.call('PTTL', lock_key) < ttl_ms then
    redis.call('PEXPIRE', lock_key, ttl_ms)
end
return 1
