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
-- ARGV[2] ttl_ms            (int, > 0; max(30, 0.25 * hard_timeout) seconds in ms)
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
--   owner_token already   -> mutex: SET NX returns nil -> 0;
--   in semaphore set         semaphore: ZADD treats it as no-op (already member),
--                            count check still applies — caller should not retry
--                            with the same token.

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
    local ok = redis.call('SET', lock_key, owner_token, 'NX', 'PX', ttl_ms)
    if ok then
        return 1
    end
    return 0
end

-- Semaphore: SortedSet, score = acquire-time-ms, member = owner_token.
-- Step 1: clean up members that expired (score < now - ttl).
redis.call('ZREMRANGEBYSCORE', lock_key, '-inf', '(' .. (now_ms - ttl_ms))

-- Step 2: count remaining live holders.
local held = redis.call('ZCARD', lock_key)
if held >= max_concurrency then
    return 0
end

-- Step 3: claim a slot and refresh the whole-set TTL so the key dies
-- naturally once every holder has released or expired.
redis.call('ZADD', lock_key, now_ms, owner_token)
redis.call('PEXPIRE', lock_key, ttl_ms)
return 1
