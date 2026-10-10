-- lock_extend.lua
--
-- Extend the deadline of a SwarmQ task lock we still hold (Plan 0.12.0,
-- MR 13, BZ-25). The lock keeper calls it while the handler runs and the
-- lock is about to expire. Never shortens: the key keeps the longer of its
-- remaining PTTL and ttl_ms.
--
-- Two modes selected by ARGV[3] (max_concurrency), like lock_acquire.lua:
--   max_concurrency == 1  -> Mutex: owner check, then PEXPIRE if longer.
--   max_concurrency  > 1  -> Semaphore: raise our member's score to now_ms
--                            (never lower it; lock_acquire.lua sweeps members
--                            whose score is older than the caller's TTL) and
--                            PEXPIRE the whole SortedSet if longer.
--
-- KEYS[1] lock key         ("sq:lock:{lock_name}")
-- ARGV[1] owner_token      (string; uuid4().hex)
-- ARGV[2] ttl_ms           (int, > 0; minimum remaining lifetime from now)
-- ARGV[3] max_concurrency  (int, >= 1)
-- ARGV[4] now_ms           (int; server-side TIME from caller, as in acquire)
--
-- Returns: 1 (extended) | 0 (lock absent, expired or held by someone else;
--          no mutation)

local lock_key = KEYS[1]
local owner_token = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local max_concurrency = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

if ttl_ms <= 0 or max_concurrency < 1 then
    return 0
end

if max_concurrency == 1 then
    if redis.call('GET', lock_key) ~= owner_token then
        return 0
    end
    if redis.call('PTTL', lock_key) < ttl_ms then
        redis.call('PEXPIRE', lock_key, ttl_ms)
    end
    return 1
end

local score = redis.call('ZSCORE', lock_key, owner_token)
if not score then
    return 0
end
if tonumber(score) < now_ms then
    redis.call('ZADD', lock_key, now_ms, owner_token)
end
if redis.call('PTTL', lock_key) < ttl_ms then
    redis.call('PEXPIRE', lock_key, ttl_ms)
end
return 1
