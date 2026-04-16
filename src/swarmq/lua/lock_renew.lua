-- lock_renew.lua
--
-- Atomically renew the TTL of a mutex lock that we still own.
--
-- Mutex-only (v1). The Python wrapper ``LockManager.renew`` guards
-- against semaphore callers with a NotImplementedError so this script
-- never sees a SortedSet value — a lookup would otherwise raise
-- WRONGTYPE at Valkey. Semaphore-renew is out of scope for the
-- embedded scheduler in Phase 3c; see plan Review-Finding P2-8.
--
-- KEYS[1] lock key        ("sq:lock:{lock_name}")
-- ARGV[1] owner_token     (string; uuid4().hex)
-- ARGV[2] ttl_ms          (int, milliseconds)
--
-- Returns:
--   1 -> lock belonged to us; TTL extended to ttl_ms
--   0 -> lock absent, expired, or held by a different owner — no mutation

if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
end
return 0
