-- chord_complete.lua
--
-- Atomically record a chord header-member completion and decide whether
-- THIS completion is the one that fires the chord body — exactly once,
-- idempotent against at-least-once redelivery (R12).
--
-- Completion is tracked per-member (SADD of the member's task id into a
-- set), NOT a blind INCR: a redelivered member that is already in the set
-- does not change the cardinality, so it cannot push the count past N or
-- trigger a second body fire. The body-enqueue decision is guarded by a
-- one-shot SET NX flag set in the SAME script invocation, so even if two
-- distinct members reach cardinality N "simultaneously" (separate EVALs
-- are still serialized by Valkey's single-threaded execution), only the
-- first to set the flag returns 1.
--
-- KEYS[1] completed_set   (SADD of member task ids; SCARD == distinct done)
-- KEYS[2] body_flag       (one-shot "body already enqueued" marker)
-- ARGV[1] member_id       (the completing member's task id — idempotency key)
-- ARGV[2] expected_n      (total header members; body fires when SCARD == N)
-- ARGV[3] ttl_ms          (TTL applied to both keys; >= workflow lifetime)
--
-- Returns:
--   1 -> this call should enqueue the chord body (exactly one call ever does)
--   0 -> not complete yet, OR already-counted redelivery, OR body already fired

local ttl_ms = tonumber(ARGV[3])

redis.call('SADD', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ttl_ms)

local count = redis.call('SCARD', KEYS[1])
if count == tonumber(ARGV[2]) then
    if redis.call('SET', KEYS[2], '1', 'NX', 'PX', ttl_ms) then
        return 1
    end
end
return 0
