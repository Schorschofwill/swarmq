-- chord_complete.lua
--
-- Atomically record a chord header-member completion and report whether the
-- chord is now complete (all N distinct members done) — idempotent against
-- at-least-once redelivery (R12).
--
-- Completion is tracked per-member (SADD of the member's task id into a
-- set), NOT a blind INCR: a redelivered member that is already in the set
-- does not change the cardinality, so it cannot push the count past N. This
-- call returns 1 whenever the set has reached N distinct members.
--
-- The ACTUAL one-shot body enqueue is guarded DOWNSTREAM by the
-- publish-then-mark step_tasks["/b"] marker (workflow_orchestration.py),
-- symmetric with chain advancement: a rare double body-enqueue under a
-- crash is absorbed by step idempotency, but the body is never LOST. An
-- earlier design set a one-shot SET NX flag INSIDE this script and returned
-- 1 only to the flag-setter; that consumed the one-shot BEFORE the body was
-- durably published, so a crash in the flag→publish window made every
-- redelivery return 0 and the body never fired (R12 zero-times, R16 hang).
--
-- KEYS[1] completed_set   (SADD of member task ids; SCARD == distinct done)
-- ARGV[1] member_id       (the completing member's task id — idempotency key)
-- ARGV[2] expected_n      (total header members; complete when SCARD == N)
-- ARGV[3] ttl_ms          (TTL applied to the set; >= workflow lifetime)
--
-- Returns:
--   1 -> the chord is complete; caller attempts the publish-then-mark body enqueue
--   0 -> not complete yet (or an already-counted redelivery before completion)

local ttl_ms = tonumber(ARGV[3])

redis.call('SADD', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ttl_ms)

if redis.call('SCARD', KEYS[1]) == tonumber(ARGV[2]) then
    return 1
end
return 0
