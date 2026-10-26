-- sliding_window_counter.lua
--
-- Global sliding-window rate limiter for SwarmQ.
--
-- Cloudflare-style approximation: combines the current and previous
-- fixed-size windows weighted by how far we are into the current one.
-- Memory is O(1) per task name (two counters), approximation error
-- around 0.003 % for realistic traffic (Cloudflare public analysis).
--
-- KEYS[1] current-window key ("sq:rl:{task_name}:{window_ts}")
-- KEYS[2] previous-window key ("sq:rl:{task_name}:{prev_window_ts}")
-- ARGV[1] max_requests        (int, >= 0; 0 means reject everything)
-- ARGV[2] window_size_ms      (int, > 0)
-- ARGV[3] now_ms              (int; client supplies server-side TIME
--                             converted to ms, see RateLimiter.check)
--
-- Returns {allowed, remaining, retry_after_ms}
--   allowed         0 | 1
--   remaining       integer >= 0, estimated remaining requests in the window
--   retry_after_ms  integer, always 0 when allowed, >= 1 when rate-limited:
--                   time until the estimate drops below max_requests
--                   (or, for max_requests == 0, until the next window)
--
-- Edge cases are explicit:
--   window_size_ms <= 0  -> return {0, 0, 0} (division-by-zero guard)
--   max_requests == 0    -> every request rejected cleanly
--   no prior data        -> previous_count = 0 -> estimated = 0 -> allowed
--
-- Atomicity: the read-compute-increment-expire sequence runs inside one
-- EVAL, so concurrent callers cannot race past max_requests.

local current_key = KEYS[1]
local previous_key = KEYS[2]
local max_requests = tonumber(ARGV[1])
local window_size_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

if window_size_ms <= 0 then
    return {0, 0, 0}
end

local current_count = tonumber(redis.call('GET', current_key) or '0')
local previous_count = tonumber(redis.call('GET', previous_key) or '0')

local window_start_ms = now_ms - (now_ms % window_size_ms)
local elapsed_ratio = (now_ms - window_start_ms) / window_size_ms

local estimated_count = previous_count * (1.0 - elapsed_ratio) + current_count

if estimated_count >= max_requests then
    -- retry_after: the earliest time the estimate drops below the limit
    -- again, assuming no further admissions (DP-01). Waiting for the end
    -- of the fixed window instead returns every rejected caller at the
    -- boundary, where the previous window still counts in full, and
    -- halves the throughput under a backlog.
    -- The boundaries are computed in integers: with floats, 1 - 55/100
    -- is 0.44999..., and an exact boundary would land on the moment the
    -- estimate equals the limit (still rejected).
    local elapsed_ms = now_ms - window_start_ms
    local wait_ms
    if max_requests <= 0 then
        -- Never any headroom: check again at the next window.
        wait_ms = window_size_ms - elapsed_ms
    elseif current_count < max_requests then
        -- Within this window: previous * (W - t) / W + current < max,
        -- i.e. t > W * (previous - (max - current)) / previous.
        local numerator = window_size_ms * (previous_count - (max_requests - current_count))
        wait_ms = (numerator - numerator % previous_count) / previous_count + 1 - elapsed_ms
    else
        -- Only in the next window: current * (W - t) / W < max,
        -- i.e. t > W * (current - max) / current.
        local numerator = window_size_ms * (current_count - max_requests)
        wait_ms = (window_size_ms - elapsed_ms) + (numerator - numerator % current_count) / current_count + 1
    end
    local retry_after = math.max(1, math.ceil(wait_ms))
    local remaining = math.max(0, math.floor(max_requests - estimated_count))
    return {0, remaining, retry_after}
end

current_count = redis.call('INCR', current_key)

if current_count == 1 then
    local ttl_ms = window_size_ms * 2
    redis.call('PEXPIRE', current_key, ttl_ms)
end

local new_estimated = previous_count * (1.0 - elapsed_ratio) + current_count
local remaining = math.max(0, math.floor(max_requests - new_estimated))

return {1, remaining, 0}
