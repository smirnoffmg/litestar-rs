-- Claim one pending entry whose owner is gone.
--
-- The liveness check and the claim must be one script: between them a live worker
-- refreshes its alive key and would have its entry stolen mid-execution.
--
-- XCLAIM's own MINIDLETIME picks a single winner among racing reclaimers -- the
-- first successful claim resets idle time to 0, so the next one gets nothing.
-- JUSTID is deliberately not used: it would not bump the delivery counter, and
-- that counter is the reclaim counter.
--
-- KEYS[1]  stream
-- KEYS[2]  alive key of this entry
-- ARGV[1]  consumer group
-- ARGV[2]  claiming consumer
-- ARGV[3]  minimum idle time, ms
-- ARGV[4]  entry id
-- ARGV[5]  alive key ttl, ms
-- returns  the XCLAIM reply, or an empty array when not claimed

if redis.call('EXISTS', KEYS[2]) == 1 then
    return {}
end

local claimed = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4])
if #claimed == 0 then
    return {}
end

redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[5])
return claimed
