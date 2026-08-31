-- Move every due scheduled job from the ZSET into its stream.
--
-- ZRANGEBYSCORE, XADD, DEL and ZREM must be one script: two leaders may run this
-- at the same moment, and only atomicity keeps a job from being enqueued twice.
-- The loser's ZRANGEBYSCORE simply comes back empty.
--
-- Time comes from Redis itself, never from a worker's clock: clock skew between
-- pods would otherwise start jobs early or late.
--
-- KEYS[1]  scheduled ZSET
-- ARGV[1]  maximum jobs to move in one pass
-- ARGV[2]  job hash key prefix
-- returns  the stream entry ids created

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', 0, ARGV[1])
local moved = {}

for _, scheduled_id in ipairs(due) do
    local job_key = ARGV[2] .. scheduled_id
    local flat = redis.call('HGETALL', job_key)
    if #flat > 0 then
        local stream = nil
        local fields = {}
        for i = 1, #flat, 2 do
            if flat[i] == '_stream' then
                stream = flat[i + 1]
            else
                fields[#fields + 1] = flat[i]
                fields[#fields + 1] = flat[i + 1]
            end
        end
        if stream ~= nil and #fields > 0 then
            moved[#moved + 1] = redis.call('XADD', stream, '*', unpack(fields))
        end
        redis.call('DEL', job_key)
    end
    redis.call('ZREM', KEYS[1], scheduled_id)
end

return moved
