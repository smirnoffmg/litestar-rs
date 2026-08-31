-- Delete consumers that hold nothing and have gone quiet.
--
-- Check and delete must be one step. Redis says the pending entries of a
-- deleted consumer "become unclaimable", so a gap between reading `pending` and
-- issuing DELCONSUMER is a window in which a live worker takes an entry and we
-- destroy it. A script is the only way to close that.
--
-- KEYS[1] stream
-- ARGV[1] group, ARGV[2] minimum idle milliseconds
local group = ARGV[1]
local min_idle = tonumber(ARGV[2])
local deleted = 0

for _, consumer in ipairs(redis.call('XINFO', 'CONSUMERS', KEYS[1], group)) do
    local name, pending, idle
    for i = 1, #consumer, 2 do
        local field = consumer[i]
        if field == 'name' then
            name = consumer[i + 1]
        elseif field == 'pending' then
            pending = tonumber(consumer[i + 1])
        elseif field == 'idle' then
            idle = tonumber(consumer[i + 1])
        end
    end
    -- Idleness alone is not enough: a worker running one long task is idle on
    -- the group while still owning the entry it is working on.
    if name and pending == 0 and idle and idle >= min_idle then
        redis.call('XGROUP', 'DELCONSUMER', KEYS[1], group, name)
        deleted = deleted + 1
    end
end

return deleted
