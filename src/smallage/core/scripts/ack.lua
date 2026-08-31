-- Acknowledge and remove entries in one step.
--
-- XACK on its own leaves the record in the stream and the stream leaks; XDEL on
-- its own leaves it in the PEL. They must not be separable.
--
-- KEYS[1]     stream
-- KEYS[2..]   alive keys, one per entry id, same order as ARGV[2..]
-- ARGV[1]     consumer group
-- ARGV[2..]   entry ids
-- returns     number of entries acknowledged

local ids = {}
for i = 2, #ARGV do
    ids[#ids + 1] = ARGV[i]
end
if #ids == 0 then
    return 0
end

local acked = redis.call('XACK', KEYS[1], ARGV[1], unpack(ids))
redis.call('XDEL', KEYS[1], unpack(ids))

if #KEYS > 1 then
    local alive = {}
    for i = 2, #KEYS do
        alive[#alive + 1] = KEYS[i]
    end
    redis.call('DEL', unpack(alive))
end

return acked
