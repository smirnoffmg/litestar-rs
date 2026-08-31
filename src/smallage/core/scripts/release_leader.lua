-- Give up leadership, but only our own.
--
-- KEYS[1]  leader key
-- ARGV[1]  our token
-- returns  1 if released, 0 if it was not ours

if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
