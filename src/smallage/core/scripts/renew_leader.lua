-- Extend leadership only if we still hold it.
--
-- A plain PEXPIRE would let a worker that already lost the lock keep extending
-- the new leader's key, which is how two leaders end up believing in themselves.
--
-- KEYS[1]  leader key
-- ARGV[1]  our token
-- ARGV[2]  ttl in ms
-- returns  1 if renewed, 0 if someone else holds it

if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
