--[[
  Checks if a specified job exists and deletes it

  Input:
    KEYS[1] job table + queue + id
    KEYS[2] job table index: by queue
    KEYS[3] queue

    ARGV[1] id
    ARGV[2] queue

  Output:
    0 found and deleted
    1 not found
]]

if redis.call("DEL", KEYS[1]) == 0 then
  return 1
end

redis.call("SREM", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[2] .. ":" .. ARGV[1])

return 0