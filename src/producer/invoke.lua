--[[
  Checks if a specified job exists and invokes it immediately

  Input:
    KEYS[1] job table + queue + id
    KEYS[2] queue

    ARGV[1] id
    ARGV[2] queue
    ARGV[3] new score / runAt

  Output:
    0 found and invoked
    1 not found
]]

if redis.call("EXISTS", KEYS[1]) == 0 then
  return 1
end

redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2] .. ":" .. ARGV[1])

-- publishes "invoked" to "<queue>:<id>"
redis.call("PUBLISH", ARGV[2] .. ":" .. ARGV[1], "invoked")
-- publishes "<queue>:<id>" to "invoked"
redis.call("PUBLISH", "invoked", ARGV[2] .. ":" .. ARGV[1])

return 0