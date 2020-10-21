--[[
  Acknowledges a job.
  Removes it from the "processing" set.

  Input:
    KEYS[1] job table + queue + id
    KEYS[2] job table index: by queue
    KEYS[3] processing

    ARGV[1] id
    ARGV[2] queue
]]

redis.call("LREM", KEYS[3], 0, ARGV[2] .. ":" .. ARGV[1])
redis.call("DEL", KEYS[1])
redis.call("SREM", KEYS[2], ARGV[1])

-- publishes "acknowledged" to "<queue>:<id>"
redis.call("PUBLISH", ARGV[2] .. ":" .. ARGV[1], "acknowledged")
-- publishes "acknowledged:<id>" to "<queue>"
redis.call("PUBLISH", ARGV[2], "acknowledged" .. ":" .. ARGV[1])
-- publishes "<queue>:<id>" to "acknowledged"
redis.call("PUBLISH", "acknowledged", ARGV[2] .. ":" .. ARGV[1])
