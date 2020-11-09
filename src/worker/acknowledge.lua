--[[
  Acknowledges a job.
  Removes it from the "processing" set.

  Input:
    KEYS[1] job table + queue + id
    KEYS[2] job table index: by queue
    KEYS[3] processing
    KEYS[4] queues

    ARGV[1] id
    ARGV[2] queue
    ARGV[3] timestamp to reschedule for
            if undefined / empty string, job will be deleted
]]

redis.call("SREM", KEYS[3], ARGV[2] .. ":" .. ARGV[1])

-- publishes "acknowledged" to "<queue>:<id>"
redis.call("PUBLISH", ARGV[2] .. ":" .. ARGV[1], "acknowledged")
-- publishes "<queue>:<id>" to "acknowledged"
redis.call("PUBLISH", "acknowledged", ARGV[2] .. ":" .. ARGV[1])

if ARGV[3] == '' then
  redis.call("DEL", KEYS[1])
  redis.call("SREM", KEYS[2], ARGV[1])
else
  redis.call("ZADD", KEYS[4], ARGV[3], ARGV[2] .. ":" .. ARGV[1])

  redis.call("HINCRBY", KEYS[1], "count", 1)
  
  -- publishes "rescheduled" to "<queue>:<id>"
  redis.call("PUBLISH", ARGV[2] .. ":" .. ARGV[1], "rescheduled:" .. ARGV[3])
  -- publishes "<queue>:<id>" to "rescheduled"
  redis.call("PUBLISH", "rescheduled", ARGV[2] .. ":" .. ARGV[1])
end