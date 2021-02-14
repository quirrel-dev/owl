--[[
  Acknowledges a job.
  Removes it from the "processing" set.

  Input:
    KEYS[1] job table + queue + id
    KEYS[2] job table index: by queue
    KEYS[3] processing
    KEYS[4] queues
    KEYS[5] blocked jobs zset key
    KEYS[6] blocked queues set key
    KEYS[7] soft-block counter hashmap

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

redis.call("HINCRBY", KEYS[7], ARGV[2], -1)

local blocked = redis.call("ZRANGE", KEYS[5], 0, -1, "WITHSCORES")

if #blocked > 0 then
  for i = 1, #blocked - 1, 2
  do
    local id = blocked[i]
    local score = blocked[i + 1]

    redis.call("ZADD", KEYS[4], score, ARGV[2] .. ":" .. id)
  end

  redis.call("DEL", KEYS[5])
  redis.call("SREM", KEYS[6], ARGV[2])
  redis.call("PUBLISH", "unblocked", ARGV[2])
end