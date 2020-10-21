--[[
  Adds the job's data to the job table and schedules it.

  Input:
    KEYS[1] job table + queue + id
    KEYS[2] job table index: by queue
    KEYS[3] queue

    ARGV[1] id
    ARGV[2] queue
    ARGV[3] payload
    ARGV[4] scheduled execution date ("-inf" for immediate execution)
]]

-- adds job data to table
redis.call("HSET", KEYS[1], "payload", ARGV[3])
redis.call("SADD", KEYS[2], ARGV[1])

-- enqueus it
redis.call("ZADD", KEYS[3], ARGV[4], ARGV[2] .. ":" .. ARGV[1])

-- publishes "scheduled" to "<queue>:<id>"
redis.call("PUBLISH", ARGV[2] .. ":" .. ARGV[1], "scheduled")
-- publishes "scheduled:<id>" to "<queue>"
redis.call("PUBLISH", ARGV[2], "scheduled" .. ":" .. ARGV[1])
-- publishes "<queue>:<id>" to "scheduled"
redis.call("PUBLISH", "scheduled", ARGV[2] .. ":" .. ARGV[1])
