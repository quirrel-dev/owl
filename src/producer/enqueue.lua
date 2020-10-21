--[[
  Adds the job's data to the job table and enqueues it.

  Input:
    KEYS[1] job table + queue + id
    KEYS[2] job table index: by queue
    KEYS[3] queue

    ARGV[1] id
    ARGV[2] queue
    ARGV[3] payload
]]

-- adds job data to table
redis.call("HSET", KEYS[1], "payload", ARGV[3])
redis.call("SADD", KEYS[2], ARGV[1])

-- enqueus it
redis.call("RPUSH", KEYS[3], ARGV[2] .. ":" .. ARGV[1])

-- publishes "enqueued" to "<queue>:<id>"
redis.call("PUBLISH", ARGV[2] .. ":" .. ARGV[1], "enqueued")
-- publishes "enqueued:<id>" to "<queue>"
redis.call("PUBLISH", ARGV[2], "enqueued" .. ":" .. ARGV[1])
-- publishes "<queue>:<id>" to "enqueued"
redis.call("PUBLISH", "enqueued", ARGV[2] .. ":" .. ARGV[1])
