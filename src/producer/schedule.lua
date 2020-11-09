--[[
  Adds the job's data to the job table and schedules it.

  Input:
    KEYS[1] job table + queue + id
    KEYS[2] job table index: by queue
    KEYS[3] queue

    ARGV[1] id
    ARGV[2] queue
    ARGV[3] payload
    ARGV[4] scheduled execution date
    ARGV[5] schedule_type
    ARGV[6] schedule_meta
    ARGV[7] maximum execution times
    ARGV[8] override if id already exists

  Output:
    0 if everything went fine
    1 if override semantics weren't used and there was another job with this id
]]

if ARGV[8] == "false" then  
  if redis.call("EXISTS", KEYS[1]) == 1 then
    return 1
  end
end

redis.call("HSET", KEYS[1], "payload", ARGV[3], "schedule_type", ARGV[5], "schedule_meta", ARGV[6], "max_times", ARGV[7], "count", 1)

redis.call("SADD", KEYS[2], ARGV[1])

-- enqueues it
redis.call("ZADD", KEYS[3], ARGV[4], ARGV[2] .. ":" .. ARGV[1])

-- publishes "scheduled" to "<queue>:<id>"
redis.call("PUBLISH", ARGV[2] .. ":" .. ARGV[1], "scheduled" .. ":" .. ARGV[4] .. ":" .. ARGV[5] .. ":" .. ARGV[6] .. ":" .. ARGV[7] .. ":" .. 1 .. ":" .. ARGV[3])
-- publishes "<queue>:<id>" to "scheduled"
redis.call("PUBLISH", "scheduled", ARGV[2] .. ":" .. ARGV[1])

return 0