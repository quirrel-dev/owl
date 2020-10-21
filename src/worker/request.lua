--[[
  Requests a job.
  Moves it to the "processing" set.
  Returns its data.

  Input:
    KEYS[1] queue
    KEYS[2] processing

    ARGV[1] job table prefix
    ARGV[2] current timestamp

  Output:
    nil, if no job was found
    if a job was found:
      - queue
      - id
      - payload
      - schedule type (if exists)
      - schedule meta (if exists)
]]

local result = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[2], "LIMIT", "0", "1")
local queueAndId = result[1]

if not queueAndId then
  return -1
end

redis.call("SADD", KEYS[2], queueAndId)
redis.call("ZREM", KEYS[1], queueAndId)
local queue, id = queueAndId:match("([^,]+):([^,]+)")

local payload, schedule_type, schedule_meta = redis.call("HGET", ARGV[1] .. ":" .. queueAndId, "payload", "schedule_type", "schedule_meta")

-- publishes "requested" to "<queue>:<id>"
redis.call("PUBLISH", queue .. ":" .. id, "requested")
-- publishes "requested:<id>" to "<queue>"
redis.call("PUBLISH", queue, "requested" .. ":" .. id)
-- publishes "<queue>:<id>" to "requested"
redis.call("PUBLISH", "requested", queue .. ":" .. id)

return { queue, id, payload, schedule_type, schedule_meta }
