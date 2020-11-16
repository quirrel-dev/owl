--[[
  Requests a job.
  Moves it to the "processing" set.
  Returns its data.

  Input:
    KEYS[1] queue
    KEYS[2] processing
    KEYS[3] blocked queues

    ARGV[1] job table prefix
    ARGV[2] current timestamp
    ARGV[3] blocked queues prefix

  Output:
    nil, if no job was found
    -1, if job was found but is blocked
    number, if job was found that's not ready to be executed
    if a job was found:
      - queue
      - id
      - payload
      - runAt
      - schedule type (if exists)
      - schedule meta (if exists)
      - count
      - max times
]]

local result = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
local queueAndId = result[1]
local scoreString = result[2]

if not queueAndId then
  return nil
end

local score = tonumber(scoreString)

if score > tonumber(ARGV[2]) then
  return score
end

local queue, id = queueAndId:match("([^,]+):([^,]+)")

redis.call("ZREM", KEYS[1], queueAndId)

if redis.call("SISMEMBER", KEYS[3], queue) == 1 then
  redis.call("ZADD", ARGV[3] .. ":" .. queue, scoreString, id)
  return -1
end

redis.call("SADD", KEYS[2], queueAndId)

local jobData = redis.call("HMGET", ARGV[1] .. ":" .. queueAndId, "payload", "schedule_type", "schedule_meta", "count", "max_times", "exclusive")

local payload = jobData[1]
local schedule_type = jobData[2]
local schedule_meta = jobData[3]
local count = jobData[4]
local max_times = jobData[5]
local exclusive = jobData[6]

if exclusive == "true" then
  redis.call("SADD", KEYS[3], queue)
end

-- publishes "requested" to "<queue>:<id>"
redis.call("PUBLISH", queue .. ":" .. id, "requested")
-- publishes "<queue>:<id>" to "requested"
redis.call("PUBLISH", "requested", queue .. ":" .. id)

return { queue, id, payload, score, schedule_type, schedule_meta, count, max_times, exclusive }
