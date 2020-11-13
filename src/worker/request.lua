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

redis.call("SADD", KEYS[2], queueAndId)
redis.call("ZREM", KEYS[1], queueAndId)
local queue, id = queueAndId:match("([^,]+):([^,]+)")

local jobData = redis.call("HMGET", ARGV[1] .. ":" .. queueAndId, "payload", "schedule_type", "schedule_meta", "count", "max_times", "exclusive")

local payload = jobData[1]
local schedule_type = jobData[2]
local schedule_meta = jobData[3]
local count = jobData[4]
local max_times = jobData[5]
local exclusive = jobData[6]

-- publishes "requested" to "<queue>:<id>"
redis.call("PUBLISH", queue .. ":" .. id, "requested")
-- publishes "<queue>:<id>" to "requested"
redis.call("PUBLISH", "requested", queue .. ":" .. id)

return { queue, id, payload, score, schedule_type, schedule_meta, count, max_times, exclusive }
