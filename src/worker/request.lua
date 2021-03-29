-- Requests a job.
-- Moves it to the "processing" set.
-- Returns its data.

local queueKey = KEYS[1]
local processing = KEYS[2]
local hardBlockedQueues = KEYS[3]
local softBlockCounterHash = KEYS[4]

local jobTablePrefix = ARGV[1]
local currentTimestamp = tonumber(ARGV[2])
local blockedQueuesPrefix = ARGV[3]

local NO_JOB_FOUND = nil
local JOB_FOUND_BUT_BLOCKED = -1

local result = redis.call("ZRANGE", queueKey, 0, 0, "WITHSCORES")
local queueAndId = result[1]
local scoreString = result[2]

if not queueAndId then
  return NO_JOB_FOUND
end

local score = tonumber(scoreString)

if score > currentTimestamp then
  return score
end

local queue, id = queueAndId:match("([^,]+):([^,]+)")

redis.call("ZREM", queueKey, queueAndId)

if redis.call("SISMEMBER", hardBlockedQueues, queue) == 1 then
  redis.call("ZADD", blockedQueuesPrefix .. ":" .. queue, scoreString, id)
  return JOB_FOUND_BUT_BLOCKED
end

local jobData = redis.call(
  "HMGET", jobTablePrefix .. ":" .. queueAndId,
  "payload", "schedule_type", "schedule_meta",
  "count", "max_times", "exclusive", "retry"
)

local payload = jobData[1]
local schedule_type = jobData[2]
local schedule_meta = jobData[3]
local count = jobData[4]
local max_times = jobData[5]
local exclusive = jobData[6]
local retry = jobData[7]

if exclusive == "true" then
  redis.call("SADD", hardBlockedQueues, queue)

  local currentlyExecutingJobs = redis.call("HGET", softBlockCounterHash, queue)
  if currentlyExecutingJobs ~= false and currentlyExecutingJobs ~= "0" then
    redis.call("ZADD", blockedQueuesPrefix .. ":" .. queue, scoreString, id)
    return JOB_FOUND_BUT_BLOCKED
  end
end

redis.call("HINCRBY", softBlockCounterHash, queue, 1)

local time = redis.call("TIME")[1]
redis.call("ZADD", processing, time, queueAndId)

-- publishes "requested" to "<queue>:<id>"
redis.call("PUBLISH", queue .. ":" .. id, "requested")
-- publishes "<queue>:<id>" to "requested"
redis.call("PUBLISH", "requested", queue .. ":" .. id)

return { queue, id, payload, score, schedule_type, schedule_meta, count, max_times, exclusive, retry }
