-- Requests a job.
-- Moves it to the "processing" set.
-- Returns its data.

local currentTimestamp = tonumber(ARGV[1])

local NO_JOB_FOUND = nil
local JOB_FOUND_BUT_BLOCKED = -1

local result = redis.call("ZRANGE", "queue", 0, 0, "WITHSCORES")
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

local function ends_with(str, ending)
  return str:sub(-#ending) == ending
end
local exclusive = ends_with(queue, "exclusive")

redis.call("ZREM", "queue", queueAndId)

if exclusive and redis.call("SISMEMBER", "blocked-queues", queue) == 1 then
  redis.call("ZADD", "blocked:" .. queue, scoreString, id)
  return JOB_FOUND_BUT_BLOCKED
end

local jobData = redis.call(
  "HMGET", "jobs:" .. queueAndId,
  "payload", "schedule_type", "schedule_meta",
  "count", "max_times", "retry"
)

local payload = jobData[1]
local schedule_type = jobData[2]
local schedule_meta = jobData[3]
local count = jobData[4]
local max_times = jobData[5]
local retry = jobData[6]

if exclusive then
  redis.call("SADD", "blocked-queues", queue)

  local currentlyExecutingJobs = redis.call("HGET", "soft-block", queue)
  if currentlyExecutingJobs ~= false and currentlyExecutingJobs ~= "0" then
    redis.call("ZADD", "blocked:" .. queue, scoreString, id)
    return JOB_FOUND_BUT_BLOCKED
  end
end

redis.call("HINCRBY", "soft-block", queue, 1)

redis.call("ZADD", "processing", currentTimestamp, queueAndId)

redis.call("PUBLISH", queue .. ":" .. id, "requested")
redis.call("PUBLISH", "requested", queue .. ":" .. id)

return { queue, id, payload, score, schedule_type, schedule_meta, count, max_times, retry }
