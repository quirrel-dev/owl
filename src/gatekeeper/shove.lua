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

if redis.call("SISMEMBER", "blocked-queues", queue) == 1 then
  redis.call("ZADD", "blocked:" .. queue, scoreString, id)
  return JOB_FOUND_BUT_BLOCKED
end

local exclusive = redis.call("HGET", "jobs:" .. queueAndId, "exclusive")

if exclusive == "true" then
  redis.call("SADD", "blocked-queues", queue)

  local currentlyExecutingJobs = redis.call("HGET", "soft-block", queue)
  if currentlyExecutingJobs ~= false and currentlyExecutingJobs ~= "0" then
    redis.call("ZADD", "blocked:" .. queue, scoreString, id)
    return JOB_FOUND_BUT_BLOCKED
  end
end

redis.call("HINCRBY", "soft-block", queue, 1)

redis.call("LPUSH", "ready", queueAndId)
redis.call("ZADD", "processing", currentTimestamp, queueAndId)

redis.call("PUBLISH", queue .. ":" .. id, "shoved")
redis.call("PUBLISH", "shoved", queue .. ":" .. id)
