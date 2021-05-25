-- Requests a job.
-- Moves it to the "processing" set.
-- Returns its data.

local tenantPrefix = KEYS[1]

local currentTimestamp = tonumber(ARGV[1])

local NO_JOB_FOUND = nil
local JOB_FOUND_BUT_BLOCKED = -1

local result = redis.call("ZRANGE", tenantPrefix .. "queue", 0, 0, "WITHSCORES")
local queueAndIdAndInstanceKey = result[1]
local scoreString = result[2]

if not queueAndIdAndInstanceKey then
  return NO_JOB_FOUND
end

local score = tonumber(scoreString)

if score > currentTimestamp then
  return score
end

local queue, id, instanceKey = queueAndIdAndInstanceKey:match("(.+):(.+):(.+)")
local count, retryCount = instanceKey:match("(.+)-(.+)")

redis.call("ZREM", tenantPrefix .. "queue", queueAndIdAndInstanceKey)

if redis.call("SISMEMBER", tenantPrefix .. "blocked-queues", queue) == 1 then
  redis.call("ZADD", tenantPrefix .. "blocked:" .. queue, scoreString, id .. ":" .. instanceKey)
  return JOB_FOUND_BUT_BLOCKED
end

local jobData = redis.call(
  "HMGET", tenantPrefix .. "jobs:" .. queue .. ":" .. id,
  "payload", "schedule_type", "schedule_meta",
  "max_times", "exclusive", "retry"
)

local payload = jobData[1]
local schedule_type = jobData[2]
local schedule_meta = jobData[3]
local max_times = jobData[4]
local exclusive = jobData[5]
local retry = jobData[6]

if exclusive == "true" then
  redis.call("SADD", tenantPrefix .. "blocked-queues", queue)

  local currentlyExecutingJobs = redis.call("HGET", tenantPrefix .. "soft-block", queue)
  if currentlyExecutingJobs ~= false and currentlyExecutingJobs ~= "0" then
    redis.call("ZADD", tenantPrefix .. "blocked:" .. queue, scoreString, id .. ":" .. instanceKey)
    return JOB_FOUND_BUT_BLOCKED
  end
end

redis.call("HINCRBY", tenantPrefix .. "soft-block", queue, 1)

redis.call("ZADD", tenantPrefix .. "processing", currentTimestamp, queueAndIdAndInstanceKey)

redis.call("PUBLISH", tenantPrefix .. queue .. ":" .. id, "requested")
redis.call("PUBLISH", tenantPrefix .. "requested", queue .. ":" .. id)

return { queue, id, payload, score, schedule_type, schedule_meta, count, max_times, exclusive, retry, retryCount }
