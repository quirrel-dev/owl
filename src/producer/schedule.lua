-- Adds the job's data to the job table and schedules it.

local jobTableJobKey = KEYS[1]
local jobTableIndexByQueue = KEYS[2]
local queue = KEYS[3]

local jobId = ARGV[1]
local jobQueue = ARGV[2]
local payload = ARGV[3]
local scheduledExecutionDate = ARGV[4]
local scheduleType = ARGV[5]
local scheduleMeta = ARGV[6]
local maximumExecutionTimes = ARGV[7]
local override = ARGV[8] == "true"
local exclusive = ARGV[9]
local retryIntervals = ARGV[10] -- as JSON array

local SCHEDULED = 0
local ID_ALREADY_EXISTS = 1

if not override then  
  if redis.call("EXISTS", jobTableJobKey) == 1 then
    return ID_ALREADY_EXISTS
  end
end

local count = 1

redis.call(
  "HSET", jobTableJobKey,
    "payload", payload,
    "schedule_type", scheduleType,
    "schedule_meta", scheduleMeta,
    "max_times", maximumExecutionTimes,
    "count", count,
    "exclusive", exclusive,
    "retry", retryIntervals
)

redis.call("SADD", jobTableIndexByQueue, jobId)

redis.call("ZADD", queue, scheduledExecutionDate, jobQueue .. ":" .. jobId)

redis.call("PUBLISH", jobQueue .. ":" .. jobId, "scheduled" .. ":" .. scheduledExecutionDate .. ":" .. scheduleType .. ":" .. scheduleMeta .. ":" .. maximumExecutionTimes .. ":" .. exclusive .. ":" .. count .. ":" .. retryIntervals .. ":" .. payload)
redis.call("PUBLISH", "scheduled", jobQueue .. ":" .. jobId)

return SCHEDULED