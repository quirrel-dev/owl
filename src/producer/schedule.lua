-- Adds the job's data to the job table and schedules it.

local tenantPrefix = KEYS[1]

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

local jobTableJobKey = tenantPrefix .. "jobs:" .. jobQueue .. ":" .. jobId

if not override then  
  if redis.call("EXISTS", jobTableJobKey) == 1 then
    return ID_ALREADY_EXISTS
  end
end

local count = 1
local try = 1

local instanceKey = count .. "-" .. try

redis.call(
  "HSET", jobTableJobKey,
    "payload", payload,
    "schedule_type", scheduleType,
    "schedule_meta", scheduleMeta,
    "max_times", maximumExecutionTimes,
    "count", count, -- deprecated
    "exclusive", exclusive,
    "retry", retryIntervals
)

redis.call("SADD", tenantPrefix .. "queues:" .. jobQueue, jobId)

redis.call("ZADD", jobTableJobKey .. ":instances", 0, instanceKey)
redis.call("ZADD", tenantPrefix .. "queue", scheduledExecutionDate, jobQueue .. ":" .. jobId .. ":" .. instanceKey)

redis.call("PUBLISH", tenantPrefix .. jobQueue .. ":" .. jobId, "scheduled" .. ":" .. scheduledExecutionDate .. ":" .. scheduleType .. ":" .. scheduleMeta .. ":" .. maximumExecutionTimes .. ":" .. exclusive .. ":" .. instanceKey .. ":" .. retryIntervals .. ":" .. payload)
redis.call("PUBLISH", tenantPrefix .. "scheduled", jobQueue .. ":" .. jobId .. ":" .. instanceKey)

return SCHEDULED