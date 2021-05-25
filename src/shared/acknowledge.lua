-- Acknowledges a job.
-- Removes it from the "processing" set.

local tenantPrefix = KEYS[1]

local jobId = ARGV[1]
local jobQueue = ARGV[2]
local count = tonumber(ARGV[3])
local retryCount = tonumber(ARGV[4])
local rescheduleFor = ARGV[5]
local retryOn = ARGV[5]

local jobTableJobKey = tenantPrefix .. "jobs:" .. jobQueue .. ":" .. jobId
local instanceKey = count .. "-" .. retryCount

redis.call("ZREM", tenantPrefix .. "processing", jobQueue .. ":" .. jobId .. ":" .. instanceKey)
redis.call("ZREM", jobTableJobKey .. ":instances", instanceKey)

redis.call("PUBLISH", tenantPrefix .. jobQueue .. ":" .. jobId, "acknowledged")
redis.call("PUBLISH", "acknowledged", tenantPrefix .. jobQueue .. ":" .. jobId)

local newInstanceKey = ''
local newTime

if rescheduleFor ~= '' then
  newTime = rescheduleFor
  newInstanceKey = (count + 1) .. ":" .. 1
end

if retryOn ~= '' then
  newTime = retryOn
  newInstanceKey = count .. ":" .. (retryOn + 1)
end

if newInstanceKey == '' then
  redis.call("DEL", jobTableJobKey)
  redis.call("SREM", tenantPrefix .. "queues:" .. jobQueue, jobId)
  -- potentially clean up :instances
else
  redis.call("ZADD", tenantPrefix .. "queue", newTime, jobQueue .. ":" .. jobId .. ":" .. newInstanceKey)
  
  redis.call("PUBLISH", tenantPrefix .. jobQueue .. ":" .. jobId, "rescheduled:" .. newTime)
  redis.call("PUBLISH", tenantPrefix .. "rescheduled", jobQueue .. ":" .. jobId)
end

redis.call("HINCRBY", tenantPrefix .. "soft-block", jobQueue, -1)

local blockedJobsByQueue = tenantPrefix .. "blocked:" .. jobQueue
local blocked = redis.call("ZRANGE", blockedJobsByQueue, 0, -1, "WITHSCORES")

if #blocked > 0 then
  for i = 1, #blocked - 1, 2
  do
    local id = blocked[i]
    local score = blocked[i + 1]

    redis.call("ZADD", tenantPrefix .. "queue", score, jobQueue .. ":" .. id)
  end

  redis.call("DEL", blockedJobsByQueue)
  redis.call("SREM", tenantPrefix .. "blocked-queues", jobQueue)
  redis.call("PUBLISH", tenantPrefix .. "unblocked", jobQueue)
end