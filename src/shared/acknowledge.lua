-- Acknowledges a job.
-- Removes it from the "processing" set.

local tenantPrefix = KEYS[1]

local jobId = ARGV[1]
local jobQueue = ARGV[2]
local rescheduleFor = ARGV[3]

local jobTableJobKey = tenantPrefix .. "jobs:" .. jobQueue .. ":" .. jobId

redis.call("ZREM", tenantPrefix .. "processing", jobQueue .. ":" .. jobId)

redis.call("PUBLISH", tenantPrefix .. jobQueue .. ":" .. jobId, "acknowledged")
redis.call("PUBLISH", "acknowledged", tenantPrefix .. jobQueue .. ":" .. jobId)

if rescheduleFor == '' then
  redis.call("DEL", jobTableJobKey)
  redis.call("SREM", tenantPrefix .. "queues:" .. jobQueue, jobId)
else
  redis.call("ZADD", tenantPrefix .. "queue", rescheduleFor, jobQueue .. ":" .. jobId)

  redis.call("HINCRBY", jobTableJobKey, "count", 1)
  
  redis.call("PUBLISH", tenantPrefix .. jobQueue .. ":" .. jobId, "rescheduled:" .. rescheduleFor)
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