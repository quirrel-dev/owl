-- Acknowledges a job.
-- Removes it from the "processing" set.

local jobId = ARGV[1]
local jobQueue = ARGV[2]
local rescheduleFor = ARGV[3]

local jobTableJobKey = "jobs:" .. jobQueue .. ":" .. jobId

redis.call("ZREM", "processing", jobQueue .. ":" .. jobId)

redis.call("PUBLISH", jobQueue .. ":" .. jobId, "acknowledged")
redis.call("PUBLISH", "acknowledged", jobQueue .. ":" .. jobId)

if rescheduleFor == '' then
  redis.call("DEL", jobTableJobKey)
  redis.call("SREM", "queues:" .. jobQueue, jobId)
else
  redis.call("ZADD", "queue", rescheduleFor, jobQueue .. ":" .. jobId)

  redis.call("HINCRBY", jobTableJobKey, "count", 1)
  
  redis.call("PUBLISH", jobQueue .. ":" .. jobId, "rescheduled:" .. rescheduleFor)
  redis.call("PUBLISH", "rescheduled", jobQueue .. ":" .. jobId)
end

redis.call("HINCRBY", "soft-block", jobQueue, -1)
redis.call("SREM", "blocked-queues", jobQueue)

local blockedJobsByQueue = "blocked:" .. jobQueue
local blocked = redis.call("ZRANGE", blockedJobsByQueue, 0, -1, "WITHSCORES")

if #blocked > 0 then
  for i = 1, #blocked - 1, 2
  do
    local id = blocked[i]
    local score = blocked[i + 1]

    redis.call("ZADD", "queue", score, jobQueue .. ":" .. id)
  end

  redis.call("DEL", blockedJobsByQueue)
  redis.call("PUBLISH", "unblocked", jobQueue)
end