-- Acknowledges a job.
-- Removes it from the "processing" set.

local jobTableJobKey = KEYS[1]
local jobIndexByQueue = KEYS[2]
local processingSet = KEYS[3]
local queue = KEYS[4]
local blockedJobs = KEYS[5]
local blockedQueues = KEYS[6]
local softBlockCounterHash = KEYS[7]

local jobId = ARGV[1]
local jobQueue = ARGV[2]
local rescheduleFor = ARGV[3]

redis.call("ZREM", processingSet, jobQueue .. ":" .. jobId)

redis.call("PUBLISH", jobQueue .. ":" .. jobId, "acknowledged")
redis.call("PUBLISH", "acknowledged", jobQueue .. ":" .. jobId)

if rescheduleFor == '' then
  redis.call("DEL", jobTableJobKey)
  redis.call("SREM", jobIndexByQueue, jobId)
else
  redis.call("ZADD", queue, rescheduleFor, jobQueue .. ":" .. jobId)

  redis.call("HINCRBY", jobTableJobKey, "count", 1)
  
  redis.call("PUBLISH", jobQueue .. ":" .. jobId, "rescheduled:" .. rescheduleFor)
  redis.call("PUBLISH", "rescheduled", jobQueue .. ":" .. jobId)
end

redis.call("HINCRBY", softBlockCounterHash, jobQueue, -1)

local blocked = redis.call("ZRANGE", blockedJobs, 0, -1, "WITHSCORES")

if #blocked > 0 then
  for i = 1, #blocked - 1, 2
  do
    local id = blocked[i]
    local score = blocked[i + 1]

    redis.call("ZADD", queue, score, jobQueue .. ":" .. id)
  end

  redis.call("DEL", blockedJobs)
  redis.call("SREM", blockedQueues, jobQueue)
  redis.call("PUBLISH", "unblocked", jobQueue)
end