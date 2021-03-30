-- Checks if a specified job exists and deletes it.

local jobId = ARGV[1]
local jobQueue = ARGV[2]

local FOUND_AND_DELETED = 0
local NOT_FOUND = 1

if redis.call("DEL", "jobs:" .. jobQueue .. ":" .. jobId) == 0 then
  return NOT_FOUND
end

redis.call("SREM", "queues:" .. jobQueue, jobId)
redis.call("ZREM", "queue", jobQueue .. ":" .. jobId)

redis.call("PUBLISH", jobQueue .. ":" .. jobId, "deleted")
redis.call("PUBLISH", "deleted", jobQueue .. ":" .. jobId)

return FOUND_AND_DELETED