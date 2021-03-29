-- Checks if a specified job exists and deletes it.

local tenantPrefix = KEYS[1]

local jobId = ARGV[1]
local jobQueue = ARGV[2]

local FOUND_AND_DELETED = 0
local NOT_FOUND = 1

if redis.call("DEL", tenantPrefix .. "jobs:" .. jobQueue .. ":" .. jobId) == 0 then
  return NOT_FOUND
end

redis.call("SREM", tenantPrefix .. "queues:" .. jobQueue, jobId)
redis.call("ZREM", tenantPrefix .. "queue", jobQueue .. ":" .. jobId)

redis.call("PUBLISH", tenantPrefix .. jobQueue .. ":" .. jobId, "deleted")
redis.call("PUBLISH", tenantPrefix .. "deleted", jobQueue .. ":" .. jobId)

return FOUND_AND_DELETED