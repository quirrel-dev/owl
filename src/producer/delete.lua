-- Checks if a specified job exists and deletes it.

local tenantPrefix = KEYS[1]

local jobId = ARGV[1]
local jobQueue = ARGV[2]

local FOUND_AND_DELETED = 0
local NOT_FOUND = 1

local instances = redis.call("ZRANGE", tenantPrefix .. jobQueue .. ":" .. jobId .. ":instances", 0, -1)

if #instances = 0
do
  return NOT_FOUND
end

for i = 1, #instances, 1
do
  redis.call("DEL", tenantPrefix .. "jobs:" .. jobQueue .. ":" .. jobId .. ":" .. instances[i])
  redis.call("ZREM", tenantPrefix .. "queue", jobQueue .. ":" .. jobId .. ":" .. instances[i])
end

redis.call("DEL", tenantPrefix .. jobQueue .. ":" .. jobId .. ":instances")
redis.call("SREM", tenantPrefix .. "queues:" .. jobQueue, jobId)

redis.call("PUBLISH", tenantPrefix .. jobQueue .. ":" .. jobId, "deleted")
redis.call("PUBLISH", tenantPrefix .. "deleted", jobQueue .. ":" .. jobId)

return FOUND_AND_DELETED