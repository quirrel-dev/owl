-- Checks if a specified job exists and deletes it.

local jobTableJobKey = KEYS[1]
local jobTableIndexByQueue = KEYS[2]
local queue = KEYS[3]

local jobId = ARGV[1]
local jobQueue = ARGV[2]

local FOUND_AND_DELETED = 0
local NOT_FOUND = 1

if redis.call("DEL", jobTableJobKey) == 0 then
  return NOT_FOUND
end

redis.call("SREM", jobTableIndexByQueue, jobId)
redis.call("ZREM", queue, jobQueue .. ":" .. jobId)

redis.call("PUBLISH", jobQueue .. ":" .. jobId, "deleted")
redis.call("PUBLISH", "deleted", jobQueue .. ":" .. jobId)

return FOUND_AND_DELETED