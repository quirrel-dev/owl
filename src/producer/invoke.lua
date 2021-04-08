-- Checks if a specified job exists and invokes it immediately.

local tenantPrefix = KEYS[1]

local jobId = ARGV[1]
local jobQueue = ARGV[2]
local newRunAt = ARGV[3]

local FOUND_AND_INVOKED = 0
local NOT_FOUND = 1

local updatedJobs = redis.call("ZADD", tenantPrefix .. "queue", "XX", "CH", newRunAt, jobQueue .. ":" .. jobId)

if updatedJobs == 0 then
  return NOT_FOUND
end

redis.call("PUBLISH", tenantPrefix .. jobQueue .. ":" .. jobId, "invoked")
redis.call("PUBLISH", tenantPrefix .. "invoked", jobQueue .. ":" .. jobId)

return FOUND_AND_INVOKED