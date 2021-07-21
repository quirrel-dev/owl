-- Checks if a specified job exists and invokes it immediately.

local jobId = ARGV[1]
local jobQueue = ARGV[2]
local newRunAt = ARGV[3]

local FOUND_AND_INVOKED = 0
local NOT_FOUND = 1

local updatedJobs = redis.call("ZADD", "queue", "XX", "CH", newRunAt, jobQueue .. ":" .. jobId)

if updatedJobs == 0 then
  return NOT_FOUND
end

redis.call("PUBLISH", jobQueue .. ":" .. jobId, "invoked")
redis.call("PUBLISH", "invoked", jobQueue .. ":" .. jobId)

return FOUND_AND_INVOKED