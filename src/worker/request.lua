--[[
  Requests a job.
  Moves it to the "processing" set.
  Returns its ID.

  Input:
    KEYS[1] queue
    KEYS[2] processing

    ARGV[1] job table prefix

  Output:
    nil, if no job was found
    if a job was found:
      - queue
      - id
      - payload
]]

local queueAndId = redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
if not queueAndId then
  return nil
end

local queue, id = queueAndId:match("([^,]+):([^,]+)")

local payload = redis.call("HGET", ARGV[1] .. ":" .. queueAndId, "payload")

-- publishes "requested" to "<queue>:<id>"
redis.call("PUBLISH", queue .. ":" .. id, "requested")
-- publishes "requested:<id>" to "<queue>"
redis.call("PUBLISH", queue, "requested" .. ":" .. id)
-- publishes "<queue>:<id>" to "requested"
redis.call("PUBLISH", "requested", queue .. ":" .. id)

return { queue, id, payload }
