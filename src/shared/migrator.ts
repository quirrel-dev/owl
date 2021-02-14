import { Redis } from "ioredis";

interface Migration {
  name: string;
  run(redis: Redis): Promise<void>;
}

export const processingToSortedSet: Migration = {
  name: "processingToSortedSet",
  async run(redis) {
    await redis.eval(
      `
local members = redis.call("SMEMBERS", "processing")
redis.call("DEL", "processing")

local time = redis.call("TIME")[1]

for i = 1, #members, 1
do
  redis.call("ZADD", "processing", time, members[i])
end
`,
      0
    );
  },
};

const migrations = [processingToSortedSet];

export async function migrate(redis: Redis) {
  const executedMigrations = await redis.smembers("owl-migrations");

  for (const migration of migrations) {
    if (executedMigrations.includes(migration.name)) {
      continue;
    }

    await migration.run(redis);

    await redis.sadd("owl-migrations", migration.name);
  }
}
