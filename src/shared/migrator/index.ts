import { Redis } from "ioredis";
import fs from "fs";
import path from "path";

interface Migration {
  name: string;
  run(redis: Redis): Promise<void>;
}

function luaScriptMigration(scriptName: string): Migration {
  return {
    name: scriptName,
    async run(redis) {
      const file = fs
        .readFileSync(path.join(__dirname, `${scriptName}.lua`))
        .toString();
      await redis.eval(file, 0);
    },
  };
}

export const processingToSortedSet = luaScriptMigration(
  "processingToSortedSet"
);

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
