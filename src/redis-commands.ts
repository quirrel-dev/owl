import type { Redis } from "ioredis";
import * as fs from "fs";
import * as path from "path";

export function defineLocalCommands(redis: Redis, dir: string) {
  const luaFiles = fs.readdirSync(dir).filter((f) => f.endsWith(".lua"));
  for (const luaFile of luaFiles) {
    const script = fs.readFileSync(path.join(dir, luaFile), {
      encoding: "utf-8",
    });
    let numberOfKeys;
    const numberOfKeysMatch = script.match(/KEYS\[(\d+)\]/g);
    if (!numberOfKeysMatch) {
      numberOfKeys = 0;
    } else {
      numberOfKeys = Math.max(
        ...numberOfKeysMatch
          .map((k) => k.slice(k.indexOf("[") + 1, k.indexOf("]")))
          .map((n) => parseInt(n))
      );
    }

    redis.defineCommand(luaFile.replace(".lua", ""), {
      lua: script,
      numberOfKeys,
    });
  }
}
