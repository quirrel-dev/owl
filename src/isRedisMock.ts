import { Redis } from "ioredis";

export function isRedisMock(redis: Redis): boolean {
  return "data" in redis;
}
