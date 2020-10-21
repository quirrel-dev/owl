import { Redis } from "ioredis";
import RedisMock from "ioredis-mock";

export function duplicateRedis(redis: Redis) {
  if (redis instanceof RedisMock) {
    return (redis as any).createConnectedClient()
  } else {
    return (redis as Redis).duplicate();
  }
}