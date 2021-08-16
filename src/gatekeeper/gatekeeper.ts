import { Closable } from "../Closable";
import { Redis } from "ioredis";
import { defineLocalCommands } from "../redis-commands";

declare module "ioredis" {
  interface Commands {
    shove(
      currentTimestamp: number
    ): Promise<
      | [
          queue: string,
          id: string,
          payload: string,
          runAt: string,
          schedule_type: string,
          schedule_meta: string,
          count: string,
          max_times: string,
          exclusive: "true" | "false",
          retry: string | null
        ]
      | null
      | -1
      | number
    >;
  }
}

export class GateKeeper implements Closable {
  private readonly redis;

  constructor(
    redisFactory: () => Redis,
    private readonly interval: number = 50
  ) {
    this.redis = redisFactory();
    defineLocalCommands(this.redis, __dirname);
  }

  private async check() {
    const result = await this.redis.shove(Date.now());
    const isEmpty = !result;
    if (isEmpty) {
      return;
    }

    const isBlocked = result === -1;
    if (isBlocked) {
      this.check();
      return;
    }

    const isNotYetReady = typeof result === "number";
    if (isNotYetReady) {
      const timerMaxLimit = 2147483647;
      const timeout = result - Date.now();
      if (timeout > timerMaxLimit) {
        return "empty";
      } else {
        return "wait";
      }
    }

    return "shoved";
  }

  public async close() {
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
    }

    await this.redis.quit();
  }

  private intervalHandle: NodeJS.Timeout | null = null;
  public start() {
    this.intervalHandle = setInterval(() => this.check(), this.interval);
  }
}
