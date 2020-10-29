import { Redis } from "ioredis";
import RedisMock from "ioredis-mock";
import { Closable } from "../Closable";
import { Producer } from "../producer/producer";

export type SubscriptionOptions = { queue?: string; id?: string };
export type OnActivity = (
  event: "scheduled" | "deleted" | "requested" | "acknowledged",
  job: { id: string; queue: string }
) => Promise<void> | void;

export class Activity<ScheduleType extends string> implements Closable {
  private redis;
  private producer;

  constructor(
    redisFactory: () => Redis,
    private readonly onEvent: OnActivity,
    options: SubscriptionOptions = {}
  ) {
    this.redis = redisFactory();
    this.producer = new Producer<ScheduleType>(redisFactory);

    if (this.redis instanceof RedisMock) {
      this.redis.on("message", (channel, message) =>
        this.handleMessage(channel, message)
      );
    } else {
      this.redis.on("pmessage", (_pattern, channel, message) =>
        this.handleMessage(channel, message)
      );
    }

    this.redis.psubscribe(`${options.queue ?? "*"}:${options.id ?? "*"}`);
  }

  private async handleMessage(
    channel: string,
    message: "scheduled" | "deleted" | "requested" | "acknowledged"
  ) {
    const [queue, id] = channel.split(":");

    await this.onEvent(message, { id, queue });
  }

  async close() {
    await this.redis.quit();
    await this.producer.close();
  }
}
