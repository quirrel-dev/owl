import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { Job } from "../Job";
import { Producer } from "../producer/producer";

export type SubscriptionOptions = { queue?: string; id?: string };
export type OnActivity<ScheduleType extends string> = (
  event: "scheduled" | "deleted" | "requested" | "acknowledged",
  job: Job<ScheduleType>
) => Promise<void> | void;

export class Activity<ScheduleType extends string> implements Closable {
  private redis;
  private producer;

  constructor(
    redisFactory: () => Redis,
    private readonly onEvent: OnActivity<ScheduleType>,
    options: SubscriptionOptions
  ) {
    this.redis = redisFactory();
    this.producer = new Producer<ScheduleType>(redisFactory);

    this.redis.on("message", (channel, message) =>
      this.handleMessage(channel, message)
    );

    this.redis.psubscribe(`${options.queue ?? "*"}:${options.id ?? "*"}`);
  }

  private async handleMessage(
    channel: string,
    message: "scheduled" | "deleted" | "requested" | "acknowledged"
  ) {
    const [queue, id] = channel.split(":");

    const job = await this.producer.findById(queue, id);
    await this.onEvent(message, job ?? ({ queue, id } as Job<ScheduleType>));
  }

  async close() {
    await this.redis.quit();
    await this.producer.close();
  }
}
