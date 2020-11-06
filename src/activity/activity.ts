import { Redis } from "ioredis";
import RedisMock from "ioredis-mock";
import { Closable } from "../Closable";
import { Job } from "../Job";
import { Producer } from "../producer/producer";

export type SubscriptionOptions = { queue?: string; id?: string };
export type OnActivity = (event: OnActivityEvent) => Promise<void> | void;

export type OnActivityEvent =
  | ScheduledEvent
  | DeletedEvent
  | RequestedEvent
  | InvokedEvent
  | RescheduledEvent
  | AcknowledgedEvent;

interface ScheduledEvent {
  type: "scheduled";
  job: Job;
}

interface InvokedEvent {
  type: "invoked";
  id: string;
  queue: string;
}

interface RescheduledEvent {
  type: "rescheduled";
  id: string;
  queue: string;
}

interface DeletedEvent {
  type: "deleted";
  id: string;
  queue: string;
}

interface RequestedEvent {
  type: "requested";
  id: string;
  queue: string;
}

interface AcknowledgedEvent {
  type: "acknowledged";
  id: string;
  queue: string;
}

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

  private async handleMessage(channel: string, message: string) {
    const [_type, ...args] = message.split(":");
    const type = _type as OnActivityEvent["type"];

    const [queue, id] = channel.split(":");

    if (type === "scheduled") {
      const [
        payload,
        runDate,
        schedule_type,
        schedule_meta,
        max_times,
        count,
      ] = args;
      await this.onEvent({
        type: "scheduled",
        job: {
          id,
          queue,
          payload,
          runAt: new Date(+runDate),
          count: Number(count),
          schedule: schedule_type
            ? {
                type: schedule_type,
                meta: schedule_meta,
                times: max_times ? Number(max_times) : undefined,
              }
            : undefined,
        },
      });
    } else {
      await this.onEvent({
        type,
        id,
        queue,
      });
    }
  }

  async close() {
    await this.redis.quit();
    await this.producer.close();
  }
}
