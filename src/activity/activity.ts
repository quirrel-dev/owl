import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { decodeRedisKey, encodeRedisKey } from "../encodeRedisKey";
import { Job } from "../Job";
import minimatch from "minimatch";

/**
 * Like String.split, but has a maximum number of delimiters it picks up.
 * @param message
 * @param maxParts
 * @param delimiter
 */
function splitEvent(message: string, maxParts: number, delimiter = ":") {
  const result: string[] = [];
  let currentOne = "";

  for (let i = 0; i < message.length; i++) {
    const char = message[i];
    if (char === delimiter) {
      if (result.length === maxParts - 1) {
        result.push(currentOne + message.slice(i));
        return result;
      } else {
        result.push(currentOne);
        currentOne = "";
      }
    } else {
      currentOne += char;
    }
  }

  result.push(currentOne);
  return result;
}

export type SubscriptionOptions = { queue?: string; id?: string };
export type OnActivity<ScheduleType extends string> = (
  event: OnActivityEvent<ScheduleType>
) => Promise<void> | void;

export type OnActivityEvent<ScheduleType extends string> =
  | ScheduledEvent<ScheduleType>
  | DeletedEvent
  | RequestedEvent
  | InvokedEvent
  | RescheduledEvent
  | AcknowledgedEvent;

interface ScheduledEvent<ScheduleType extends string> {
  type: "scheduled";
  job: Job<ScheduleType>;
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
  runAt: Date;
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
  private readonly pattern: string;
  constructor(
    private readonly redis: Redis,
    private readonly onEvent: OnActivity<ScheduleType>,
    options: SubscriptionOptions = {}
  ) {
    this.redis.on("pmessage", (_pattern, channel, message) =>
      this.handleMessage(channel, message)
    );

    const queue = options.queue ? encodeRedisKey(options.queue) : "*";
    const id = options.id ? encodeRedisKey(options.id) : "*";
    this.pattern = `${queue}:${id}`;

    this.redis.psubscribe(this.pattern);
  }

  private matchesPattern(channel: string) {
    return minimatch(channel, this.pattern);
  }

  private async handleMessage(channel: string, message: string) {
    if (!this.matchesPattern(channel)) {
      return;
    }

    const [_type, ...args] = splitEvent(message, 9);
    const type = _type as OnActivityEvent<ScheduleType>["type"];

    const channelParts = channel.split(":").map(decodeRedisKey);
    if (channelParts.length !== 2) {
      return;
    }

    const [queue, id] = channelParts;

    if (type === "scheduled") {
      const [
        runDate,
        schedule_type,
        schedule_meta,
        max_times,
        exclusive,
        count,
        retryJson,
        payload,
      ] = args;
      await this.onEvent({
        type: "scheduled",
        job: {
          id,
          queue,
          payload,
          runAt: new Date(+runDate),
          count: Number(count),
          exclusive: exclusive === "true",
          retry: JSON.parse(retryJson),
          schedule: schedule_type
            ? {
                type: schedule_type as ScheduleType,
                meta: schedule_meta,
                times: max_times ? Number(max_times) : undefined,
              }
            : undefined,
        },
      });
    } else if (type === "rescheduled") {
      const [runDate] = args;
      await this.onEvent({
        type: "rescheduled",
        id,
        queue,
        runAt: new Date(+runDate),
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
    await this.redis.punsubscribe(this.pattern);
  }
}
