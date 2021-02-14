import createDebug from "debug";
import { Redis } from "ioredis";
import * as fs from "fs";
import * as path from "path";

const debug = createDebug("owl:acknowledger");

declare module "ioredis" {
  interface Commands {
    acknowledge(
      jobTableQueueIdKey: string,
      jobTableQueueIndex: string,
      processingKey: string,
      scheduledQueueKey: string,
      blockedJobsKey: string,
      blockedQueuesSetKey: string,
      softBlockCounterKey: string,
      id: string,
      queue: string,
      timestampToRescheduleFor: number | undefined
    ): Promise<void>;
  }
}

export interface AcknowledgementDescriptor {
  queueId: string;
  jobId: string;
  timestampForNextRetry?: number;
  nextExecutionDate?: number;
}

export type OnError = (job: AcknowledgementDescriptor, error: Error) => void;

export class Acknowledger {
  constructor(
    private readonly redis: Redis,
    private readonly onError?: OnError
  ) {
    this.redis.defineCommand("acknowledge", {
      lua: fs.readFileSync(path.join(__dirname, "acknowledge.lua")).toString(),
      numberOfKeys: 7,
    });
  }

  public async reportFailure(
    descriptor: AcknowledgementDescriptor,
    error: any
  ) {
    const { timestampForNextRetry, queueId, jobId } = descriptor;
    const isRetryable = !!timestampForNextRetry;
    const event = isRetryable ? "retry" : "fail";
    const pipeline = this.redis.pipeline();

    const errorString = encodeURIComponent(error);

    pipeline.publish(event, `${queueId}:${jobId}:${errorString}`);
    pipeline.publish(queueId, `${event}:${jobId}:${errorString}`);
    pipeline.publish(`${queueId}:${jobId}`, `${event}:${errorString}`);
    pipeline.publish(`${queueId}:${jobId}:${event}`, errorString);

    await pipeline.exec();

    if (!isRetryable) {
      this.onError?.(descriptor, error);
    }

    await this.redis.acknowledge(
      `jobs:${queueId}:${jobId}`,
      `queues:${queueId}`,
      "processing",
      "queue",
      `blocked:${queueId}`,
      "blocked-queues",
      "soft-block",
      jobId,
      queueId,
      timestampForNextRetry
    );
  }

  public async acknowledge(
    descriptor: AcknowledgementDescriptor,
    options: { dontReschedule?: boolean } = {}
  ) {
    const { queueId, jobId, nextExecutionDate } = descriptor;

    await this.redis.acknowledge(
      `jobs:${queueId}:${jobId}`,
      `queues:${queueId}`,
      "processing",
      "queue",
      `blocked:${queueId}`,
      "blocked-queues",
      "soft-block",
      jobId,
      queueId,
      options.dontReschedule ? undefined : nextExecutionDate
    );
    if (nextExecutionDate) {
      debug(
        `requestNextJobs(): job #${jobId} - acknowledged (next execution: ${nextExecutionDate})`
      );
    } else {
      debug(`requestNextJobs(): job #${jobId} - acknowledged`);
    }
  }
}
