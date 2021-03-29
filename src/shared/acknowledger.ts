import createDebug from "debug";
import { Pipeline, Redis } from "ioredis";
import { encodeRedisKey } from "../encodeRedisKey";
import { defineLocalCommands } from "../redis-commands";

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

  interface Pipeline {
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
    ): this;
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
    defineLocalCommands(this.redis, __dirname);
  }

  public _reportFailure(
    descriptor: AcknowledgementDescriptor,
    error: any,
    pipeline: Pipeline
  ) {
    const { timestampForNextRetry, queueId, jobId } = descriptor;
    const isRetryable = !!timestampForNextRetry;
    const event = isRetryable ? "retry" : "fail";

    const errorString = encodeURIComponent(error);

    const _queueId = encodeRedisKey(queueId);
    const _jobId = encodeRedisKey(jobId);

    pipeline.publish(event, `${_queueId}:${_jobId}:${errorString}`);
    pipeline.publish(_queueId, `${event}:${_jobId}:${errorString}`);
    pipeline.publish(`${_queueId}:${_jobId}`, `${event}:${errorString}`);
    pipeline.publish(`${_queueId}:${_jobId}:${event}`, errorString);

    pipeline.acknowledge(
      `jobs:${_queueId}:${_jobId}`,
      `queues:${_queueId}`,
      "processing",
      "queue",
      `blocked:${_queueId}`,
      "blocked-queues",
      "soft-block",
      _jobId,
      _queueId,
      timestampForNextRetry
    );

    if (!isRetryable) {
      this.onError?.(descriptor, error);
    }
  }

  public async reportFailure(
    descriptor: AcknowledgementDescriptor,
    error: any
  ) {
    const pipeline = this.redis.pipeline();
    this._reportFailure(descriptor, error, pipeline);
    await pipeline.exec();
  }

  public async acknowledge(
    descriptor: AcknowledgementDescriptor,
    options: { dontReschedule?: boolean } = {}
  ) {
    const { queueId, jobId, nextExecutionDate } = descriptor;

    await this.redis.acknowledge(
      `jobs:${encodeRedisKey(queueId)}:${encodeRedisKey(jobId)}`,
      `queues:${encodeRedisKey(queueId)}`,
      "processing",
      "queue",
      `blocked:${encodeRedisKey(queueId)}`,
      "blocked-queues",
      "soft-block",
      encodeRedisKey(jobId),
      encodeRedisKey(queueId),
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
