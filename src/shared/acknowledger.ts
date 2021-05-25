import createDebug from "debug";
import { Pipeline, Redis } from "ioredis";
import { encodeRedisKey, tenantToRedisPrefix } from "../encodeRedisKey";
import { Job } from "../Job";
import { Producer } from "../producer/producer";
import { defineLocalCommands } from "../redis-commands";

const debug = createDebug("owl:acknowledger");

declare module "ioredis" {
  type AcknowledgeArgs = [
    tenantPrefix: string,
    id: string,
    queue: string,
    count: number,
    retryCount: number,
    timestampToRescheduleFor: number | undefined,
    timestampToRetryOn: number | undefined
  ];
  interface Commands {
    acknowledge(...args: AcknowledgeArgs): Promise<void>;
  }

  interface Pipeline {
    acknowledge(...args: AcknowledgeArgs): this;
  }
}

export interface AcknowledgementDescriptor {
  tenant: string;
  queueId: string;
  jobId: string;
  count: number;
  retryCount: number;
  timestampForNextRetry?: number;
  nextExecutionDate?: number;
}

export type OnError<ScheduleType extends string> = (
  ack: AcknowledgementDescriptor,
  job: Job<ScheduleType>,
  error: any
) => void;

export class Acknowledger<ScheduleType extends string> {
  constructor(
    private readonly redis: Redis,
    private readonly producer: Producer<ScheduleType>,
    private readonly onError?: OnError<ScheduleType>
  ) {
    defineLocalCommands(this.redis, __dirname);
  }

  public async _reportFailure(
    descriptor: AcknowledgementDescriptor,
    job: Job<ScheduleType> | null,
    error: any,
    pipeline: Pipeline,
    options: { dontReschedule?: boolean } = {}
  ) {
    if (!job) {
      job = await this.producer.findById(
        descriptor.tenant,
        descriptor.queueId,
        descriptor.jobId
      );

      if (!job) {
        console.error("Job couldn't be found, but should be here.");
        job = {
          id: descriptor.jobId,
          queue: descriptor.queueId,
          tenant: descriptor.tenant,
          count: 1,
          exclusive: false,
          payload: "ERROR: Job couldn't be found",
          retry: [],
          runAt: new Date(0),
        };
      }
    }

    const {
      timestampForNextRetry,
      queueId,
      jobId,
      count,
      retryCount,
      tenant,
      nextExecutionDate,
    } = descriptor;
    const isRetryable = !!timestampForNextRetry;
    const event = isRetryable ? "retry" : "fail";
    const isScheduled = !!nextExecutionDate;

    const errorString = encodeURIComponent(error);

    const _queueId = encodeRedisKey(queueId);
    const _jobId = encodeRedisKey(jobId);

    const prefix = tenantToRedisPrefix(tenant);

    pipeline.publish(prefix + event, `${_queueId}:${_jobId}:${errorString}`);
    pipeline.publish(prefix + _queueId, `${event}:${_jobId}:${errorString}`);
    pipeline.publish(
      prefix + `${_queueId}:${_jobId}`,
      `${event}:${errorString}`
    );
    pipeline.publish(prefix + `${_queueId}:${_jobId}:${event}`, errorString);

    pipeline.acknowledge(
      prefix,
      _jobId,
      _queueId,
      count,
      retryCount,
      undefined,
      timestampForNextRetry
    );

    if (!isRetryable) {
      this.onError?.(descriptor, job, error);
    }
  }

  public async reportFailure(
    descriptor: AcknowledgementDescriptor,
    job: Job<ScheduleType> | null,
    error: any,
    options: { dontReschedule?: boolean } = {}
  ) {
    const pipeline = this.redis.pipeline();
    await this._reportFailure(descriptor, job, error, pipeline, options);
    await pipeline.exec();
  }

  public async acknowledge(
    descriptor: AcknowledgementDescriptor,
    options: { dontReschedule?: boolean } = {}
  ) {
    const { queueId, jobId, nextExecutionDate, tenant, timestampForNextRetry, count, retryCount } = descriptor;

    await this.redis.acknowledge(
      tenantToRedisPrefix(tenant),
      encodeRedisKey(jobId),
      encodeRedisKey(queueId),
      count,
      retryCount,
      nextExecutionDate,
      timestampForNextRetry
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
