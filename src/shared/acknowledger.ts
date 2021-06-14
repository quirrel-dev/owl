import { Pipeline, Redis } from "ioredis";
import { Span } from "opentracing";
import type { Logger } from "pino";
import { encodeRedisKey, tenantToRedisPrefix } from "../encodeRedisKey";
import { Job } from "../Job";
import { Producer } from "../producer/producer";
import { defineLocalCommands } from "../redis-commands";

declare module "ioredis" {
  type AcknowledgeArgs = [
    tenantPrefix: string,
    id: string,
    queue: string,
    timestampToRescheduleFor: number | undefined
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
    private readonly onError?: OnError<ScheduleType>,
    private readonly logger?: Logger
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
    this.logger?.trace(
      { job, descriptor, options },
      "Acknowledger: Starting to report failure"
    );
    if (!job) {
      job = await this.producer.findById(
        descriptor.tenant,
        descriptor.queueId,
        descriptor.jobId
      );

      if (!job) {
        this.logger?.error(
          { descriptor },
          "Acknowledger: Job couldn't be found, but should be here."
        );
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

    const { timestampForNextRetry, queueId, jobId, nextExecutionDate } =
      descriptor;
    const isRetryable = !!timestampForNextRetry;
    const event = isRetryable ? "retry" : "fail";
    const isScheduled = !!nextExecutionDate;

    const errorString = encodeURIComponent(error);

    const _queueId = encodeRedisKey(queueId);
    const _jobId = encodeRedisKey(jobId);

    const prefix = tenantToRedisPrefix(descriptor.tenant);

    pipeline.publish(prefix + event, `${_queueId}:${_jobId}:${errorString}`);
    pipeline.publish(prefix + _queueId, `${event}:${_jobId}:${errorString}`);
    pipeline.publish(
      prefix + `${_queueId}:${_jobId}`,
      `${event}:${errorString}`
    );
    pipeline.publish(prefix + `${_queueId}:${_jobId}:${event}`, errorString);

    const rescheduleFor =
      isScheduled && options.dontReschedule ? undefined : timestampForNextRetry;

    pipeline.acknowledge(prefix, _jobId, _queueId, rescheduleFor);
    this.logger?.trace(
      { descriptor, rescheduleFor },
      "Acknowledger: Job will be reported as failure."
    );

    if (!isRetryable) {
      this.onError?.(descriptor, job, error);

      this.logger?.trace(
        { descriptor },
        "Acknowledger: Job isn't retryable, so it'll be accounted as an error."
      );
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
    options: { dontReschedule?: boolean } = {},
    parentSpan?: Span
  ) {
    const { queueId, jobId, nextExecutionDate, tenant } = descriptor;

    const span = parentSpan
      ?.tracer()
      .startSpan("acknowledge", { childOf: parentSpan });
    span?.addTags(descriptor);
    span?.addTags(options);

    this.logger?.trace(
      { descriptor },
      "Acknowledger: Job will be acknowledged."
    );

    await this.redis.acknowledge(
      tenantToRedisPrefix(tenant),
      encodeRedisKey(jobId),
      encodeRedisKey(queueId),
      options.dontReschedule ? undefined : nextExecutionDate
    );

    this.logger?.trace(
      { descriptor, options },
      "Acknowledger: Job was acknowledged."
    );

    span?.finish();
  }
}
