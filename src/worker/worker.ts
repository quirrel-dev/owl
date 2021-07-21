import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { Job } from "../Job";
import type { ScheduleMap } from "../index";
import { computeTimestampForNextRetry } from "./retry";
import {
  AcknowledgementDescriptor,
  Acknowledger,
  OnError,
} from "../shared/acknowledger";
import { decodeRedisKey } from "../encodeRedisKey";
import { JobDistributor } from "./job-distributor";
import { defineLocalCommands } from "../redis-commands";
import * as tracer from "../shared/tracer";
import * as opentracing from "opentracing";
import type { Logger } from "pino";

export type Processor<ScheduleType extends string> = (
  job: Readonly<Job<ScheduleType>>,
  ackDescriptor: AcknowledgementDescriptor,
  span: opentracing.Span
) => Promise<void>;

export function getNextExecutionDate<ScheduleType extends string>(
  scheduleMap: ScheduleMap<ScheduleType>,
  schedule_type: ScheduleType | undefined,
  schedule_meta: string,
  lastExecution: Date
): number | undefined {
  if (!schedule_type) {
    return undefined;
  }

  const scheduleFunc = scheduleMap[schedule_type];
  if (!scheduleFunc) {
    throw new Error(`Schedule ${schedule_type} not found.`);
  }

  const result = scheduleFunc(lastExecution, schedule_meta);
  if (!result) {
    return undefined;
  }

  return +result;
}

export class Worker<ScheduleType extends string> implements Closable {
  private readonly redis;

  public readonly acknowledger: Acknowledger<ScheduleType>;

  constructor(
    redisFactory: () => Redis,
    private readonly scheduleMap: ScheduleMap<ScheduleType>,
    private readonly processor: Processor<ScheduleType>,
    onError?: OnError<ScheduleType>,
    private readonly logger?: Logger,
    private readonly maximumConcurrency = 100
  ) {
    this.redis = redisFactory();

    this.acknowledger = new Acknowledger(
      this.redis,
      null as any,
      onError,
      this.logger
    );

    defineLocalCommands(this.redis, __dirname);
  }

  public start() {
    this.distributor.start();
  }

  private getNextExecutionDate(
    schedule_type: ScheduleType | undefined,
    schedule_meta: string,
    lastExecution: Date
  ): number | undefined {
    return getNextExecutionDate(
      this.scheduleMap,
      schedule_type,
      schedule_meta,
      lastExecution
    );
  }

  private readonly distributor = new JobDistributor(
    tracer.wrap("peek-queue", (span) => async () => {
      const queueAndId = await this.redis.brpoplpush(
        "ready",
        "ready-backup",
        1000
      );
      console.log({queueAndId})
      if (!queueAndId) {
        return null;
      }

      const timestamp = await this.redis.zscore("processing", queueAndId);
      if (!timestamp) {
        console.log({ timestamp })
        throw new Error("something is wrong");
      }
      const jobObject = await this.redis.hmget(
        "jobs:" + queueAndId,
        "payload",
        "schedule_type",
        "schedule_meta",
        "count",
        "max_times",
        "exclusive",
        "retry"
      );
      console.log({ jobObject })
      if (!jobObject) {
        throw new Error("missing thing unexpectedly");
      }

      await this.redis.publish(queueAndId, "requested");
      await this.redis.publish("requested", queueAndId);

      return [queueAndId, timestamp, ...(jobObject as string[])];
    }),
    tracer.wrap("run-job", (span) => async (result) => {
      const [
        _queueAndId,
        runAtTimestamp,
        payload,
        _schedule_type,
        schedule_meta,
        count,
        max_times,
        exclusive,
        retryJSON,
      ] = result;
      const [_queue, _id] = _queueAndId.split(":");
      const schedule_type = _schedule_type as ScheduleType | undefined;
      const queue = decodeRedisKey(_queue);
      const id = decodeRedisKey(_id);
      const runAt = new Date(+runAtTimestamp);
      const retry = JSON.parse(retryJSON ?? "[]") as number[];

      const job: Job<ScheduleType> = {
        queue,
        id,
        payload,
        runAt,
        count: +count,
        exclusive: exclusive === "true",
        schedule: schedule_type
          ? {
              type: schedule_type,
              meta: schedule_meta,
              times: max_times ? +max_times : undefined,
            }
          : undefined,
        retry,
      };

      span.addTags({
        ...job,
        payload: undefined,
      });

      let nextExecutionDate: number | undefined = undefined;

      if (max_times === "" || +count < +max_times) {
        nextExecutionDate = this.getNextExecutionDate(
          schedule_type,
          schedule_meta,
          runAt
        );
      }

      const ackDescriptor: AcknowledgementDescriptor = {
        jobId: job.id,
        queueId: job.queue,
        timestampForNextRetry: computeTimestampForNextRetry(
          runAt,
          retry,
          +count
        ),
        nextExecutionDate,
      };

      this.logger?.trace({ job, ackDescriptor }, "Worker: Starting execution");

      try {
        await this.processor(job, ackDescriptor, span);
        span.setTag("result", "success");
        this.logger?.trace(
          { job, ackDescriptor },
          "Worker: Finished execution"
        );
      } catch (error) {
        tracer.logError(span, error);
        this.logger?.trace({ job, ackDescriptor }, "Worker: Execution errored");
        await this.acknowledger.reportFailure(ackDescriptor, job, error);
      }
    }),
    this.logger,
    this.maximumConcurrency
  );

  public async close() {
    this.distributor.close();
    await this.redis.quit();
  }
}
