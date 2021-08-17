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

declare module "ioredis" {
  interface Commands {
    request(
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
          retry: string | null
        ]
      | null
      | -1
      | number
    >;
  }
}

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
  private readonly redisSub;

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
    this.redisSub = redisFactory();

    this.acknowledger = new Acknowledger(
      this.redis,
      null as any,
      onError,
      this.logger
    );

    defineLocalCommands(this.redis, __dirname);
  }

  public async start() {
    await this.listenForPubs();
    this.distributor.start();
  }

  private async listenForPubs() {
    let throttled = false;

    this.redisSub.on("message", () => {
      if (throttled) {
        return;
      }

      throttled = true;

      setImmediate(() => {
        throttled = false;
        this.logger?.trace("received pub/sub message");
        this.distributor.checkForNewJobs();
      });
    });

    await this.redisSub.subscribe(
      "scheduled",
      "invoked",
      "rescheduled",
      "unblocked"
    );
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
      this.logger?.trace("Peeking into queue");
      const result = await this.redis.request(Date.now());

      if (!result) {
        span.setTag("result", "empty");
        return ["empty"];
      }

      if (result === -1) {
        span.setTag("result", "retry");
        return ["retry"];
      }

      if (typeof result === "number") {
        const timerMaxLimit = 2147483647;
        const timeout = result - Date.now();
        if (timeout > timerMaxLimit) {
          span.setTag("result", "too-long");
          return ["empty"];
        } else {
          span.setTag("result", "wait");
          span.setTag("wait-for", timeout);
          return ["wait", timeout];
        }
      }

      return ["success", result];
    }),
    tracer.wrap("run-job", (span) => async (result) => {
      const [
        _queue,
        _id,
        payload,
        runAtTimestamp,
        _schedule_type,
        schedule_meta,
        count,
        max_times,
        retryJSON,
      ] = result;
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
    await this.redisSub.quit();
  }
}
