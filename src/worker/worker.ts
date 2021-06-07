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
import { decodeRedisKey, tenantToRedisPrefix } from "../encodeRedisKey";
import { JobDistributor } from "./job-distributor";
import { defineLocalCommands } from "../redis-commands";
import { scanTenants } from "../shared/scan-tenants";
import * as tracer from "../shared/tracer";
import opentracing, { Span } from "opentracing";

declare module "ioredis" {
  interface Commands {
    request(
      tenantPrefix: string,
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
          exclusive: "true" | "false",
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
  span: Span
) => Promise<void>;

function parseTenantFromChannel(topic: string) {
  if (topic.startsWith("{")) {
    return topic.slice(1, topic.indexOf("}"));
  }

  return "";
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
    private readonly maximumConcurrency = 100
  ) {
    this.redis = redisFactory();
    this.redisSub = redisFactory();

    this.acknowledger = new Acknowledger(this.redis, null as any, onError);

    defineLocalCommands(this.redis, __dirname);
  }

  public async start() {
    await this.listenForPubs();
    this.distributor.start();
  }

  private async listenForPubs() {
    const handleMessage = (channel: string) => {
      setImmediate(() => {
        this.distributor.checkForNewJobs(parseTenantFromChannel(channel));
      });
    };

    this.redisSub.on("pmessage", (_pattern, channel) => {
      handleMessage(channel);
    });

    await this.redisSub.psubscribe(
      "*scheduled",
      "*invoked",
      "*rescheduled",
      "*unblocked"
    );
  }

  private getNextExecutionDate(
    schedule_type: ScheduleType | undefined,
    schedule_meta: string,
    lastExecution: Date
  ): number | undefined {
    if (!schedule_type) {
      return undefined;
    }

    const scheduleFunc = this.scheduleMap[schedule_type];
    if (!scheduleFunc) {
      throw new Error(`Schedule ${schedule_type} not found.`);
    }

    const result = scheduleFunc(lastExecution, schedule_meta);
    if (!result) {
      return undefined;
    }

    return +result;
  }

  private readonly distributor = new JobDistributor(
    () => scanTenants(this.redis),
    tracer.wrap("peek-queue", (span) => async (tenant) => {
      const result = await this.redis.request(
        tenantToRedisPrefix(tenant),
        Date.now()
      );

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
          return [
            "wait",
            new Promise((resolve) => {
              setTimeout(resolve, timeout);
            }),
          ];
        }
      }

      return ["success", result];
    }),
    tracer.wrap("run-job", (span) => async (result, tenant) => {
      const [
        _queue,
        _id,
        payload,
        runAtTimestamp,
        _schedule_type,
        schedule_meta,
        count,
        max_times,
        exclusive,
        retryJSON,
      ] = result;
      const schedule_type = _schedule_type as ScheduleType | undefined;
      const queue = decodeRedisKey(_queue);
      const id = decodeRedisKey(_id);
      const runAt = new Date(+runAtTimestamp);
      const retry = JSON.parse(retryJSON ?? "[]") as number[];

      const job: Job<ScheduleType> = {
        tenant,
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
        tenant,
        jobId: job.id,
        queueId: job.queue,
        timestampForNextRetry: computeTimestampForNextRetry(
          runAt,
          retry,
          +count
        ),
        nextExecutionDate,
      };

      try {
        await this.processor(job, ackDescriptor, span);
        span.setTag("result", "success");
      } catch (error) {
        span.setTag(opentracing.Tags.ERROR, true);
        tracer.logError(span, error);
        await this.acknowledger.reportFailure(ackDescriptor, job, error);
      }
    }),
    this.maximumConcurrency
  );

  public async close() {
    this.distributor.close();
    await this.redis.quit();
    await this.redisSub.quit();
  }
}
