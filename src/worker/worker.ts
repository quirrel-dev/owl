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
import RedisMock from "ioredis-mock";

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

export type Processor = (
  job: Readonly<Job>,
  ackDescriptor: AcknowledgementDescriptor
) => Promise<void>;

function parseTenantFromChannel(topic: string) {
  if (topic.startsWith("{")) {
    return topic.slice(1, topic.indexOf("}"));
  }

  return topic;
}

export class Worker implements Closable {
  private readonly redis;
  private readonly redisSub;

  public readonly acknowledger: Acknowledger;

  constructor(
    redisFactory: () => Redis,
    private readonly scheduleMap: ScheduleMap<string>,
    private readonly processor: Processor,
    onError?: OnError,
    private readonly maximumConcurrency = 100
  ) {
    this.redis = redisFactory();
    this.redisSub = redisFactory();

    this.acknowledger = new Acknowledger(this.redis, onError);

    defineLocalCommands(this.redis, __dirname);

    this.listenForPubs();

    this.distributor.start();
  }

  private listenForPubs() {
    const handleMessage = (channel: string) => {
      setImmediate(() => {
        this.distributor.checkForNewJobs(parseTenantFromChannel(channel));
      });
    };

    if (this.redisSub instanceof RedisMock) {
      this.redisSub.on("message", (channel) => {
        handleMessage(channel);
      });
    } else {
      this.redisSub.on("pmessage", (_pattern, channel) => {
        handleMessage(channel);
      });
    }

    this.redisSub.psubscribe(
      "*scheduled",
      "*invoked",
      "*rescheduled",
      "*unblocked"
    );
  }

  private getNextExecutionDate(
    schedule_type: string,
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
    async () => [""],
    async (tenant) => {
      const result = await this.redis.request(
        tenantToRedisPrefix(tenant),
        Date.now()
      );

      if (!result) {
        return ["empty"];
      }

      if (result === -1) {
        return ["retry"];
      }

      if (typeof result === "number") {
        const timerMaxLimit = 2147483647;
        const timeout = result - Date.now();
        if (timeout > timerMaxLimit) {
          return ["empty"];
        } else {
          return [
            "wait",
            new Promise((resolve) => {
              setTimeout(resolve, timeout);
            }),
          ];
        }
      }

      return ["success", result];
    },
    async (result, tenant) => {
      const [
        _queue,
        _id,
        payload,
        runAtTimestamp,
        schedule_type,
        schedule_meta,
        count,
        max_times,
        exclusive,
        retryJSON,
      ] = result;
      const queue = decodeRedisKey(_queue);
      const id = decodeRedisKey(_id);
      const runAt = new Date(+runAtTimestamp);
      const retry = JSON.parse(retryJSON ?? "[]") as number[];

      const job: Job = {
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
        await this.processor(job, ackDescriptor);
      } catch (error) {
        await this.acknowledger.reportFailure(ackDescriptor, error);
      }
    },
    this.maximumConcurrency
  );

  public async close() {
    this.distributor.close();
    await this.redis.quit();
    await this.redisSub.quit();
  }
}
