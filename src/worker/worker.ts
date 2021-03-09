import { EventEmitter } from "events";
import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { Job } from "../Job";
import * as fs from "fs";
import * as path from "path";
import type { ScheduleMap } from "../index";
import createDebug from "debug";
import { EggTimer } from "./egg-timer";
import { computeTimestampForNextRetry } from "./retry";
import {
  AcknowledgementDescriptor,
  Acknowledger,
  OnError,
} from "../shared/acknowledger";
import { decodeRedisKey } from "../encodeRedisKey";

const debug = createDebug("owl:worker");

declare module "ioredis" {
  interface Commands {
    request(
      queueKey: string,
      processingKey: string,
      blockedQueuesKey: string,
      softBlockCounterKey: string,
      jobTablePrefix: string,
      currentTimestamp: number,
      blockedQueuesPrefix: string
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

export class Worker implements Closable {
  private readonly currentlyProcessingJobs: Set<Promise<void>> = new Set();
  private readonly events = new EventEmitter();
  private closing = false;

  private readonly redis;
  private readonly redisSub;

  private readonly eggTimer = new EggTimer(() => this.events.emit("next"));
  private queueIsKnownToBeEmpty = false;

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

    this.redis.defineCommand("request", {
      lua: fs.readFileSync(path.join(__dirname, "request.lua")).toString(),
      numberOfKeys: 4,
    });

    this.events.on("next", (d) => this.requestNextJobs(d));

    this.redisSub.on("message", (channel) => {
      setImmediate(() => {
        this.queueIsKnownToBeEmpty = false;
        this.events.emit("next", "sub");
      });
    });

    this.redisSub
      .subscribe("scheduled", "invoked", "rescheduled", "unblocked")
      .then(() => {
        this.events.emit("next", "init");
      });
  }

  private isMaxedOut() {
    return this.currentlyProcessingJobs.size >= this.maximumConcurrency;
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

  private async requestNextJobs(origin: string = "") {
    debug("requestNextJobs() called", origin);
    if (this.isMaxedOut()) {
      debug("requestNextJobs(): skipped (worker is maxed out)");
      return;
    }
    if (this.closing) {
      debug("requestNextJobs(): skipped (worker is closing)");
      return;
    }

    const result = await this.redis.request(
      "queue",
      "processing",
      "blocked-queues",
      "soft-block",
      "jobs",
      Date.now(),
      "blocked"
    );

    if (!result) {
      debug("requestNextJobs(): skipped (queue is empty)");
      this.queueIsKnownToBeEmpty = true;
      return;
    }

    if (result === -1) {
      debug("requestNextJobs(): job's blocked", result);
      this.events.emit("next");
      return;
    }

    if (typeof result === "number") {
      debug("requestNextJobs(): skipped (next job due at %o)", result);
      this.eggTimer.setTimer(result);
      return;
    }

    const currentlyProcessing = (async () => {
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
        debug(`requestNextJobs(): job #${id} - started working`);

        await this.processor(job, ackDescriptor);
      } catch (error) {
        debug(`requestNextJobs(): job #${id} - found error`);
        await this.acknowledger.reportFailure(ackDescriptor, error);
      }
    })();

    this.currentlyProcessingJobs.add(currentlyProcessing);
    if (!this.queueIsKnownToBeEmpty) {
      this.events.emit("next");
    }

    await currentlyProcessing;
    this.currentlyProcessingJobs.delete(currentlyProcessing);

    if (!this.queueIsKnownToBeEmpty) {
      this.events.emit("next");
    }
  }

  public async close() {
    this.closing = true;
    await Promise.all(this.currentlyProcessingJobs);
    await this.redis.quit();
    await this.redisSub.quit();
  }
}
