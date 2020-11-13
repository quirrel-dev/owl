import { EventEmitter } from "events";
import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { Job } from "../Job";
import * as fs from "fs";
import * as path from "path";
import type { ScheduleMap } from "../index";
import createDebug from "debug";
import { EggTimer } from "./egg-timer";

const debug = createDebug("owl:worker");

declare module "ioredis" {
  interface Commands {
    request(
      queueKey: string,
      processingKey: string,
      jobTablePrefix: string,
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
          exclusive: "true" | "false"
        ]
      | null
      | number
    >;
    acknowledge(
      jobTableQueueIdKey: string,
      jobTableQueueIndex: string,
      processingKey: string,
      scheduledQueueKey: string,
      id: string,
      queue: string,
      timestampToRescheduleFor: number | undefined
    ): Promise<void>;
  }
}

export type Processor = (job: Job) => Promise<void>;
export type OnError = (job: Job, error: Error) => void;

export class Worker implements Closable {
  private readonly currentlyProcessingJobs: Set<Promise<void>> = new Set();
  private readonly events = new EventEmitter();
  private closing = false;

  private readonly redis;
  private readonly redisSub;

  private readonly eggTimer = new EggTimer(() => this.events.emit("next"));
  private queueIsKnownToBeEmpty = false;

  constructor(
    redisFactory: () => Redis,
    private readonly scheduleMap: ScheduleMap<string>,
    private readonly processor: Processor,
    private readonly onError?: OnError,
    private readonly maximumConcurrency = 100
  ) {
    this.redis = redisFactory();
    this.redisSub = redisFactory();

    this.redis.defineCommand("request", {
      lua: fs.readFileSync(path.join(__dirname, "request.lua")).toString(),
      numberOfKeys: 2,
    });

    this.redis.defineCommand("acknowledge", {
      lua: fs.readFileSync(path.join(__dirname, "acknowledge.lua")).toString(),
      numberOfKeys: 4,
    });

    this.events.on("next", (d) => this.requestNextJobs(d));

    this.redisSub.on("message", (channel) => {
      setImmediate(() => {
        this.queueIsKnownToBeEmpty = false;
        this.events.emit("next", "sub");
      });
    });

    this.redisSub.subscribe("scheduled", "invoked", "rescheduled").then(() => {
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
      "jobs",
      Date.now()
    );

    if (!result) {
      debug("requestNextJobs(): skipped (queue is empty)");
      this.queueIsKnownToBeEmpty = true;
      return;
    }

    if (typeof result === "number") {
      debug("requestNextJobs(): skipped (next job due at %o)", result);
      this.eggTimer.setTimer(result);
      return;
    }

    const currentlyProcessing = (async () => {
      const [
        queue,
        id,
        payload,
        runAtTimestamp,
        schedule_type,
        schedule_meta,
        count,
        max_times,
        exclusive,
      ] = result;
      const runAt = new Date(+runAtTimestamp);

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
      };
      try {
        debug(`requestNextJobs(): job #${id} - started working`);
        await this.processor(job);
        debug(`requestNextJobs(): job #${id} - finished working`);
      } catch (error) {
        debug(`requestNextJobs(): job #${id} - failed`);

        const pipeline = this.redis.pipeline();

        pipeline.publish("fail", `${queue}:${id}:${error}`);
        pipeline.publish(queue, `fail:${id}:${error}`);
        pipeline.publish(`${queue}:${id}`, `fail:${error}`);
        pipeline.publish(`${queue}:${id}:fail`, error);

        await pipeline.exec();

        this.onError?.(job, error);
      } finally {
        let nextExecDate: number | undefined = undefined;

        if (max_times === "" || +count < +max_times) {
          nextExecDate = this.getNextExecutionDate(
            schedule_type,
            schedule_meta,
            runAt
          );
        }

        await this.redis.acknowledge(
          `jobs:${queue}:${id}`,
          `queues:${queue}`,
          "processing",
          "queue",
          id,
          queue,
          nextExecDate
        );
        if (nextExecDate) {
          debug(
            `requestNextJobs(): job #${id} - acknowledged (next execution: ${nextExecDate})`
          );
        } else {
          debug(`requestNextJobs(): job #${id} - acknowledged`);
        }
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
