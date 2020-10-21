import { EventEmitter } from "events";
import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { Job } from "../Job";
import * as fs from "fs";
import * as path from "path";
import { duplicateRedis } from "../util/duplicateRedis";

declare module "ioredis" {
  interface Commands {
    request(
      queueKey: string,
      processingKey: string,
      jobTablePrefix: string
    ): Promise<[queue: string, id: string, payload: string] | null>;
    acknowledge(
      jobTableQueueIdKey: string,
      jobTableQueueIndex: string,
      processingKey: string,
      id: string,
      queue: string
    ): Promise<void>;
  }
}

export type Processor = (job: Job) => Promise<void>;
export type OnError = (job: Job, error: Error) => void;

export class Worker implements Closable {
  private readonly currentlyProcessingJobs: Set<Promise<void>> = new Set();
  private readonly events = new EventEmitter();
  private closing = false;

  private readonly redisSub;

  constructor(
    private readonly redis: Redis,
    private readonly processor: Processor,
    private readonly onError?: OnError,
    private readonly maximumConcurrency = 10
  ) {
    this.redis = duplicateRedis(this.redis);
    this.redisSub = duplicateRedis(this.redis);

    this.redis.defineCommand("request", {
      lua: fs.readFileSync(path.join(__dirname, "request.lua")).toString(),
      numberOfKeys: 2,
    });

    this.redis.defineCommand("acknowledge", {
      lua: fs.readFileSync(path.join(__dirname, "acknowledge.lua")).toString(),
      numberOfKeys: 3,
    });

    this.events.on("next", () => this.requestNextJobs());

    this.redisSub.on("message", (channel: string) => {
      if (channel === "enqueued") {
        this.events.emit("next");
      }
    });
    this.redisSub.subscribe("enqueued");

    this.events.emit("next");
  }

  isMaxedOut() {
    return this.currentlyProcessingJobs.size >= this.maximumConcurrency;
  }

  private async requestNextJobs() {
    if (this.isMaxedOut() || this.closing) {
      return;
    }

    const job = await this.redis.request("queue", "processing", "jobs");
    if (!job) {
      return;
    }

    const currentlyProcessing = (async () => {
      const [queue, id, payload] = job;
      try {
        await this.processor({
          queue,
          id,
          payload,
        });
      } catch (error) {
        const pipeline = this.redis.pipeline();

        pipeline.publish("fail", `${queue}:${id}:${error}`);
        pipeline.publish(queue, `fail:${id}:${error}`);
        pipeline.publish(`${queue}:${id}`, `fail:${error}`);
        pipeline.publish(`${queue}:${id}:fail`, error);

        await pipeline.exec();

        this.onError?.({ queue, id, payload }, error);
      } finally {
        await this.redis.acknowledge(
          `jobs:${queue}:${id}`,
          `queues:${queue}`,
          "processing",
          id,
          queue
        );
      }
    })();

    this.currentlyProcessingJobs.add(currentlyProcessing);
    this.events.emit("next");

    await currentlyProcessing;
    this.currentlyProcessingJobs.delete(currentlyProcessing);
    this.events.emit("next");
  }

  public async close() {
    this.closing = true;
    await Promise.all(this.currentlyProcessingJobs);
    await this.redis.quit();
    await this.redisSub.quit();
  }
}
