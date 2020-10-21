import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { JobEnqueue, JobSchedule } from "../Job";
import * as fs from "fs";
import * as path from "path";
import { duplicateRedis } from "../util/duplicateRedis";

declare module "ioredis" {
  interface Commands {
    enqueue(
      jobTableJobId: string,
      jobTableIndexByQueue: string,
      queueKey: string,
      id: string,
      queue: string,
      payload: string
    ): Promise<void>;
  }
}

export class Producer<ScheduleType extends string> implements Closable {
  constructor(private readonly redis: Redis) {
    this.redis = duplicateRedis(this.redis);

    this.redis.defineCommand("enqueue", {
      lua: fs.readFileSync(path.join(__dirname, "enqueue.lua")).toString(),
      numberOfKeys: 3,
    });
  }

  public async enqueue(job: JobEnqueue) {
    await this.redis.enqueue(
      `jobs:${job.queue}:${job.id}`,
      `queues:${job.queue}`,
      "queue",
      job.id,
      job.queue,
      job.payload
    );
  }

  public async schedule(job: JobSchedule<ScheduleType>) {
    await this.redis.enqueue(
      `jobs:${job.queue}:${job.id}`,
      `queues:${job.queue}`,
      "queue",
      job.id,
      job.queue,
      job.payload
    );
  }

  async close() {
    await this.redis.quit();
  }
}
