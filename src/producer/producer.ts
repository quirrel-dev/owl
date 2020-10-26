import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { JobEnqueue } from "../Job";
import * as fs from "fs";
import * as path from "path";
import createDebug from "debug";

const debug = createDebug("owl:producer");

declare module "ioredis" {
  interface Commands {
    schedule(
      jobTableJobId: string,
      jobTableIndexByQueue: string,
      queueKey: string,
      id: string,
      queue: string,
      payload: string,
      executionDate: number,
      schedule_type: string | undefined,
      schedule_meta: string | undefined,
      useUpsertSemantics: boolean
    ): Promise<0 | 1>;
  }
}

export class Producer<ScheduleType extends string> implements Closable {
  private readonly redis;
  constructor(redisFactory: () => Redis) {
    this.redis = redisFactory();

    this.redis.defineCommand("schedule", {
      lua: fs.readFileSync(path.join(__dirname, "schedule.lua")).toString(),
      numberOfKeys: 3,
    });
  }

  public async enqueue(job: JobEnqueue<ScheduleType>) {
    debug("job #%o: enqueueing", job.id);
    await this.redis.schedule(
      `jobs:${job.queue}:${job.id}`,
      `queues:${job.queue}`,
      "queue",
      job.id,
      job.queue,
      job.payload,
      job.runAt ? +job.runAt : 0,
      job.schedule?.type,
      job.schedule?.meta,
      job.upsert ?? false
    );
    debug("job #%o: enqueued", job.id);
  }

  async close() {
    await this.redis.quit();
  }
}
