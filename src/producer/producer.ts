import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { Job, JobEnqueue } from "../Job";
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
      times: number | undefined,
      useUpsertSemantics: boolean
    ): Promise<0 | 1>;

    delete(
      jobTableJobId: string,
      jobTableIndexByQueue: string,
      queueKey: string,
      id: string,
      queue: string
    ): Promise<0 | 1>;

    invoke(
      jobTableJobId: string,
      queueKey: string,
      id: string,
      queue: string
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

    this.redis.defineCommand("invoke", {
      lua: fs.readFileSync(path.join(__dirname, "invoke.lua")).toString(),
      numberOfKeys: 2,
    });

    this.redis.defineCommand("delete", {
      lua: fs.readFileSync(path.join(__dirname, "delete.lua")).toString(),
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
      job.times,
      job.upsert ?? false
    );
    debug("job #%o: enqueued", job.id);
  }

  public async scanQueue(
    queue: string,
    cursor: number = 0
  ): Promise<{ newCursor: number; jobs: Job<ScheduleType>[] }> {
    const [newCursor, jobIds] = await this.redis.sscan(
      `queues:${queue}`,
      cursor
    );

    return {
      newCursor: +newCursor,
      jobs: (await this.findJobs(queue, jobIds)).filter((j) => !!j) as Job<
        ScheduleType
      >[],
    };
  }

  private async findJobs(
    queue: string,
    ids: string[]
  ): Promise<(Job<ScheduleType> | null)[]> {
    const pipeline = this.redis.pipeline();

    for (const id of ids) {
      pipeline.hgetall(`jobs:${queue}:${id}`);
      pipeline.zscore("queue", `${queue}:${id}`);
    }

    const jobResults: (Job<ScheduleType> | null)[] = [];

    const redisResults = await pipeline.exec();
    for (let i = 0; i < redisResults.length; i += 2) {
      const [hgetallErr, hgetallResult] = redisResults[i];
      const [zscoreErr, zscoreResult] = redisResults[i + 1];
      const jobId = ids[i / 2];

      if (hgetallErr) {
        throw hgetallErr;
      }

      if (zscoreErr) {
        throw zscoreErr;
      }

      const { payload, schedule_type, schedule_meta, count, max_times } = hgetallResult;

      if (typeof payload === "undefined") {
        jobResults.push(null);
        continue;
      }

      const runAt = +zscoreResult;

      jobResults.push({
        id: jobId,
        queue,
        payload,
        runAt: new Date(runAt),
        schedule: schedule_type
          ? {
              type: schedule_type,
              meta: schedule_meta,
            }
          : undefined,
        count: +count,
        times: max_times ? +max_times : undefined
      });
    }

    return jobResults;
  }

  public async findById(
    queue: string,
    id: string
  ): Promise<Job<ScheduleType> | null> {
    const [job] = await this.findJobs(queue, [id]);
    return job;
  }

  public async delete(queue: string, id: string) {
    await this.redis.delete(
      `jobs:${queue}:${id}`,
      `queues:${queue}`,
      "queue",
      id,
      queue
    );
  }

  public async invoke(queue: string, id: string) {
    await this.redis.invoke(`jobs:${queue}:${id}`, "queue", id, queue);
  }

  async close() {
    await this.redis.quit();
  }
}
