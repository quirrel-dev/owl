import { Redis } from "ioredis";
import { Closable } from "../Closable";
import { Job, JobEnqueue } from "../Job";
import * as fs from "fs";
import * as path from "path";
import createDebug from "debug";
import { decodeRedisKey, encodeRedisKey } from "../encodeRedisKey";

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
      overwrite: boolean,
      exclusive: boolean,
      retryIntervals: string
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
      queue: string,
      newRunAt: number
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

  public async enqueue(
    job: JobEnqueue<ScheduleType>
  ): Promise<Job<ScheduleType>> {
    debug("job #%o: enqueueing", job.id);

    if (typeof job.runAt === "undefined") {
      job.runAt = new Date();
    }

    const { retry = [], schedule } = job;

    if (retry.length && schedule) {
      throw new Error("retry and schedule cannot be used together");
    }

    await this.redis.schedule(
      `jobs:${encodeRedisKey(job.queue)}:${encodeRedisKey(job.id)}`,
      `queues:${encodeRedisKey(job.queue)}`,
      "queue",
      encodeRedisKey(job.id),
      encodeRedisKey(job.queue),
      job.payload,
      +job.runAt,
      job.schedule?.type,
      job.schedule?.meta,
      job.schedule?.times,
      job.override ?? false,
      job.exclusive ?? false,
      JSON.stringify(retry)
    );
    debug("job #%o: enqueued", job.id);

    return {
      id: job.id,
      queue: job.queue,
      count: 1,
      payload: job.payload,
      runAt: job.runAt,
      exclusive: job.exclusive ?? false,
      schedule: job.schedule,
      retry,
    };
  }

  public async scanQueue(
    queue: string,
    cursor: number = 0,
    count = 100
  ): Promise<{ newCursor: number; jobs: Job<ScheduleType>[] }> {
    const [newCursor, jobIds] = await this.redis.sscan(
      `queues:${encodeRedisKey(queue)}`,
      cursor,
      "COUNT",
      count
    );

    return {
      newCursor: +newCursor,
      jobs: (
        await this.findJobs(
          jobIds.map(decodeRedisKey).map((id) => ({ id, queue }))
        )
      ).filter((j) => !!j) as Job<ScheduleType>[],
    };
  }

  public async scanQueuePattern(
    queuePattern: string,
    cursor: number = 0,
    count = 100
  ): Promise<{ newCursor: number; jobs: Job<ScheduleType>[] }> {
    const [newCursor, jobIdKeys] = await this.redis.scan(
      cursor,
      "MATCH",
      `jobs:${encodeRedisKey(queuePattern)}:*`,
      "COUNT",
      count
    );

    const jobIds = jobIdKeys.map((jobIdKey) => {
      const [, queue, id] = jobIdKey.split(":");
      return { queue: decodeRedisKey(queue), id: decodeRedisKey(id) };
    });

    return {
      newCursor: +newCursor,
      jobs: (await this.findJobs(jobIds)).filter(
        (j) => !!j
      ) as Job<ScheduleType>[],
    };
  }

  private async findJobs(
    ids: { queue: string; id: string }[]
  ): Promise<(Job<ScheduleType> | null)[]> {
    const pipeline = this.redis.pipeline();

    for (const { queue, id } of ids) {
      pipeline.hgetall(
        `jobs:${encodeRedisKey(queue)}:${encodeRedisKey(id)}`
      );
      pipeline.zscore(
        "queue",
        `${encodeRedisKey(queue)}:${encodeRedisKey(id)}`
      );
    }

    const jobResults: (Job<ScheduleType> | null)[] = [];

    const redisResults = await pipeline.exec();
    for (let i = 0; i < redisResults.length; i += 2) {
      const [hgetallErr, hgetallResult] = redisResults[i];
      const [zscoreErr, zscoreResult] = redisResults[i + 1];
      const { id: _id, queue: _queue } = ids[i / 2];
      const id = decodeRedisKey(_id);
      const queue = decodeRedisKey(_queue);

      if (hgetallErr) {
        throw hgetallErr;
      }

      if (zscoreErr) {
        throw zscoreErr;
      }

      const {
        payload,
        schedule_type,
        schedule_meta,
        count,
        max_times,
        exclusive,
        retry,
      } = hgetallResult;

      if (typeof payload === "undefined") {
        jobResults.push(null);
        continue;
      }

      const runAt = +zscoreResult;

      jobResults.push({
        id,
        queue,
        payload,
        runAt: new Date(runAt),
        exclusive: exclusive === "true",
        schedule: schedule_type
          ? {
              type: schedule_type,
              meta: schedule_meta,
              times: max_times ? +max_times : undefined,
            }
          : undefined,
        count: +count,
        retry: JSON.parse(retry ?? "[]"),
      });
    }

    return jobResults;
  }

  public async findById(
    queue: string,
    id: string
  ): Promise<Job<ScheduleType> | null> {
    const [job] = await this.findJobs([{ id, queue }]);
    return job;
  }

  public async delete(queue: string, id: string) {
    const result = await this.redis.delete(
      `jobs:${encodeRedisKey(queue)}:${encodeRedisKey(id)}`,
      `queues:${encodeRedisKey(queue)}`,
      "queue",
      encodeRedisKey(id),
      encodeRedisKey(queue)
    );

    switch (result) {
      case 0:
        return "deleted";
      case 1:
        return "not_found";
    }
  }

  public async invoke(queue: string, id: string) {
    const result = await this.redis.invoke(
      `jobs:${encodeRedisKey(queue)}:${encodeRedisKey(id)}`,
      "queue",
      encodeRedisKey(id),
      encodeRedisKey(queue),
      Date.now()
    );
    switch (result) {
      case 0:
        return "invoked";
      case 1:
        return "not_found";
    }
  }

  async close() {
    await this.redis.quit();
  }
}
