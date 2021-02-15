import type { Redis } from "ioredis";
import { Closable } from "../Closable";
import type { Producer } from "../producer/producer";
import { computeTimestampForNextRetry } from "../worker/retry";
import type { Acknowledger } from "./acknowledger";

const oneMinute = 60 * 1000;

export class StaleChecker implements Closable {
  private intervalId?: NodeJS.Timeout;

  constructor(
    private readonly redis: Redis,
    private readonly acknowledger: Acknowledger,
    private readonly producer: Producer<any>,
    private readonly interval = oneMinute,
    private readonly staleAfter = 60 * oneMinute
  ) {}

  public start() {
    this.intervalId = setInterval(() => this.check(), this.interval);
  }

  public close() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
    }
  }

  private getMaxDate(now = Date.now()): number {
    return now - this.staleAfter * 1000;
  }

  private toRedisDate(ms: number): number {
    return Math.floor(ms / 1000);
  }

  private async zremrangebyscoreandreturn(
    key: string,
    min: string | number,
    max: string | number
  ) {
    const result = await this.redis
      .pipeline()
      .zrangebyscore(key, min, max)
      .zremrangebyscore(key, min, max)
      .exec();

    const [rangeByScoreResult, remRangeByScoreResult] = result;

    if (rangeByScoreResult[0]) {
      throw rangeByScoreResult[0];
    }

    if (remRangeByScoreResult[0]) {
      throw remRangeByScoreResult[0];
    }

    return rangeByScoreResult[1] as string[];
  }

  private parseJobDescriptor(descriptor: string) {
    const [queue, id] = descriptor.split(":");
    return { queue, id };
  }

  public async check() {
    const staleJobDescriptors = await this.zremrangebyscoreandreturn(
      "processing",
      "-inf",
      this.toRedisDate(this.getMaxDate())
    );

    const staleJobs = await this.producer.findJobs(
      staleJobDescriptors.map(this.parseJobDescriptor)
    );

    const pipeline = this.redis.pipeline();

    const error = "Job Timed Out";

    for (const job of staleJobs) {
      if (!job) {
        console.error(new Error("Expected job to still exist"));
        continue;
      }

      const timestampForNextRetry = computeTimestampForNextRetry(
        job.runAt,
        job.retry,
        job.count
      );

      this.acknowledger._reportFailure(
        { queueId: job.queue, jobId: job.id, timestampForNextRetry },
        error,
        pipeline
      );
    }

    await pipeline.exec();
  }
}