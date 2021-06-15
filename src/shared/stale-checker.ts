import type { Redis } from "ioredis";
import type { Logger } from "pino";
import { ScheduleMap } from "..";
import { Closable } from "../Closable";
import { tenantToRedisPrefix } from "../encodeRedisKey";
import type { Producer } from "../producer/producer";
import { computeTimestampForNextRetry } from "../worker/retry";
import { getNextExecutionDate } from "../worker/worker";
import type { Acknowledger } from "./acknowledger";
import { scanTenantsForProcessing } from "./scan-tenants";

function valueAndScoreToObj(arr: (string | number)[]) {
  const result: { value: string; score: number }[] = [];

  for (let i = 0; i < arr.length; i += 2) {
    result.push({
      value: String(arr[i]),
      score: Number(arr[i + 1]),
    });
  }

  return result;
}

const oneMinute = 60 * 1000;

export interface StaleCheckerConfig {
  interval?: number | "manual";
  staleAfter?: number;
}

export class StaleChecker<ScheduleType extends string> implements Closable {
  private intervalId?: NodeJS.Timeout;

  private readonly staleAfter;

  constructor(
    private readonly redis: Redis,
    private readonly acknowledger: Acknowledger<ScheduleType>,
    private readonly producer: Producer<ScheduleType>,
    private readonly scheduleMap: ScheduleMap<ScheduleType>,
    config: StaleCheckerConfig = {},
    private readonly logger?: Logger
  ) {
    this.staleAfter = config.staleAfter ?? 60 * oneMinute;

    if (config.interval !== "manual") {
      this.intervalId = setInterval(
        () => this.check(),
        config.interval ?? oneMinute
      );
    }
  }

  public close() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
    }
  }

  private getMaxDate(now = Date.now()): number {
    return now - this.staleAfter;
  }

  private async zremrangebyscoreandreturn(
    key: string,
    min: string | number,
    max: string | number
  ) {
    const result = await this.redis
      .pipeline()
      .zrangebyscore(key, min, max, "WITHSCORES")
      .zremrangebyscore(key, min, max)
      .exec();

    const [rangeByScoreResult, remRangeByScoreResult] = result;

    if (rangeByScoreResult[0]) {
      throw rangeByScoreResult[0];
    }

    if (remRangeByScoreResult[0]) {
      throw remRangeByScoreResult[0];
    }

    return valueAndScoreToObj(rangeByScoreResult[1]);
  }

  private parseJobDescriptor(descriptor: string) {
    const [queue, id] = descriptor.split(":");
    return { queue, id };
  }

  public async check() {
    this.logger?.trace("Stale-Checker: Starting.");
    for await (const tenants of scanTenantsForProcessing(this.redis)) {
      await Promise.all(
        tenants.map(async (tenant) => {
          this.logger?.trace({ tenant }, "Stale-Checker: Starting for tenant.");

          const staleJobDescriptors = await this.zremrangebyscoreandreturn(
            tenantToRedisPrefix(tenant) + "processing",
            "-inf",
            this.getMaxDate()
          );

          if (staleJobDescriptors.length === 0) {
            this.logger?.trace(
              { tenant },
              "Stale-Checker: No stale jobs found."
            );
            return;
          }

          this.logger?.trace(
            { staleJobDescriptors, tenant },
            "Stale-Checker: Found stale jobs."
          );

          const staleJobs = await this.producer.findJobs(
            tenant,
            staleJobDescriptors.map(({ value }) =>
              this.parseJobDescriptor(value)
            )
          );

          const pipeline = this.redis.pipeline();

          const error = "Job Timed Out";

          for (let i = 0; i < staleJobs.length; i++) {
            const job = staleJobs[0];
            const score = staleJobDescriptors[0].score;
            if (!job) {
              this.logger?.error(
                { tenant },
                "Stale-Checker: Expected job to still exist"
              );
              continue;
            }

            const timestampForNextRetry = computeTimestampForNextRetry(
              job.runAt,
              job.retry,
              job.count
            );

            // TODO: duplicated logic in producer, please extract
            let nextExecutionDate: number | undefined = undefined;

            if (!job.schedule?.times || job.count < job.schedule?.times) {
              nextExecutionDate = getNextExecutionDate(
                this.scheduleMap,
                job.schedule?.type,
                job.schedule?.meta ?? "",
                new Date(score)
              );
            }

            this.logger?.trace(
              { tenant, job },
              "Stale-Checker: Adding Failure report to pipeline"
            );

            await this.acknowledger._reportFailure(
              {
                tenant,
                queueId: job.queue,
                jobId: job.id,
                timestampForNextRetry,
                nextExecutionDate,
              },
              job,
              error,
              pipeline
            );
          }

          this.logger?.trace(
            { tenant },
            "Stale-Checker: Starting pipeline execution"
          );
          await pipeline.exec();
          this.logger?.trace(
            { tenant },
            "Stale-Checker: Pipeline execution successful"
          );
        })
      );
    }
  }
}
