import { Redis } from "ioredis";
import { Producer } from "./producer/producer";
import { Processor, Worker } from "./worker/worker";
import { Activity, OnActivity, SubscriptionOptions } from "./activity/activity";
import { OnError } from "./shared/acknowledger";
import { migrate } from "./shared/migrator/migrator";
import { StaleCheckerConfig } from "./shared/stale-checker";

export { Job, JobEnqueue } from "./Job";
export { Closable } from "./Closable";

export type ScheduleMap<ScheduleType extends string> = Record<
  ScheduleType,
  (lastExecution: Date, scheduleMeta: string) => Date | null
>;

export interface OwlConfig<ScheduleType extends string> {
  redisFactory: () => Redis;
  scheduleMap?: ScheduleMap<ScheduleType>;
  staleChecker?: StaleCheckerConfig;
  onError?: OnError;
}

export default class Owl<ScheduleType extends string> {
  private readonly redisFactory;
  private readonly scheduleMap?: ScheduleMap<ScheduleType>;
  private readonly staleCheckerConfig?: StaleCheckerConfig;
  private readonly onError?;
  constructor(config: OwlConfig<ScheduleType>) {
    this.redisFactory = config.redisFactory;
    this.scheduleMap = config.scheduleMap;
    this.staleCheckerConfig = config.staleChecker;
    this.onError = config.onError;
  }

  public createWorker(processor: Processor) {
    return new Worker(
      this.redisFactory,
      this.scheduleMap ?? {},
      processor,
      this.onError
    );
  }

  public createProducer() {
    return new Producer<ScheduleType>(
      this.redisFactory,
      this.onError,
      this.staleCheckerConfig
    );
  }

  public createActivity(
    onEvent: OnActivity,
    options: SubscriptionOptions = {}
  ) {
    return new Activity<ScheduleType>(this.redisFactory, onEvent, options);
  }

  public async runMigrations() {
    const client = this.redisFactory();
    await migrate(client);
    client.disconnect();
  }
}
