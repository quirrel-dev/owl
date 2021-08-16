import { Redis } from "ioredis";
import { Producer } from "./producer/producer";
import { Processor, Worker } from "./worker/worker";
import { Activity, OnActivity, SubscriptionOptions } from "./activity/activity";
import { OnError } from "./shared/acknowledger";
import { migrate } from "./shared/migrator/migrator";
import { StaleCheckerConfig } from "./shared/stale-checker";
import type { Logger } from "pino";
import { GateKeeper } from "./gatekeeper/gatekeeper";

export { Job, JobEnqueue } from "./Job";
export { Closable } from "./Closable";

export type ScheduleMap<ScheduleType extends string> = Record<
  ScheduleType,
  (lastExecution: Date, scheduleMeta: string) => Date | null
>;

export interface OwlConfig<ScheduleType extends string> {
  redisFactory: () => Redis;
  scheduleMap: ScheduleMap<ScheduleType>;
  staleChecker?: StaleCheckerConfig;
  gateKeeper?: { interval: number };
  onError?: OnError<ScheduleType>;
  logger?: Logger;
}

export default class Owl<ScheduleType extends string> {
  private readonly redisFactory;
  private readonly scheduleMap: ScheduleMap<ScheduleType>;
  private readonly staleCheckerConfig?: StaleCheckerConfig;
  private readonly onError?;
  private readonly logger?;
  private readonly gateKeeperInterval?;
  constructor(config: OwlConfig<ScheduleType>) {
    this.redisFactory = config.redisFactory;
    this.scheduleMap = config.scheduleMap;
    this.staleCheckerConfig = config.staleChecker;
    this.onError = config.onError;
    this.logger = config.logger;
    this.gateKeeperInterval = config.gateKeeper?.interval;
  }

  public createWorker(processor: Processor<ScheduleType>) {
    const worker = new Worker<ScheduleType>(
      this.redisFactory,
      this.scheduleMap,
      processor,
      this.onError,
      this.logger
    );

    worker.start();

    return worker;
  }

  public createProducer() {
    return new Producer<ScheduleType>(
      this.redisFactory,
      this.scheduleMap,
      this.onError,
      this.staleCheckerConfig,
      this.logger
    );
  }

  public createGateKeeper() {
    const gatekeeper = new GateKeeper(
      this.redisFactory,
      this.gateKeeperInterval
    );
    gatekeeper.start();
    return gatekeeper;
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
