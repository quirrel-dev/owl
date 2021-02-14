import { Redis } from "ioredis";
import { Producer } from "./producer/producer";
import { Processor, Worker } from "./worker/worker";
import { Activity, OnActivity, SubscriptionOptions } from "./activity/activity";
import { OnError } from "./shared/acknowledger";
import { migrate } from "./shared/migrator";

export { Job, JobEnqueue } from "./Job";
export { Closable } from "./Closable";

export type ScheduleMap<ScheduleType extends string> = Record<
  ScheduleType,
  (lastExecution: Date, scheduleMeta: string) => Date | null
>;

export default class Owl<ScheduleType extends string> {
  constructor(
    private readonly redisFactory: () => Redis,
    private readonly scheduleMap: ScheduleMap<ScheduleType> = {} as any,
    private readonly onError?: OnError
  ) {}

  public createWorker(processor: Processor) {
    return new Worker(
      this.redisFactory,
      this.scheduleMap,
      processor,
      this.onError
    );
  }

  public createProducer() {
    return new Producer<ScheduleType>(this.redisFactory, this.onError);
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
