import { Redis } from "ioredis";
import { Producer } from "./producer/producer";
import { OnError, Processor, Worker } from "./worker/worker";
import RedisMock from "ioredis-mock";
import { Activity, OnActivity, SubscriptionOptions } from "./activity/activity";

export { Job, JobEnqueue } from "./Job";
export { Closable } from "./Closable";

export type ScheduleMap<ScheduleType extends string> = Record<
  ScheduleType,
  (lastExecution: Date, scheduleMeta: string) => Date | null
>;

export default class Owl<ScheduleType extends string> {
  constructor(
    private readonly redisFactory: () => Redis,
    private readonly scheduleMap: ScheduleMap<ScheduleType> = {} as any
  ) {}

  public createWorker(processor: Processor, onError?: OnError) {
    return new Worker(this.redisFactory, this.scheduleMap, processor, onError);
  }

  public createProducer() {
    return new Producer<ScheduleType>(this.redisFactory);
  }

  public createActivity(
    onEvent: OnActivity,
    options: SubscriptionOptions = {}
  ) {
    return new Activity<ScheduleType>(this.redisFactory, onEvent, options);
  }
}

export class MockOwl<ScheduleType extends string> extends Owl<ScheduleType> {
  constructor(scheduleMap?: ScheduleMap<ScheduleType>) {
    const redis = new RedisMock();
    super(() => (redis as any).createConnectedClient(), scheduleMap);
  }
}
