import { Redis } from "ioredis";
import { Producer } from "./producer/producer";
import { OnError, Processor, Worker } from "./worker/worker";
import RedisMock from "ioredis-mock";

export type ScheduleMap<ScheduleType extends string> = Record<
  ScheduleType,
  (lastExecution: Date, scheduleMeta: string) => Date | null | "immediate"
>;

export default class Owl<ScheduleType extends string> {
  constructor(
    private readonly redis: Redis,
    private readonly scheduleMap: ScheduleMap<ScheduleType> = {} as any
  ) {}

  public createWorker(processor: Processor, onError?: OnError) {
    return new Worker(this.redis, this.scheduleMap, processor, onError);
  }

  public createProducer() {
    return new Producer<ScheduleType>(this.redis);
  }
}

export class MockOwl<ScheduleType extends string> extends Owl<ScheduleType> {
  constructor(scheduleMap?: ScheduleMap<ScheduleType>) {
    super(new RedisMock(), scheduleMap);
  }
}
