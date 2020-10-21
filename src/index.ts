import { Redis } from "ioredis";
import { Producer } from "./producer/producer";
import { OnError, Processor, Worker } from "./worker/worker";
import RedisMock from "ioredis-mock";

export default class Owl {
  constructor(private readonly redis: Redis) {}

  public createWorker(processor: Processor, onError?: OnError) {
    return new Worker(this.redis, processor, onError);
  }

  public createProducer() {
    return new Producer(this.redis);
  }
}

export class MockOwl extends Owl {
  constructor() {
    super(new RedisMock());
  }
}
