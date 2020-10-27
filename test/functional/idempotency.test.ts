import { expect } from "chai";
import Owl from "../../src";
import IORedis, { Redis } from "ioredis";
import { Producer } from "../../src/producer/producer";

describe("Idempotency", () => {
  let redis: Redis;
  let owl: Owl<string>;
  let producer: Producer<string>;

  beforeEach(async () => {
    redis = new IORedis();
    await redis.flushall();

    owl = new Owl(() => new IORedis());

    producer = owl.createProducer();
  });

  afterEach(async () => {
    await redis.quit();
    await producer.close();
  });

  describe("when enqueueing an already-existant ID", () => {
    describe("and upsert = true", () => {
      it("replaces existing job", async () => {
        await producer.enqueue({
          queue: "upsert-true-queue",
          id: "a",
          payload: "1",
        });

        await producer.enqueue({
          queue: "upsert-true-queue",
          id: "a",
          payload: "2",
          upsert: true,
        });

        const job = await producer.findById("upsert-true-queue", "a");
        expect(job.payload).to.equal("2");
      });
    });

    describe("and upsert = false", () => {
      it("is a no-op", async () => {
        await producer.enqueue({
          queue: "upsert-false-queue",
          id: "a",
          payload: "1",
        });

        await producer.enqueue({
          queue: "upsert-false-queue",
          id: "a",
          payload: "2",
          upsert: false,
        });

        const job = await producer.findById("upsert-false-queue", "a");
        expect(job.payload).to.equal("1");
      });
    });
  });
});
