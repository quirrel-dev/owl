import { expect } from "chai";
import Owl from "../../src";
import IORedis, { Redis } from "ioredis";
import { Producer } from "../../src/producer/producer";

describe("job management", () => {
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

  describe("Producer#scanQueue", () => {
    it("returns pending jobs", async () => {
      await producer.enqueue({
        queue: "producer-scan-queue",
        id: "a",
        payload: "a",
        runAt: new Date("2020-10-27T07:36:56.321Z"),
      });

      await producer.enqueue({
        queue: "producer-scan-queue",
        id: "b",
        payload: "b",
        runAt: new Date("2020-10-27T07:36:56.321Z"),
      });

      const { jobs, newCursor } = await producer.scanQueue(
        "producer-scan-queue",
        0
      );

      expect(jobs).to.deep.eq([
        {
          queue: "producer-scan-queue",
          id: "b",
          payload: "b",
          runAt: new Date("2020-10-27T07:36:56.321Z"),
          schedule: undefined,
        },
        {
          queue: "producer-scan-queue",
          id: "a",
          payload: "a",
          runAt: new Date("2020-10-27T07:36:56.321Z"),
          schedule: undefined,
        },
      ]);

      expect(newCursor).to.eq(0);
    });
  });

  describe("Producer#findById", () => {
    it("returns the right job", async () => {
      await producer.enqueue({
        queue: "producer-find-by-id",
        id: "my-random-id",
        payload: "lol",
        runAt: new Date("2020-10-27T07:36:56.321Z"),
      });

      const job = await producer.findById(
        "producer-find-by-id",
        "my-random-id"
      );

      expect(job).to.deep.eq({
        queue: "producer-find-by-id",
        id: "my-random-id",
        payload: "lol",
        runAt: new Date("2020-10-27T07:36:56.321Z"),
        schedule: undefined,
      });
    });

    describe("when giving non-existing ID", () => {
      it("returns null", async () => {
        const job = await producer.findById(
          "producer-find-by-id",
          "my-random-id"
        );

        expect(job).to.be.null;
      });
    });
  });

  describe("Producer#invoke", () => {
    it("moves job to be executed immediately", async () => {
      await producer.enqueue({
        queue: "producer-invoke",
        id: "a",
        payload: "a",
        runAt: new Date("1970-10-27T07:36:56.321Z"),
      });

      const job = await producer.findById("producer-invoke", "a");
      expect(+job.runAt).to.equal(+new Date("1970-10-27T07:36:56.321Z"));

      await producer.invoke("producer-invoke", "a");

      const invokedJob = await producer.findById("producer-invoke", "a");
      expect(+invokedJob.runAt).to.be.eq(0);
    });
  });

  describe("Producer#delete", () => {
    it("deletes pending job", async () => {
      await producer.enqueue({
        queue: "producer-delete",
        id: "a",
        payload: "a",
      });

      await producer.delete("producer-delete", "a");

      const job = await producer.findById("producer-delete", "a");
      expect(job).to.be.null;
    });
  });
});
