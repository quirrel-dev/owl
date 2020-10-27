import { expect } from "chai";
import Owl from "../../src";
import Redis from "ioredis";

describe("job management", () => {
  describe("Producer#scanQueue", () => {
    it("returns pending jobs", async () => {
      const redis = new Redis();
      await redis.flushall();
      const owl = new Owl(() => new Redis());

      const producer = owl.createProducer();

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

      await producer.close();
      await redis.quit();
    });
  });

  describe("Producer#findById", () => {
    it("returns the right job", async () => {
      const redis = new Redis();
      await redis.flushall();
      const owl = new Owl(() => new Redis());

      const producer = owl.createProducer();

      await producer.enqueue({
        queue: "producer-find-by-id",
        id: "my-random-id",
        payload: "lol",
        runAt: new Date("2020-10-27T07:36:56.321Z"),
      });

      const job = await producer.findById("producer-find-by-id", "my-random-id");

      expect(job).to.deep.eq({
        queue: "producer-find-by-id",
        id: "my-random-id",
        payload: "lol",
        runAt: new Date("2020-10-27T07:36:56.321Z"),
        schedule: undefined
      });

      await producer.close();
      await redis.quit();
    });
  });

  describe("Producer#scanQueue", () => {
    it("returns pending jobs", async () => {
      const redis = new Redis();
      await redis.flushall();
      const owl = new Owl(() => new Redis());

      const producer = owl.createProducer();

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

      await producer.close();
      await redis.quit();
    });
  });
});
