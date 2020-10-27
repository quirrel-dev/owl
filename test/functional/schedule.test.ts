import { expect } from "chai";
import Owl from "../../src";
import IORedis, { Redis } from "ioredis";
import { Producer } from "../../src/producer/producer";
import { Worker } from "../../src/worker/worker";
import { Job } from "../../src/Job";

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("Schedule", () => {
  let redis: Redis;
  let owl: Owl<"every">;
  let producer: Producer<"every">;
  let worker: Worker;

  let jobs: [number, Job][] = [];

  beforeEach(async () => {
    redis = new IORedis();
    await redis.flushall();

    owl = new Owl(() => new IORedis(), {
      every: (lastDate, meta) => new Date(+lastDate + +meta),
    });

    jobs = [];

    producer = owl.createProducer();
    worker = owl.createWorker(async (job) => {
      jobs.push([Date.now(), job]);
    });
  });

  afterEach(async () => {
    await redis.quit();
    await producer.close();
    await worker.close();
  });

  describe("every 10 msec", () => {
    describe("without 'times' limit", () => {
      it("executes until deleted", async () => {
        await producer.enqueue({
          queue: "scheduled-eternity",
          id: "a",
          payload: "a",
          schedule: {
            type: "every",
            meta: "10",
          },
        });

        await delay(100);

        expect(jobs.length).to.be.closeTo(7, 1);

        const lengthBeforeDeletion = jobs.length;

        await producer.delete("scheduled-eternity", "a");

        await delay(100);

        const lengthAfterDeletion = jobs.length;

        expect(lengthAfterDeletion - lengthBeforeDeletion).to.be.closeTo(0, 1);
      });
    });

    describe("with 'times' limit", () => {
      it("executes specified amount of times", async () => {
        await producer.enqueue({
          queue: "scheduled-times",
          id: "a",
          payload: "a",
          schedule: {
            type: "every",
            meta: "10",
          },
          times: 5,
        });

        await delay(100);

        expect(jobs.length).to.equal(5);
      });
    });
  });
});
