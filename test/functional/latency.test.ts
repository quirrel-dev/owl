import { expect } from "chai";
import Owl from "../../src";
import IORedis, { Redis } from "ioredis";
import { Producer } from "../../src/producer/producer";
import { Worker } from "../../src/worker/worker";
import { Job } from "../../src/Job";

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function sum(nums: number[]) {
  return nums.reduce((curr, acc) => curr + acc, 0);
}

function average(nums: number[]) {
  return sum(nums) / nums.length;
}

describe("Latency", () => {
  let redis: Redis;
  let owl: Owl<string>;
  let producer: Producer<string>;
  let worker: Worker;

  let jobs: [number, Job][] = [];

  beforeEach(async () => {
    redis = new IORedis();
    await redis.flushall();

    owl = new Owl(() => new IORedis());

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

  describe("when inserting 1000 jobs", () => {
    it("they're executed in 1 sec", async () => {
      const enqueueals: Promise<any>[] = [];
      for (let i = 0; i < 1000; i++) {
        enqueueals.push(
          producer.enqueue({
            id: "" + i,
            payload: "" + Date.now(),
            queue: "latency",
          })
        );
      }

      await Promise.all(enqueueals);

      await delay(500);

      expect(jobs).to.be.length(1000);

      const delays = jobs.map(([execTime, { payload }]) => execTime - +payload);

      expect(Math.min(...delays)).to.be.below(300);
      expect(average(delays)).to.be.below(600);
      expect(Math.max(...delays)).to.be.below(900);
    });
  });
});
