import RedisOwl, { MockOwl, ScheduleMap } from "../src";
import delay from "delay";
import Redis, { Redis as IORedis } from "ioredis";
import test from "ava";

function Test(name: string, getOwl: () => RedisOwl<"every">) {
  const owl = getOwl();

  const redis: IORedis = (owl as any).redis;
  test.beforeEach(async () => {
    await redis.flushall();
  });

  test.serial(name + " > queueing flow", async (t) => {
    t.plan(1);
    t.timeout(500, "give it a bit of headspace")

    const worker = owl.createWorker(async (job) => {
      t.pass(job.payload);
    });

    const producer = owl.createProducer();

    await producer.enqueue({
      id: "1234",
      queue: "bakery",
      payload: "bread",
    });

    await producer.close();
    await worker.close();
  });

  test.serial(name + " > a lot of queued jobs", async (t) => {
    const jobNumber = 50;
    t.plan(jobNumber);
    const worker = owl.createWorker(async () => {
      t.pass();
    });

    const producer = owl.createProducer();

    for (let i = 0; i < jobNumber; i++) {
      await producer.enqueue({
        id: "" + i,
        queue: "bakery",
        payload: "",
      });
    }

    await producer.close();
    await worker.close();
  });

  test.serial(name + " > failing job", async (t) => {
    t.plan(1);
    const worker = owl.createWorker(
      async (job) => {
        throw new Error("epic fail");
      },
      (job, err) => {
        t.is(err.message, "epic fail");
      }
    );

    const producer = owl.createProducer();

    await producer.enqueue({
      id: "1234",
      queue: "bakery",
      payload: "bread",
    });

    await delay(10);

    await producer.close();
    await worker.close();
  });

  test.serial(name + " > every 50ms", async (t) => {
    t.plan(2);
    t.timeout(500, "give it a bit of headspace")

    const worker = owl.createWorker(async (job) => {
      t.pass();
    });

    const producer = owl.createProducer();

    await producer.enqueue({
      id: "1234",
      queue: "bakery",
      payload: "bread",
      schedule: {
        type: "every",
        meta: "50",
      },
    });

    await delay(100);

    await producer.close();
    await worker.close();
  });
}

Test.skip = (_name: string, _redis: () => RedisOwl<"every">) => {};

const scheduleMap: ScheduleMap<"every"> = {
  every: (lastExecution, meta) => new Date(+lastExecution + Number(meta)),
};

Test("Redis", () => new RedisOwl(new Redis(), scheduleMap));
Test.skip("Mocked", () => new MockOwl(scheduleMap));
