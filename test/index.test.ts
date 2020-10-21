import RedisOwl, { MockOwl, ScheduleMap } from "../src";
import Redis, { Redis as IORedis } from "ioredis";
import test from "ava";

function Test(name: string, getOwl: () => RedisOwl<"every">) {
  const owl = getOwl();

  const redis: IORedis = (owl as any).redisFactory();

  test.afterEach(async () => {
    await redis.flushall();
  });

  test.beforeEach(async () => {
    await redis.flushall();
  });

  test.after(async () => {
    await redis.quit();
  });

  test.serial(name + " > queueing flow", async (t) => {
    t.plan(1);

    const worker = owl.createWorker(async (job) => {
      t.is(job.payload, "bread");
    });

    const producer = owl.createProducer();

    t.teardown(async () => {
      await producer.close();
      await worker.close();
    });

    await producer.enqueue({
      id: "1234",
      queue: "queueing_flow",
      payload: "bread",
    });
  });

  test.serial(name + " > a lot of queued jobs", async (t) => {
    const jobNumber = 50;
    t.plan(jobNumber);
    const worker = owl.createWorker(async (job) => {
      t.is(job.queue, "a_lot_of_jobs");
    });

    const producer = owl.createProducer();

    t.teardown(async () => {
      await producer.close();
      await worker.close();
    });

    for (let i = 0; i < jobNumber; i++) {
      await producer.enqueue({
        id: "" + i,
        queue: "a_lot_of_jobs",
        payload: "",
      });
    }
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

    t.teardown(async () => {
      await producer.close();
      await worker.close();
    });

    await producer.enqueue({
      id: "1234",
      queue: "epic_fail",
      payload: "bread",
    });
  });

  test.serial(name + " > every 50ms", async (t) => {
    t.plan(2);
    t.timeout(500, "give it a bit of headspace");

    const worker = owl.createWorker(async (job) => {
      t.is(job.queue, "repeatedly");
    });

    const producer = owl.createProducer();

    await producer.enqueue({
      id: "1234",
      queue: "repeatedly",
      payload: "bread",
      schedule: {
        type: "every",
        meta: "50",
      },
    });

    t.teardown(async () => {
      await producer.close();
      await worker.close();
    });
  });
}

Test.skip = (_name: string, _redis: () => RedisOwl<"every">) => {};

const scheduleMap: ScheduleMap<"every"> = {
  every: (lastExecution, meta) => new Date(+lastExecution + Number(meta)),
};

Test("Redis", () => new RedisOwl(() => new Redis(), scheduleMap));
Test("Mocked", () => new MockOwl(scheduleMap));
