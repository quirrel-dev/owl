import Owl, { MockOwl } from "../../src";
import IORedis, { Redis } from "ioredis";
import { Producer } from "../../src/producer/producer";
import { Worker } from "../../src/worker/worker";
import { Job } from "../../src/Job";

export function makeProducerEnv(inMemory = false) {
  const env: {
    redis: Redis;
    owl: Owl<"every">;
    producer: Producer<"every">;
    setup: () => Promise<void>;
    teardown: () => Promise<void>;
  } = {
    redis: null as any,
    owl: null as any,
    producer: null as any,
    setup,
    teardown,
  };

  async function setup() {
    const scheduleMap = {
      every: (lastDate, meta) => new Date(+lastDate + +meta),
    };
    if (inMemory) {
      env.owl = new MockOwl(scheduleMap);
    } else {
      env.redis = new IORedis(process.env.REDIS_URL);
      await env.redis.flushall();

      env.owl = new Owl(() => new IORedis(), scheduleMap);
    }

    env.producer = env.owl.createProducer();
  }

  async function teardown() {
    await env.redis?.quit();
    await env.producer.close();
  }

  return env;
}

export function makeWorkerEnv(inMemory = false) {
  const producerEnv = makeProducerEnv(inMemory);

  const producerSetup = producerEnv.setup;
  const producerTeardown = producerEnv.teardown;

  const workerEnv: typeof producerEnv & {
    worker: Worker;
    jobs: [number, Job][];
  } = producerEnv as any;

  workerEnv.worker = null as any;
  workerEnv.jobs = [];

  workerEnv.setup = async function setup() {
    await producerSetup();

    workerEnv.jobs = [];

    workerEnv.worker = producerEnv.owl.createWorker(async (job) => {
      workerEnv.jobs.push([Date.now(), job]);
    });
  };

  workerEnv.teardown = async function teardown() {
    await producerTeardown();
    await workerEnv.worker.close();
  };

  return workerEnv;
}
