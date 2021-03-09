import Owl from "../../src";
import IORedis, { Redis } from "ioredis";
import IORedisMock from "ioredis-mock";
import { Producer } from "../../src/producer/producer";
import { Activity, OnActivityEvent } from "../../src/activity/activity";
import { Worker } from "../../src/worker/worker";
import { Job } from "../../src/Job";

export function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
      env.redis = new IORedisMock();
      env.owl = new Owl(
        () => (env.redis as any).createConnectedClient(),
        scheduleMap
      );
    } else {
      env.redis = new IORedis(process.env.REDIS_URL);
      await env.redis.flushall();

      env.owl = new Owl(() => new IORedis(process.env.REDIS_URL), scheduleMap);
    }

    env.producer = env.owl.createProducer();
  }

  async function teardown() {
    await env.redis?.quit();
    await env.producer.close();
  }

  return env;
}

type WorkerFailPredicate = (job: Job<string>) => boolean;

export function makeWorkerEnv(
  inMemory = false,
  fail: WorkerFailPredicate = (job: Job<string>) => false
) {
  const producerEnv = makeProducerEnv(inMemory);

  const producerSetup = producerEnv.setup;
  const producerTeardown = producerEnv.teardown;

  const workerEnv: typeof producerEnv & {
    worker: Worker;
    jobs: [number, Job][];
    nextExecDates: (Date | undefined)[];
    errors: [Job, Error][];
  } = producerEnv as any;

  workerEnv.worker = null as any;
  workerEnv.jobs = [];
  workerEnv.errors = [];
  workerEnv.nextExecDates = [];

  workerEnv.setup = async function setup() {
    await producerSetup();

    workerEnv.jobs = [];

    workerEnv.worker = producerEnv.owl.createWorker(
      async (job, meta) => {
        workerEnv.jobs.push([Date.now(), job]);
        workerEnv.nextExecDates.push(meta.nextExecDate);

        if (job.payload.startsWith("block:")) {
          const duration = job.payload.split(":")[1];
          await delay(+duration);
        }

        if (fail(job)) {
          throw new Error("failing!");
        }
      },
      (job, error) => {
        workerEnv.errors.push([job, error]);
      }
    );
  };

  workerEnv.teardown = async function teardown() {
    await producerTeardown();
    await workerEnv.worker.close();
  };

  return workerEnv;
}

export function makeActivityEnv(inMemory = false, fail?: WorkerFailPredicate) {
  const workerEnv = makeWorkerEnv(inMemory, fail);

  const workerSetup = workerEnv.setup;
  const workerTeardown = workerEnv.teardown;

  const activityEnv: typeof workerEnv & {
    activity: Activity<"every">;
    events: OnActivityEvent[];
  } = workerEnv as any;

  activityEnv.activity = null as any;
  activityEnv.events = [];

  activityEnv.setup = async function setup() {
    await workerSetup();

    activityEnv.events = [];

    activityEnv.activity = workerEnv.owl.createActivity((event) => {
      activityEnv.events.push(event);
    });
  };

  activityEnv.teardown = async function teardown() {
    await workerTeardown();
    await activityEnv.activity.close();
  };

  return activityEnv;
}
