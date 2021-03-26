import Owl, { OwlConfig } from "../../src";
import IORedis, { Redis } from "ioredis";
import IORedisMock from "ioredis-mock";
import { Producer } from "../../src/producer/producer";
import { Activity, OnActivityEvent } from "../../src/activity/activity";
import { Worker } from "../../src/worker/worker";
import { Job } from "../../src/Job";
import { AcknowledgementDescriptor } from "../../src/shared/acknowledger";
import { Backend, delay } from "../util";

export function makeProducerEnv(
  backend: Backend,
  config?: Partial<OwlConfig<any>>
) {
  const env: {
    redis: Redis;
    owl: Owl<"every">;
    producer: Producer<"every">;
    setup: () => Promise<void>;
    teardown: () => Promise<void>;
    errors: [AcknowledgementDescriptor, Error][];
  } = {
    redis: null as any,
    owl: null as any,
    producer: null as any,
    setup,
    teardown,
    errors: [],
  };

  function onError(descriptor: AcknowledgementDescriptor, error?: any) {
    env.errors.push([descriptor, error]);
  }

  async function setup() {
    const scheduleMap = {
      every: (lastDate: Date, meta: string) => new Date(+lastDate + +meta),
    };
    if (backend === "In-Memory") {
      env.redis = new IORedisMock();
      env.owl = new Owl({
        redisFactory: () => (env.redis as any).createConnectedClient(),
        scheduleMap,
        onError,
        ...config,
      });
    } else {
      env.redis = new IORedis(process.env.REDIS_URL);
      await env.redis.flushall();

      env.owl = new Owl({
        redisFactory: () => new IORedis(process.env.REDIS_URL),
        scheduleMap,
        onError,
        ...config,
      });
    }

    env.producer = env.owl.createProducer();
    env.errors = [];
  }

  async function teardown() {
    await env.redis?.quit();
    await env.producer.close();
  }

  return env;
}

type WorkerFailPredicate = (job: Job<string>) => boolean;

type JobListener = (job: Job<string>) => void;

export function makeWorkerEnv(
  backend: Backend,
  fail: WorkerFailPredicate = (job: Job<string>) => false
) {
  const producerEnv = makeProducerEnv(backend);

  const producerSetup = producerEnv.setup;
  const producerTeardown = producerEnv.teardown;

  const workerEnv: typeof producerEnv & {
    worker: Worker;
    jobs: [number, Job][];
    nextExecDates: (number | undefined)[];
    errors: [Job, Error][];
    onStartedJob(doIt: JobListener): void;
    onFinishedJob(doIt: JobListener): void;
  } = producerEnv as any;

  workerEnv.worker = null as any;
  workerEnv.jobs = [];
  workerEnv.errors = [];
  workerEnv.nextExecDates = [];

  let onStartedListeners: JobListener[] = [];
  workerEnv.onStartedJob = (doIt) => onStartedListeners.push(doIt);

  let onFinishedListeners: JobListener[] = [];
  workerEnv.onFinishedJob = (doIt) => onFinishedListeners.push(doIt);

  workerEnv.setup = async function setup() {
    await producerSetup();

    workerEnv.jobs = [];
    onStartedListeners = [];
    onFinishedListeners = [];

    workerEnv.worker = producerEnv.owl.createWorker(
      async (job, ackDescriptor) => {
        onStartedListeners.forEach((listener) => listener(job));

        workerEnv.jobs.push([Date.now(), job]);
        workerEnv.nextExecDates.push(ackDescriptor.nextExecutionDate);

        if (job.payload.startsWith("block:")) {
          const duration = job.payload.split(":")[1];
          await delay(+duration);
        } else {
          await delay(1);
        }

        if (fail(job)) {
          throw new Error("failing!");
        } else {
          await workerEnv.worker.acknowledger.acknowledge(ackDescriptor);
        }

        onFinishedListeners.forEach((listener) => listener(job));
      }
    );
  };

  workerEnv.teardown = async function teardown() {
    await producerTeardown();
    await workerEnv.worker.close();
  };

  return workerEnv;
}

export function makeActivityEnv(backend: Backend, fail?: WorkerFailPredicate) {
  const workerEnv = makeWorkerEnv(backend, fail);

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

    await delay(10);
  };

  activityEnv.teardown = async function teardown() {
    await workerTeardown();
    await activityEnv.activity.close();
  };

  return activityEnv;
}
