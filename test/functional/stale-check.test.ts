import { expect } from "chai";
import { Worker } from "../../src/worker/worker";
import { delay, describeAcrossBackends, makeSignal, waitUntil } from "../util";
import { makeProducerEnv } from "./support";

describeAcrossBackends("stale-check", (backend) => {
  const env = makeProducerEnv(backend, {
    staleChecker: {
      interval: "manual",
      staleAfter: 100,
    },
  });
  beforeEach(env.setup);
  afterEach(async () => {
    await worker.close();
    await env.teardown();
  });

  let worker: Worker<any>;

  it("emits errors for stalling jobs", async () => {
    const received = makeSignal();
    worker = await env.owl.createWorker(async (job) => {
      // happily takes jobs, but never acknowledges any of them
      // simulating a dying worker
      received.signal();
    });

    await env.producer.enqueue({
      id: "stalling-job",
      payload: "i am stalling, just like susanne",
      queue: "stally-stall",
    });

    await received;

    await waitUntil(async () => {
      await env.producer.staleChecker.check();
      return env.errors.length === 1;
    }, 1000);

    expect(env.errors).to.deep.equal([
      [
        {
          jobId: "stalling-job",
          queueId: "stally-stall",
          timestampForNextRetry: undefined,
          nextExecutionDate: undefined,
        },
        "Job Timed Out",
      ],
    ]);
  });

  it("reschedules jobs with retry", async function () {
    const received = makeSignal();
    let calls = 0;
    worker = await env.owl.createWorker(async (job, ack) => {
      calls++;
      if (job.count > 1) {
        await worker.acknowledger.acknowledge(ack);
      } else {
        received.signal();
      }
    });

    await env.producer.enqueue({
      id: "retryable-stalling-job",
      payload: "i am stalling, just like susanne",
      queue: "retry-stally-stall",
      retry: [100],
    });

    await received;

    await waitUntil(async () => {
      await env.producer.staleChecker.check();
      return calls === 2;
    }, 500);

    expect(env.errors).to.deep.equal([]);
  });

  it("does not emit errors if everything is fine", async () => {
    worker = await env.owl.createWorker(async (job, ack) => {
      setTimeout(() => {
        worker.acknowledger.acknowledge(ack);
      }, 50);
    });

    await env.producer.enqueue({
      id: "non-stalling-job",
      payload: "i am not stalling",
      queue: "unstally-stall",
    });

    await env.producer.staleChecker.check();
    expect(env.errors).to.deep.equal([]);

    await delay(200);

    await env.producer.staleChecker.check();
    expect(env.errors).to.deep.equal([]);
  });

  describe("when a schedule jobs goes stale", () => {
    it("is re-scheduled", async () => {
      worker = await env.owl.createWorker(async (job, ack) => {
        // let job be stale
      });

      await env.producer.enqueue({
        id: "@cron",
        payload: "null",
        queue: "cron-job",
        schedule: {
          type: "every",
          meta: "1000",
        },
      });

      await delay(200);

      await env.producer.staleChecker.check();

      const job = await env.producer.findById("cron-job", "@cron");
      expect(job).not.to.be.null;
      expect(+job.runAt).to.be.greaterThan(Date.now());
    });
  });
});
