import { expect } from "chai";
import { describeAcrossBackends } from "../util";
import { delay, makeProducerEnv } from "./support";

describeAcrossBackends("stale-check", (backend) => {
  const env = makeProducerEnv(backend, {
    staleChecker: {
      interval: "manual",
      staleAfter: 1000,
    },
  });
  beforeEach(env.setup);
  afterEach(env.teardown);

  it("emits errors for stalling jobs", async () => {
    const worker = env.owl.createWorker(async () => {
      // happily takes jobs, but never acknowledges any of them
      // simulating a dying worker
    });

    await env.producer.enqueue({
      id: "stalling-job",
      payload: "i am stalling, just like susanne",
      queue: "stally-stall",
    });

    await env.producer.staleChecker.check();
    expect(env.errors).to.deep.equal([]);

    await delay(1500);

    await env.producer.staleChecker.check();
    expect(env.errors).to.deep.equal([
      [
        {
          jobId: "stalling-job",
          queueId: "stally-stall",
          timestampForNextRetry: undefined,
        },
        "Job Timed Out",
      ],
    ]);

    await worker.close();
  });

  it("reschedules jobs with retry", async () => {
    let calls = 0;
    const worker = env.owl.createWorker(async (job, ack) => {
      calls++;
      if (job.count > 1) {
        await worker.acknowledger.acknowledge(ack);
      }
    });

    await env.producer.enqueue({
      id: "retryable-stalling-job",
      payload: "i am stalling, just like susanne",
      queue: "retry-stally-stall",
      retry: [100],
    });

    await env.producer.staleChecker.check();
    expect(env.errors).to.deep.equal([]);

    await delay(1100);

    await env.producer.staleChecker.check();
    expect(env.errors).to.deep.equal([]);

    await delay(300);

    expect(calls).to.eq(2);

    await worker.close();
  });

  it("does not emit errors if everything is fine", async () => {
    const worker = env.owl.createWorker(async (job, ack) => {
      setTimeout(() => {
        worker.acknowledger.acknowledge(ack);
      }, 500);
    });

    await env.producer.enqueue({
      id: "non-stalling-job",
      payload: "i am not stalling",
      queue: "unstally-stall",
    });

    await env.producer.staleChecker.check();
    expect(env.errors).to.deep.equal([]);

    await delay(1500);

    await env.producer.staleChecker.check();
    expect(env.errors).to.deep.equal([]);

    await worker.close();
  });
});
