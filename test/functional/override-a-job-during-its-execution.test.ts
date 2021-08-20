import { expect } from "chai";
import { makeWorkerEnv } from "./support";
import { delay, describeAcrossBackends, makeSignal } from "../util";

describeAcrossBackends("Override during Execution", (backend) => {
  const firstJobWasFinished = makeSignal();
  let executions = 0;
  let executionResult;
  const env = makeWorkerEnv(backend, async (job) => {
    if (job.payload === "enqueue-second") {
      executionResult = await env.producer.enqueue({
        queue: job.queue,
        id: job.id,
        payload: "second",
        override: true,
      });
      firstJobWasFinished.signal();
    }

    executions++;

    return false;
  });
  beforeEach(env.setup);
  afterEach(env.teardown);

  describe("when job is overriden while being executed (repro quirrel#739)", () => {
    it("is tossed", async () => {
      const queue = "doesntstopnext";
      const id = "foo";
      await env.producer.enqueue({
        queue,
        id,
        payload: "enqueue-second",
      });

      await firstJobWasFinished;

      await delay(100);

      expect(executions).to.equal(1);
      expect(executionResult).to.equal("is_in_execution");
    });
  });
});
