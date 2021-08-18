import { expect } from "chai";
import { makeWorkerEnv } from "./support";
import { delay, describeAcrossBackends, makeSignal } from "../util";

describeAcrossBackends("Repro: Override during Execution", (backend) => {
  const executionStarted = makeSignal();
  const jobWasOverriden = makeSignal();
  const env = makeWorkerEnv(backend, async (job) => {
    if (job.payload === "wait") {
      executionStarted.signal();
      await jobWasOverriden;
    }

    return false;
  });
  beforeEach(env.setup);
  afterEach(env.teardown);

  describe("when job is overriden while being executed", () => {
    it("is executed along the new (overriden) schedule", async () => {
      const queue = "override-during";
      const id = "foo";
      await env.producer.enqueue({
        queue,
        id,
        schedule: { type: "every", meta: "100" },
        payload: "wait",
      });

      await executionStarted;

      const newRunAt = new Date(Date.now() + 10000000);
      await env.producer.enqueue({
        queue,
        id,
        runAt: newRunAt,
        override: true,
        payload: "",
      });

      jobWasOverriden.signal();
      await delay(10);

      const job = await env.producer.findById(queue, id);
      expect(+job.runAt).to.equal(+newRunAt);
    });
  });
});
