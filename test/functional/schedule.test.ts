import { expect } from "chai";
import { delay, describeAcrossBackends } from "../util";
import { makeWorkerEnv } from "./support";

describeAcrossBackends("Schedule", (backend) => {
  const env = makeWorkerEnv(backend);

  beforeEach(env.setup);
  afterEach(env.teardown);

  describe("every 10 msec", () => {
    describe("without 'times' limit", () => {
      it("executes until deleted", async () => {
        const start = Date.now();

        await env.producer.enqueue({
          tenant: "",
          queue: "scheduled-eternity",
          id: "a",
          payload: "a",
          schedule: {
            type: "every",
            meta: "10",
          },
        });

        await delay(100);

        const end = Date.now();

        const duration = end - start;
        const expectedExecutions = duration / 10;

        expect(env.jobs.length).to.be.closeTo(
          expectedExecutions,
          backend === "Redis" ? 1.5 : 8
        );
        expect(env.nextExecDates.every((value) => typeof value === "number")).to
          .be.true;

        expect(env.jobs.every(([, job], index) => job.count === index + 1)).to
          .be.true;

        const lengthBeforeDeletion = env.jobs.length;

        await env.producer.delete("", "scheduled-eternity", "a");

        await delay(100);

        const lengthAfterDeletion = env.jobs.length;

        expect(lengthAfterDeletion - lengthBeforeDeletion).to.be.closeTo(0, 2);
      });
    });

    describe("with 'times' limit", () => {
      it("executes specified amount of times", async () => {
        await env.producer.enqueue({
          tenant: "",
          queue: "scheduled-times",
          id: "a",
          payload: "a",
          schedule: {
            type: "every",
            meta: "10",
            times: 5,
          },
        });

        await delay(100);

        expect(
          env.jobs.filter(([, job]) => job.queue === "scheduled-times").length
        ).to.equal(5);
      });
    });
  });
});
