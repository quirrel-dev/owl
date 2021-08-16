import { expect } from "chai";
import { delay, describeAcrossBackends } from "../util";
import { makeWorkerEnv } from "./support";

describeAcrossBackends("Schedule", (backend) => {
  const env = makeWorkerEnv(backend, (job) => {
    return job.payload === "reportFailure";
  });

  beforeEach(env.setup);
  afterEach(env.teardown);

  describe("every 10 msec", () => {
    describe("without 'times' limit", () => {
      it.only("executes until deleted", async () => {
        const start = Date.now();

        await env.producer.enqueue({
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

        expect(env.jobs.length).to.be.gt(3); // cant estimate better b/c of gatekeeper
        expect(env.nextExecDates.every((value) => typeof value === "number")).to
          .be.true;

          console.log(env.jobs)
        expect(env.jobs.every(([, job], index) => job.count === index + 1)).to
          .be.true;

        const lengthBeforeDeletion = env.jobs.length;

        await env.producer.delete("scheduled-eternity", "a");

        await delay(100);

        const lengthAfterDeletion = env.jobs.length;

        expect(lengthAfterDeletion - lengthBeforeDeletion).to.be.closeTo(0, 2);
      });

      describe("when failing", () => {
        it("gets rescheduled", async () => {
          await env.producer.enqueue({
            queue: "scheduled-eternity",
            id: "a",
            payload: "reportFailure",
            schedule: {
              type: "every",
              meta: "10",
            },
          });

          await delay(100);

          expect(await env.producer.findById("scheduled-eternity", "a")).not.to
            .be.null;
        });
      });
    });

    describe("with 'times' limit", () => {
      it("executes specified amount of times", async () => {
        await env.producer.enqueue({
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
