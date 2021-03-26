import { expect } from "chai";
import { makeActivityEnv } from "./support";
import { makeSignal } from "./dont-reschedule.test";
import { waitUntil } from "./latency.test";

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function test(backend: "Redis" | "In-Memory") {
  describe(backend + " > Retry", () => {
    const env = makeActivityEnv(backend === "In-Memory", (job) => {
      if (job.payload === "thou shalt succeed") {
        return false;
      }

      return true;
    });

    beforeEach(env.setup);
    afterEach(env.teardown);

    describe("when given both retry and schedule", () => {
      it("throws", async () => {
        try {
          await env.producer.enqueue({
            queue: "scheduled-eternity",
            id: "a",
            payload: "a",
            retry: [10, 100, 1000],
            schedule: {
              type: "every",
              meta: "100",
            },
          });

          expect(true).to.be.false;
        } catch (error) {
          expect(error.message).to.equal(
            "retry and schedule cannot be used together"
          );
        }
      });
    });

    describe("when given retry: [10, 100, 1000]", () => {
      it("retries along that schedule", async () => {
        await delay(10);

        await env.producer.enqueue({
          queue: "scheduled-eternity",
          id: "a",
          payload: "a",
          retry: [10, 100, 200],
        });

        await delay(500);

        const retryCycle = [
          "requested",
          "retry",
          "acknowledged",
          "rescheduled",
        ];

        expect(env.events.map((e) => e.type)).to.deep.equal([
          "scheduled",
          ...retryCycle,
          ...retryCycle,
          ...retryCycle,
          "requested",
          "fail",
          "acknowledged",
        ]);

        expect(env.errors).to.have.length(1);

        const executionDates = env.jobs.map(([executionDate]) => executionDate);
        expect(executionDates).to.have.length(4);
        expect(executionDates[1] - executionDates[0]).to.be.within(5, 20);
        expect(executionDates[2] - executionDates[0]).to.be.within(80, 120);
        expect(executionDates[3] - executionDates[0]).to.be.within(180, 220);

        const counts = env.jobs.map(([, job]) => job.count);
        expect(counts).to.eql([1, 2, 3, 4]);
      });

      describe("and jobs dont fail", () => {
        it("executes only once", async function () {
          const finished = makeSignal();
          env.onFinishedJob(finished.signal);

          await env.producer.enqueue({
            queue: "scheduled-eternity",
            id: "a",
            payload: "thou shalt succeed",
            retry: [10, 100, 200],
          });

          await finished;

          await waitUntil(() => env.events.length === 3, 100);

          expect(env.events.map((e) => e.type)).to.deep.equal([
            "scheduled",
            "requested",
            "acknowledged",
          ]);
        });
      });
    });
  });
}

test("Redis");
test("In-Memory");
