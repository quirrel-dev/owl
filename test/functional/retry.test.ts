import { expect } from "chai";
import { makeActivityEnv } from "./support";

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function test(backend: "Redis" | "In-Memory") {
  describe(backend + " > Retry", () => {
    const env = makeActivityEnv(backend === "In-Memory", () => true);

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
        expect(executionDates[1] - executionDates[0]).to.be.within(5, 15);
        expect(executionDates[2] - executionDates[0]).to.be.within(90, 110);
        expect(executionDates[3] - executionDates[0]).to.be.within(190, 210);

        const counts = env.jobs.map(([, job]) => job.count);
        expect(counts).to.eql([1, 2, 3, 4]);
      });
    });
  });
}

test("Redis");
test("In-Memory");
