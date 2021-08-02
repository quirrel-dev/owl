import { expect } from "chai";
import { makeActivityEnv } from "./support";
import { delay, describeAcrossBackends, makeSignal, waitUntil } from "../util";

describeAcrossBackends("Retry", (backend) => {
  const env = makeActivityEnv(backend, (job) => {
    if (job.payload === "thou shalt succeed") {
      return false;
    }

    throw new Error("failing!");
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
      const finishedAllRetries = makeSignal();
      env.onStartedJob((job) => {
        if (job.count === 3) {
          finishedAllRetries.signal();
        }
      });
      await env.producer.enqueue({
        queue: "scheduled-eternity",
        id: "a",
        payload: "a",
        retry: [10, 100, 200],
      });

      await finishedAllRetries;

      const retryCycle = ["requested", "retry", "acknowledged", "rescheduled"];

      const expectedTypes = [
        "scheduled",
        ...retryCycle,
        ...retryCycle,
        ...retryCycle,
        "requested",
        "fail",
        "acknowledged",
      ];

      await waitUntil(() => env.events.length === expectedTypes.length, 300);

      expect(env.events.map((e) => e.type)).to.deep.equal(expectedTypes);

      expect(env.errors).to.have.length(1);

      const executionDates = env.jobs.map(([executionDate]) => executionDate);
      expect(executionDates).to.have.length(4);
      const firstDuration = executionDates[1] - executionDates[0];
      const secondDuration = executionDates[2] - executionDates[0];
      const thirdDuration = executionDates[3] - executionDates[0];

      if (backend === "Redis") {
        expect(firstDuration).to.be.within(5, 25);
        expect(secondDuration).to.be.within(80, 120);
        expect(thirdDuration).to.be.within(180, 220);
      } else {
        expect(firstDuration).to.be.within(5, 50);
        expect(secondDuration).to.be.within(50, 150);
        expect(thirdDuration).to.be.within(150, 250);
      }

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

    describe("overriding a job that's retried", () => {
      it("prevents future retries", async () => {
        const finishedFirstRetry = makeSignal();
        const finishedOverride = makeSignal();

        env.onStartedJob((job) => {
          if (job.count === 1) {
            finishedFirstRetry.signal();
          }

          if (job.payload === "b") {
            finishedOverride.signal();
          }
        });

        await env.producer.enqueue({
          queue: "retry-override",
          id: "a",
          payload: "a",
          retry: [100, 200],
        });

        await finishedFirstRetry;
        await delay(10);

        await env.producer.enqueue({
          queue: "retry-override",
          id: "a",
          payload: "b",
          override: true,
        });

        await finishedOverride;

        await delay(400);

        expect(env.events.map((e) => e.type)).to.deep.equal([
          "scheduled", // job a ...
          "requested", // .. begins work ...
          "retry", // ... fails ...
          "acknowledged",
          "rescheduled", // ... so it's rescheduled!
          "scheduled", // job b overrides it
          "requested",
          "fail", // it doesn't have retry, so it "fails"
          "acknowledged",
          // no other executions
        ]);
      });
    });
  });
});
