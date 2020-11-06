import { expect } from "chai";
import { makeWorkerEnv } from "./support";

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function test(backend: "Redis" | "In-Memory") {
  describe(backend + " > Schedule", () => {
    const env = makeWorkerEnv(backend === "In-Memory");

    beforeEach(env.setup);
    afterEach(env.teardown);

    describe("every 10 msec", () => {
      describe("without 'times' limit", () => {
        it("executes until deleted", async () => {
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
          const expectedExecutions = duration / 10;

          expect(env.jobs.length).to.be.closeTo(expectedExecutions, 1);

          expect(env.jobs.every(([, job], index) => job.count === index + 1)).to
            .be.true;

          const lengthBeforeDeletion = env.jobs.length;

          await env.producer.delete("scheduled-eternity", "a");

          await delay(100);

          const lengthAfterDeletion = env.jobs.length;

          expect(lengthAfterDeletion - lengthBeforeDeletion).to.be.closeTo(
            0,
            2
          );
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

          expect(env.jobs.length).to.equal(5);
        });
      });
    });
  });
}

test("Redis");
test("In-Memory");
