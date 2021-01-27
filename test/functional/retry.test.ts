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

    describe("when given an empty array", () => {
      it("throws", async () => {
        try {
          await env.producer.enqueue({
            queue: "scheduled-eternity",
            id: "a",
            payload: "a",
            retry: [],
          });

          expect(true).to.be.false;
        } catch (error) {
          expect(error.message).to.equal("retry must not be empty");
        }
      });
    });

    describe("when given retry: [10, 100, 1000]", () => {
      it("retries along that schedule", async () => {
        await env.producer.enqueue({
          queue: "scheduled-eternity",
          id: "a",
          payload: "a",
          retry: [10, 100, 200],
        });

        await delay(500);

        expect(env.events.map((e) => e.type)).to.deep.equal([
          "scheduled",
          "requested",
          "fail",
          "acknowledged",
        ]);
        expect(env.jobs).to.equal([]);
      });
    });
  });
}

test("Redis");
test("In-Memory");
