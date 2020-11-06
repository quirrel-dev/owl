import { expect } from "chai";
import { makeActivityEnv } from "./support";

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function test(backend: "Redis" | "In-Memory") {
  describe(backend + " > Activity", () => {
    const env = makeActivityEnv(backend === "In-Memory");

    beforeEach(env.setup);
    afterEach(env.teardown);

    it("publishes all relevant information", async () => {
      await delay(50);

      const currentDate = new Date()

      await env.producer.enqueue({
        queue: "activity-queue",
        id: "a",
        payload: "lol",
        runAt: currentDate
      });

      await env.producer.enqueue({
        queue: "activity-queue",
        id: "b",
        payload: "lol",
        runAt: new Date(9999999999999),
      });

      await env.producer.delete("activity-queue", "b");

      await delay(50);

      expect(env.events).to.have.deep.members([
        {
          type: "scheduled",
          job: {
            queue: "activity-queue",
            id: "a",
            payload: "lol",
            runAt: currentDate,
            count: 1,
            schedule: undefined,
            times: undefined
          },
        },
        {
          type: "scheduled",
          job: {
            queue: "activity-queue",
            id: "b",
            payload: "lol",
            runAt: new Date(9999999999999),
            count: 1,
            schedule: undefined,
            times: undefined
          },
        },
        {
          type: "deleted",
          queue: "activity-queue",
          id: "b",
        },
        {
          type: "requested",
          queue: "activity-queue",
          id: "a",
        },
        {
          type: "acknowledged",
          queue: "activity-queue",
          id: "a",
        },
      ]);
    });
  });
}

test("Redis");
test("In-Memory");
