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

      const currentDate = new Date();

      await env.producer.enqueue({
        queue: "activity:queue",
        id: "a",
        payload: '{"lol":"lel"}',
        runAt: currentDate,
      });

      await env.producer.enqueue({
        queue: "activity:queue",
        id: "b;wild",
        payload: "lol",
        runAt: new Date(9999999999999),
        exclusive: true,
      });

      await env.producer.enqueue({
        queue: "activity:queue",
        id: "repeated",
        payload: "lol",
        runAt: currentDate,
        schedule: {
          type: "every",
          meta: "10",
          times: 2,
        },
      });

      await env.producer.delete("activity:queue", "b;wild");

      await delay(50);

      expect(env.events).to.have.deep.members([
        {
          type: "scheduled",
          job: {
            queue: "activity:queue",
            id: "a",
            payload: '{"lol":"lel"}',
            runAt: currentDate,
            count: 1,
            schedule: undefined,
            exclusive: false,
            retry: [],
          },
        },
        {
          type: "scheduled",
          job: {
            queue: "activity:queue",
            id: "b;wild",
            payload: "lol",
            runAt: new Date(9999999999999),
            count: 1,
            schedule: undefined,
            exclusive: true,
            retry: [],
          },
        },
        {
          type: "scheduled",
          job: {
            queue: "activity:queue",
            id: "repeated",
            payload: "lol",
            count: 1,
            runAt: currentDate,
            exclusive: false,
            retry: [],
            schedule: {
              type: "every",
              meta: "10",
              times: 2,
            },
          },
        },
        {
          type: "deleted",
          queue: "activity:queue",
          id: "b;wild",
        },
        {
          type: "requested",
          queue: "activity:queue",
          id: "a",
        },
        {
          type: "requested",
          queue: "activity:queue",
          id: "repeated",
        },
        {
          type: "acknowledged",
          queue: "activity:queue",
          id: "a",
        },
        {
          type: "acknowledged",
          queue: "activity:queue",
          id: "repeated",
        },
        {
          type: "rescheduled",
          queue: "activity:queue",
          runAt: new Date(+currentDate + 10),
          id: "repeated",
        },
        {
          type: "requested",
          queue: "activity:queue",
          id: "repeated",
        },
        {
          type: "acknowledged",
          queue: "activity:queue",
          id: "repeated",
        },
      ]);
    });
  });
}

test("Redis");
test("In-Memory");
