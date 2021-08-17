import { expect } from "chai";
import { describeAcrossBackends, waitUntil } from "../util";
import { makeActivityEnv } from "./support";

describeAcrossBackends("Activity", (backend) => {
  const env = makeActivityEnv(backend);

  beforeEach(env.setup);
  afterEach(env.teardown);

  async function expectEventsToEventuallyMatch(
    members: any[],
    maxWait: number
  ) {
    await waitUntil(() => {
      return env.events.length === members.length;
    }, maxWait);
    expect(env.events).to.have.deep.members(members);
  }

  it("publishes all relevant information", async () => {
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

    await expectEventsToEventuallyMatch(
      [
        {
          type: "scheduled",
          job: {
            queue: "activity:queue",
            id: "a",
            payload: '{"lol":"lel"}',
            runAt: currentDate,
            count: 1,
            schedule: undefined,

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
      ],
      200
    );
  });
});
