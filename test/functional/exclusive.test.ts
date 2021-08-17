import { expect } from "chai";
import { OnActivityEvent } from "../../src/activity/activity";
import { describeAcrossBackends, waitUntil } from "../util";
import { makeActivityEnv } from "./support";

function expectInOrder(numbers: number[]) {
  expect(numbers).to.not.contain(-1);
  expect(numbers).to.eql([...numbers].sort());
}

describeAcrossBackends("Exclusive", (backend) => {
  const env = makeActivityEnv(backend);

  beforeEach(env.setup);
  afterEach(env.teardown);

  function eventIndex(type: OnActivityEvent["type"], id: string) {
    return env.events.findIndex((e) => {
      if (e.type === "scheduled") {
        return e.type == type && e.job.id === id;
      }

      return e.type === type && e.id === id;
    });
  }

  async function waitUntilEvent(
    type: OnActivityEvent["type"],
    id: string,
    maxWait: number = 100
  ) {
    await waitUntil(() => eventIndex(type, id) !== -1, maxWait);
  }

  describe("non-exclusive", () => {
    it("executes jobs in parallel", async () => {
      await env.producer.enqueue({
        id: "a",
        payload: "block:10",
        queue: "my-queue",
      });
      await env.producer.enqueue({
        id: "b",
        payload: "block:10",
        queue: "my-queue",
      });

      await waitUntilEvent("acknowledged", "b");

      expectInOrder([
        eventIndex("requested", "a"),
        eventIndex("requested", "b"),
        eventIndex("acknowledged", "a"),
        eventIndex("acknowledged", "b"),
      ]);
    });
  });

  async function expectToBeExecutedInSerial() {
    await waitUntilEvent("acknowledged", "b");

    expectInOrder([
      eventIndex("requested", "a"),
      eventIndex("acknowledged", "a"),
      eventIndex("requested", "b"),
      eventIndex("acknowledged", "b"),
    ]);
  }

  describe("exclusive", () => {
    it("executes jobs in serial", async () => {
      await env.producer.enqueue({
        id: "a",
        payload: "abcde",
        queue: "my-queue-exclusive",
      });
      await env.producer.enqueue({
        id: "b",
        payload: "abcde",
        queue: "my-queue-exclusive",
      });

      await expectToBeExecutedInSerial();
    });
  });
});
