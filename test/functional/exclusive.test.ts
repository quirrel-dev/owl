import { expect } from "chai";
import { OnActivityEvent } from "../../src/activity/activity";
import { describeAcrossBackends, waitUntil } from "../util";
import { makeActivityEnv } from "./support";

function expectInOrder(numbers: number[]) {
  expect(numbers).to.not.contain(-1);
  expect(numbers).to.eql([...numbers].sort((a, b) => a - b));
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

  describe("exclusive: false", () => {
    it("executes jobs in parallel", async () => {
      await env.producer.enqueue({
        id: "a",
        payload: "block:10",
        queue: "my-queue",
        exclusive: false,
      });
      await env.producer.enqueue({
        id: "b",
        payload: "block:10",
        queue: "my-queue",
        exclusive: false,
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
    await waitUntilEvent("acknowledged", "b", 500);

    expectInOrder([
      eventIndex("requested", "a"),
      eventIndex("acknowledged", "a"),
      eventIndex("requested", "b"),
      eventIndex("acknowledged", "b"),
    ]);
  }

  describe("exclusive: true", () => {
    it("executes jobs in serial", async () => {
      await env.producer.enqueue({
        id: "a",
        payload: "block:100",
        queue: "my-queue",
        exclusive: true,
      });
      await env.producer.enqueue({
        id: "b",
        payload: "abcde",
        queue: "my-queue",
        exclusive: true,
      });

      const job = await env.producer.findById("my-queue", "b");
      expect(+job.runAt).to.be.closeTo(Date.now(), 1000);

      await expectToBeExecutedInSerial();
    });
  });

  describe("non-exclusive followed by exclusive", () => {
    it("executes jobs in serial", async () => {
      await env.producer.enqueue({
        id: "a",
        payload: "abcde",
        queue: "my-queue",
        exclusive: false,
      });

      await env.producer.enqueue({
        id: "b",
        payload: "abcde",
        queue: "my-queue",
        exclusive: true,
      });

      await expectToBeExecutedInSerial();
    });

    it("does not starve", async () => {
      await env.producer.enqueue({
        id: "a",
        payload: "block:50",
        queue: "my-queue",
        exclusive: false,
      });

      await env.producer.enqueue({
        id: "b",
        payload: "2",
        queue: "my-queue",
        exclusive: true,
      });

      await env.producer.enqueue({
        id: "c",
        payload: "2",
        queue: "my-queue",
        exclusive: false,
      });

      await waitUntilEvent("acknowledged", "c", 300);

      expectInOrder([
        eventIndex("requested", "a"),
        eventIndex("acknowledged", "a"),
        eventIndex("requested", "b"),
        eventIndex("acknowledged", "b"),
        eventIndex("requested", "c"),
        eventIndex("acknowledged", "c"),
      ]);
    });

    it("repro quirrel#717", async () => {
      const date1 = new Date(Date.now() + 50);
      const date2 = new Date(Date.now() + 100);
      const objects = [
        { id: "1", runAt: date1 },
        { id: "2", runAt: date1 },
        { id: "3", runAt: date2 },
        { id: "4", runAt: date2 },
      ];

      await Promise.all(
        objects.map(({ id, runAt }) =>
          env.producer.enqueue({
            id,
            payload: id,
            queue: "717-repro",
            runAt,
            override: true,
            exclusive: true,
          })
        )
      );

      await waitUntilEvent("acknowledged", "4", 200);

      expectInOrder([
        eventIndex("requested", "1"),
        eventIndex("acknowledged", "1"),
        eventIndex("requested", "2"),
        eventIndex("acknowledged", "2"),
        eventIndex("requested", "4"),
        eventIndex("acknowledged", "4"),
      ]);
    });
  });
});
