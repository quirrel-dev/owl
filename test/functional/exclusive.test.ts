import { expect } from "chai";
import { OnActivityEvent } from "../../src/activity/activity";
import { describeAcrossBackends } from "../util";
import { delay, makeActivityEnv } from "./support";

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

      await delay(50);

      expectInOrder([
        eventIndex("requested", "a"),
        eventIndex("requested", "b"),
        eventIndex("acknowledged", "a"),
        eventIndex("acknowledged", "b"),
      ]);
    });
  });

  describe("exclusive: true", () => {
    it("executes jobs in serial", async () => {
      await env.producer.enqueue({
        id: "a",
        payload: "abcde",
        queue: "my-queue",
        exclusive: true,
      });
      await env.producer.enqueue({
        id: "b",
        payload: "abcde",
        queue: "my-queue",
        exclusive: true,
      });

      await delay(50);

      expectInOrder([
        eventIndex("requested", "a"),
        eventIndex("acknowledged", "a"),
        eventIndex("requested", "b"),
        eventIndex("acknowledged", "b"),
      ]);
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

      await delay(50);

      expectInOrder([
        eventIndex("requested", "a"),
        eventIndex("acknowledged", "a"),
        eventIndex("requested", "b"),
        eventIndex("acknowledged", "b"),
      ]);
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

      await delay(200);

      expectInOrder([
        eventIndex("requested", "a"),
        eventIndex("acknowledged", "a"),
        eventIndex("requested", "b"),
        eventIndex("acknowledged", "b"),
        eventIndex("requested", "c"),
        eventIndex("acknowledged", "c"),
      ]);
    });
  });
});
