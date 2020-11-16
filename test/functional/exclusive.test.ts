import { expect } from "chai";
import { makeActivityEnv } from "./support";

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function expectInOrder(numbers: number[]) {
  expect(numbers).to.eql([...numbers].sort());
}

function test(backend: "Redis" | "In-Memory") {
  describe(backend + " > Exclusive", () => {
    const env = makeActivityEnv(backend === "In-Memory");

    beforeEach(env.setup);
    afterEach(env.teardown);

    describe("exclusive: false", () => {
      it("executes jobs in parallel", async () => {
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
          exclusive: false,
        });

        await delay(50);

        const indexOfARequested = env.events.findIndex(
          (e) => e.type === "requested" && e.id === "a"
        );
        const indexOfBRequested = env.events.findIndex(
          (e) => e.type === "requested" && e.id === "b"
        );

        const indexOfAAcknowledged = env.events.findIndex(
          (e) => e.type === "acknowledged" && e.id === "a"
        );
        const indexOfBAcknowledged = env.events.findIndex(
          (e) => e.type === "acknowledged" && e.id === "b"
        );

        expectInOrder([
          indexOfARequested,
          indexOfBRequested,
          indexOfAAcknowledged,
          indexOfBAcknowledged,
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

        const indexOfARequested = env.events.findIndex(
          (e) => e.type === "requested" && e.id === "a"
        );
        const indexOfBRequested = env.events.findIndex(
          (e) => e.type === "requested" && e.id === "b"
        );

        const indexOfAAcknowledged = env.events.findIndex(
          (e) => e.type === "acknowledged" && e.id === "a"
        );
        const indexOfBAcknowledged = env.events.findIndex(
          (e) => e.type === "acknowledged" && e.id === "b"
        );

        expectInOrder([
          indexOfARequested,
          indexOfAAcknowledged,
          indexOfBRequested,
          indexOfBAcknowledged,
        ]);
      });
    });
  });
}

test("Redis");
test("In-Memory");
