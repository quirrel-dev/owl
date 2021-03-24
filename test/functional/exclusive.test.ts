import { expect } from "chai";
import { delay, makeActivityEnv } from "./support";

function expectInOrder(numbers: number[]) {
  expect(numbers).to.not.contain(-1);
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

        await delay(100);

        const indexOfARequested = env.events.findIndex(
          (e) => e.type === "requested" && e.id === "a"
        );
        const indexOfBRequested = env.events.findIndex(
          (e) => e.type === "requested" && e.id === "b"
        );
        const indexOfCRequested = env.events.findIndex(
          (e) => e.type === "requested" && e.id === "c"
        );

        const indexOfAAcknowledged = env.events.findIndex(
          (e) => e.type === "acknowledged" && e.id === "a"
        );
        const indexOfBAcknowledged = env.events.findIndex(
          (e) => e.type === "acknowledged" && e.id === "b"
        );
        const indexOfCAcknowledged = env.events.findIndex(
          (e) => e.type === "acknowledged" && e.id === "c"
        );
        expectInOrder([
          indexOfARequested,
          indexOfAAcknowledged,
          indexOfBRequested,
          indexOfBAcknowledged,
          indexOfCRequested,
          indexOfCAcknowledged,
        ]);
      });
    });
  });
}

test("Redis");
test("In-Memory");
