import { expect } from "chai";
import { makeProducerEnv } from "./support";

describe("Idempotency", () => {
  const env = makeProducerEnv();

  beforeEach(env.setup);
  afterEach(env.teardown);

  describe("when enqueueing an already-existant ID", () => {
    describe("and upsert = true", () => {
      it("replaces existing job", async () => {
        await env.producer.enqueue({
          queue: "upsert-true-queue",
          id: "a",
          payload: "1",
        });

        await env.producer.enqueue({
          queue: "upsert-true-queue",
          id: "a",
          payload: "2",
          upsert: true,
        });

        const job = await env.producer.findById("upsert-true-queue", "a");
        expect(job.payload).to.equal("2");
      });
    });

    describe("and upsert = false", () => {
      it("is a no-op", async () => {
        await env.producer.enqueue({
          queue: "upsert-false-queue",
          id: "a",
          payload: "1",
        });

        await env.producer.enqueue({
          queue: "upsert-false-queue",
          id: "a",
          payload: "2",
          upsert: false,
        });

        const job = await env.producer.findById("upsert-false-queue", "a");
        expect(job.payload).to.equal("1");
      });
    });
  });
});
