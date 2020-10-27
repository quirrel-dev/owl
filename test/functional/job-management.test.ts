import { expect } from "chai";
import { makeProducerEnv } from "./support";

function test(backend: "Redis" | "In-Memory") {
  describe(backend + " > job management", () => {
    const env = makeProducerEnv(backend === "In-Memory");
    beforeEach(env.setup);

    afterEach(env.teardown);

    describe("Producer#scanQueue", () => {
      it("returns pending jobs", async () => {
        await env.producer.enqueue({
          queue: "producer-scan-queue",
          id: "a",
          payload: "a",
          runAt: new Date("2020-10-27T07:36:56.321Z"),
        });

        await env.producer.enqueue({
          queue: "producer-scan-queue",
          id: "b",
          payload: "b",
          runAt: new Date("2020-10-27T07:36:56.321Z"),
        });

        const { jobs, newCursor } = await env.producer.scanQueue(
          "producer-scan-queue",
          0
        );

        expect(jobs).to.have.deep.members([
          {
            queue: "producer-scan-queue",
            id: "b",
            payload: "b",
            runAt: new Date("2020-10-27T07:36:56.321Z"),
            schedule: undefined,
            count: 1,
            times: undefined,
          },
          {
            queue: "producer-scan-queue",
            id: "a",
            payload: "a",
            runAt: new Date("2020-10-27T07:36:56.321Z"),
            schedule: undefined,
            count: 1,
            times: undefined,
          },
        ]);

        expect(newCursor).to.eq(0);
      });
    });

    describe("Producer#findById", () => {
      it("returns the right job", async () => {
        await env.producer.enqueue({
          queue: "producer-find-by-id",
          id: "my-random-id",
          payload: "lol",
          runAt: new Date("2020-10-27T07:36:56.321Z"),
        });

        const job = await env.producer.findById(
          "producer-find-by-id",
          "my-random-id"
        );

        expect(job).to.deep.eq({
          queue: "producer-find-by-id",
          id: "my-random-id",
          payload: "lol",
          runAt: new Date("2020-10-27T07:36:56.321Z"),
          schedule: undefined,
          count: 1,
          times: undefined,
        });
      });

      describe("when giving non-existing ID", () => {
        it("returns null", async () => {
          const job = await env.producer.findById(
            "producer-find-by-id",
            "my-random-id"
          );

          expect(job).to.be.null;
        });
      });
    });

    describe("Producer#invoke", () => {
      it("moves job to be executed immediately", async () => {
        await env.producer.enqueue({
          queue: "producer-invoke",
          id: "a",
          payload: "a",
          runAt: new Date("1970-10-27T07:36:56.321Z"),
        });

        const job = await env.producer.findById("producer-invoke", "a");
        expect(+job.runAt).to.equal(+new Date("1970-10-27T07:36:56.321Z"));

        await env.producer.invoke("producer-invoke", "a");

        const invokedJob = await env.producer.findById("producer-invoke", "a");
        expect(+invokedJob.runAt).to.be.eq(0);
      });
    });

    describe("Producer#delete", () => {
      it("deletes pending job", async () => {
        await env.producer.enqueue({
          queue: "producer-delete",
          id: "a",
          payload: "a",
        });

        await env.producer.delete("producer-delete", "a");

        const job = await env.producer.findById("producer-delete", "a");
        expect(job).to.be.null;
      });
    });
  });
}

test("Redis");
test("In-Memory");
