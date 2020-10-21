import RedisOwl, { MockOwl } from "../src";
import delay from "delay";

function Test(name: string, owl: RedisOwl) {
  describe(name, () => {
    test("queueing flow", async () => {
      const executedPayloads: string[] = [];

      const worker = owl.createWorker(async (job) => {
        executedPayloads.push(job.payload);
      });

      const producer = owl.createProducer();

      await producer.enqueue({
        id: "1234",
        queue: "bakery",
        payload: "bread",
      });

      await delay(10);

      expect(executedPayloads).toEqual(["bread"]);

      await producer.close();
      await worker.close();
    });

    test("a lot of queued jobs", async () => {
      let numberExecuted = 0;
      const worker = owl.createWorker(async () => {
        await delay(10);
        numberExecuted++;
      });

      const producer = owl.createProducer();

      for (let i = 0; i < 50; i++) {
        await producer.enqueue({
          id: "" + i,
          queue: "bakery",
          payload: "",
        });
      }

      await delay(200);

      expect(numberExecuted).toBe(50);

      await producer.close();
      await worker.close();
    });

    test("failing job", async () => {
      const worker = owl.createWorker(
        async (job) => {
          throw new Error("epic fail");
        },
        (job, err) => {
          expect(err.message).toBe("epic fail");
        }
      );

      const producer = owl.createProducer();

      await producer.enqueue({
        id: "1234",
        queue: "bakery",
        payload: "bread",
      });

      await delay(10);

      await producer.close();
      await worker.close();
    });
  });
}

// Test("Redis", RedisOwl);
Test("Mocked", new MockOwl());
