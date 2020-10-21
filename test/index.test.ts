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
  });
}

// Test("Redis", RedisOwl);
Test("Mocked", new MockOwl());
