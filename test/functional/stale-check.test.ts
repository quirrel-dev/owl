import { expect } from "chai";
import { delay, makeProducerEnv } from "./support";

function testAgainst(backend: "Redis" | "In-Memory") {
  describe(backend + " > stale-check", () => {
    const owl = makeProducerEnv(backend === "In-Memory", {
      staleChecker: {
        interval: 60 * 1000,
        staleAfter: 1000,
      },
    });
    beforeEach(owl.setup);
    afterEach(owl.teardown);

    it("works", async () => {
      owl.owl.createWorker(async () => {
        // happily takes jobs, but never acknowledges any of them
        // simulating a dying worker
      });

      await owl.producer.enqueue({
        id: "stalling-job",
        payload: "i am stalling, just like susanne",
        queue: "stally-stall",
      });

      await owl.producer.staleChecker.check();
      expect(owl.errors).to.deep.equal([]);

      await delay(1500);

      await owl.producer.staleChecker.check();
      expect(owl.errors).to.deep.equal([
        [
          {
            jobId: "stalling-job",
            queueId: "stally-stall",
            timestampForNextRetry: undefined,
          },
          "Job Timed Out",
        ],
      ]);
    });
  });
}

testAgainst("Redis");
testAgainst("In-Memory");
