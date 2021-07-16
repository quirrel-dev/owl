import { expect } from "chai";
import { describeAcrossBackends, waitUntil } from "../util";
import { makeWorkerEnv } from "./support";

function sum(nums: number[]) {
  return nums.reduce((curr, acc) => curr + acc, 0);
}

function average(nums: number[]) {
  return sum(nums) / nums.length;
}

describeAcrossBackends("Latency", (backend) => {
  const env = makeWorkerEnv(backend);

  beforeEach(env.setup);
  afterEach(env.teardown);

  describe("when inserting 1000 jobs", () => {
    it("they're executed in 1 sec", async function () {
      if (backend === "In-Memory") {
        this.timeout(15 * 1000);
      }

      const enqueueals: Promise<any>[] = [];
      for (let i = 0; i < 1000; i++) {
        enqueueals.push(
          env.producer.enqueue({
            id: "" + i,
            payload: "" + Date.now(),
            queue: "latency",
          })
        );
      }

      await Promise.all(enqueueals);

      await waitUntil(
        () => env.jobs.length === 1000,
        backend === "Redis" ? 500 : 7000
      );

      expect(env.jobs).to.be.length(1000);
      expect(env.nextExecDates.every((value) => typeof value === "undefined"))
        .to.be.true;

      const delays = env.jobs.map(
        ([execTime, { payload }]) => execTime - +payload
      );

      if (backend === "Redis") {
        expect(Math.min(...delays)).to.be.below(300);
        expect(average(delays)).to.be.below(700);
        expect(Math.max(...delays)).to.be.below(1100);
      }
    });
  });
});
