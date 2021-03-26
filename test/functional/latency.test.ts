import { expect, AssertionError } from "chai";
import { againstAllBackends } from "../util";
import { makeWorkerEnv } from "./support";

function sum(nums: number[]) {
  return nums.reduce((curr, acc) => curr + acc, 0);
}

function average(nums: number[]) {
  return sum(nums) / nums.length;
}

function removeFirstStackLine(string: string): string {
  return string.replace(/\n.*\n/, "");
}

export function waitUntil(
  predicate: () => boolean,
  butMax: number,
  interval = 50
) {
  const potentialError = new AssertionError(
    `Predicate was not fulfilled on time (${predicate.toString()})`,
    {
      showDiff: false,
    }
  );
  potentialError.stack = removeFirstStackLine(potentialError.stack);

  return new Promise<void>((resolve, reject) => {
    const check = setInterval(() => {
      if (predicate()) {
        clearInterval(check);
        clearTimeout(max);
        resolve();
      }
    }, interval);

    const max = setTimeout(() => {
      clearInterval(check);
      reject(potentialError);
    }, butMax);
  });
}

againstAllBackends("Latency", (backend) => {
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
        backend === "Redis" ? 500 : 6000
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
