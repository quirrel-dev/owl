import { expect } from "chai";
import EventEmitter from "events";
import { ParallelWorker } from "./parallel-worker";

export function makeBlocker() {
  const emitter = new EventEmitter();

  async function signal(forName: string = "") {
    emitter.emit(forName);
    await new Promise(setImmediate);
  }

  signal.block = (name: string = "") => {
    return new Promise<void>((resolve) => {
      emitter.on(name, () => {
        resolve();
      });
    });
  };

  return signal;
}

function tick() {
  return new Promise(setImmediate);
}

describe(ParallelWorker.name, () => {
  it("works with a normal flow", async () => {
    let callNr = 0;
    const finish = makeBlocker();
    const worker = new ParallelWorker(async () => {
      callNr++;

      await finish.block("" + callNr);

      switch (callNr) {
        case 1:
          return "empty";
        case 2:
          return "success";
        case 3:
          return "empty";
      }

      throw new Error("No Way!");
    });

    let setTimeoutCalls = 0;
    worker.setTimeout = () => {
      setTimeoutCalls++;
      return null as any;
    };

    worker.queueCouldContainSomething();
    expect(worker.status).to.eql({
      workingOn: 1,
      packed: false,
      queueAssumed: "filled",
    });
    expect(setTimeoutCalls).to.eq(1);

    await finish("1");
    expect(worker.status).to.eql({
      workingOn: 0,
      packed: false,
      queueAssumed: "empty",
    });

    worker.queueCouldContainSomething();
    expect(worker.status).to.eql({
      workingOn: 1,
      packed: false,
      queueAssumed: "filled",
    });

    await finish("2");
    expect(worker.status).to.eql({
      workingOn: 1,
      packed: false,
      queueAssumed: "filled",
    });

    await finish("3");
    expect(worker.status).to.eql({
      workingOn: 0,
      packed: false,
      queueAssumed: "empty",
    });
  });

  describe("autocheck", () => {
    it("works", async () => {
      let workerCalls = 0;
      const worker = new ParallelWorker(async () => {
        workerCalls++;
        return "empty";
      });

      let setTimeoutCalls = 0;
      const runAutoCheck = makeBlocker();
      worker.setTimeout = (cb) => {
        setTimeoutCalls++;
        runAutoCheck.block().then(cb);
        return null as any;
      };

      worker.queueCouldContainSomething();
      await tick();
      expect(worker.status).to.eql({
        workingOn: 0,
        packed: false,
        queueAssumed: "empty",
      });
      expect(workerCalls).to.eq(1);
      expect(setTimeoutCalls).to.eq(1);

      await runAutoCheck();
      expect(workerCalls).to.eq(2);
      expect(setTimeoutCalls).to.eq(2);
    });
  });

  describe("retry", () => {
    it("works", async () => {
      let workerCalls = 0;
      let delayRetry = makeBlocker();
      const worker = new ParallelWorker(async () => {
        workerCalls++;

        switch (workerCalls) {
          case 1:
            return ["retry", delayRetry.block("done")];
          case 2:
            return "success";
          default:
            return "empty";
        }
      });

      worker.queueCouldContainSomething();
      await tick();
      expect(workerCalls).to.eq(1);
      expect(worker.status).to.eql({
        workingOn: 0,
        packed: false,
        queueAssumed: "waiting",
      });

      await delayRetry("done");

      expect(worker.status).to.eql({
        workingOn: 0,
        packed: false,
        queueAssumed: "waiting",
      });
    });
  });
});
