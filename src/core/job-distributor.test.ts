import { expect } from "chai";
import EventEmitter from "events";
import { JobDistributor } from "./job-distributor";

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

describe(JobDistributor.name, () => {
  it("fetches all available jobs", async () => {
    const availableJobs = ["a", "b", "c"];
    const workedJobs: string[] = [];

    const distributor = new JobDistributor<string>(
      async () => {
        if (availableJobs.length === 0) {
          return ["empty"];
        }

        return ["success", availableJobs.pop()!];
      },
      async (job) => {
        workedJobs.push(job);
      }
    );

    await distributor.checkForNewJobs();

    expect(workedJobs).to.eql(["c", "b", "a"]);
  });

  it("respects maxJob", async () => {
    const log: string[] = [];

    let fetchCount = 0;
    const blocker = makeBlocker();
    const distributor = new JobDistributor(
      async () => {
        fetchCount++;

        if (fetchCount > 4) {
          return ["empty"];
        }

        log.push("fetch:" + fetchCount);
        blocker("fetch:" + fetchCount);

        return ["success", "" + fetchCount];
      },
      async (job) => {
        await tick();
        log.push("work:" + job);
        blocker("work:" + job);
      },
      3
    );

    distributor.checkForNewJobs();

    await tick();

    expect(log).to.eql(["fetch:1", "fetch:2", "fetch:3"]);

    await blocker.block("work:1");
    await tick();

    expect(log).to.eql([
      "fetch:1",
      "fetch:2",
      "fetch:3",
      "work:1",
      "fetch:4",
      "work:2",
      "work:3",
    ]);
  });

  it("supports 'wait'", async () => {
    const log: string[] = [];

    let fetchCount = 0;
    const blocker = makeBlocker();
    const distributor = new JobDistributor(
      async () => {
        fetchCount++;

        switch (fetchCount) {
          case 1:
            return ["wait", blocker.block("wait")];
          case 2:
            return ["success", "1"];
          default:
            return ["empty"];
        }
      },
      async (job) => {
        log.push("work:" + job);
      }
    );

    distributor.checkForNewJobs();

    expect(log).to.eql([]);

    await blocker("wait");

    expect(log).to.eql(["work:1"]);
  });

  it("automatically fetches periodically", async () => {
    const log: string[] = [];

    let fetchCount = 0;
    const blocker = makeBlocker();
    const distributor = new JobDistributor(
      async () => {
        fetchCount++;
        log.push("fetch:" + fetchCount);
        return ["empty"];
      },
      async (job) => {}
    );

    distributor.setTimeout = (cb) => {
      blocker.block("timeout").then(cb);
      return null as any;
    };

    await distributor.checkForNewJobs();

    expect(log).to.eql(["fetch:1"]);

    await blocker("timeout");

    expect(log).to.eql(["fetch:1", "fetch:2"]);
  });

  describe("error handling", () => {
    describe("during fetching", () => {
      it("throws", async () => {
        const distributor = new JobDistributor(
          async () => {
            throw new Error("Fetch failed!");
          },
          async (job) => {}
        );

        distributor.setTimeout = (cb) => {
          return null as any;
        };

        try {
          await distributor.checkForNewJobs();

          expect(false).to.be.true;
        } catch (error) {
          expect(error.message).to.equal("Fetch failed!");
        }
      });
    });
    describe("during execution", () => {
      let oldError: any;
      const errors: any[][] = [];
      before(() => {
        oldError = global.console.error;
        global.console.error = (...args: any[]) => {
          errors.push(args.map(String));
        };
      });

      after(() => {
        global.console.error = oldError;
      });

      it("console.errors", async () => {
        let call = 0;
        const distributor = new JobDistributor(
          async () => {
            call++;
            if (call === 1) {
              return ["success", "job"];
            }
            return ["empty"];
          },
          async (job) => {
            console.log("Hello");
            throw new Error("Run failed!");
          }
        );

        distributor.setTimeout = (cb) => {
          return null as any;
        };

        await distributor.checkForNewJobs();

        expect(errors).to.eql([["Error: Run failed!"]]);
      });
    });
  });

  it("deals with errors during execution", () => {});
});
