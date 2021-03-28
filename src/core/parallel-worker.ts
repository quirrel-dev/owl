import { Closable } from "../Closable";

type QueueAssumption = "empty" | "filled" | "waiting";

interface ParallelWorkerStatus {
  workingOn: number;
  packed: boolean;
  queueAssumed: QueueAssumption;
}

export class ParallelWorker implements Closable {
  assumption: QueueAssumption = "empty";

  workingOn = new Set<Promise<unknown>>();

  get isPacked() {
    return this.workingOn.size >= this.maxParallel;
  }

  constructor(
    private readonly run: () => Promise<
      "empty" | "success" | [type: "retry", after: Promise<void>]
    >,
    public readonly maxParallel: number = 100,
    private readonly autoCheckEvery: number = 1000
  ) {}

  autoCheckId?: NodeJS.Timeout;

  // DI for testing
  setTimeout: (cb: () => void, timeout: number) => NodeJS.Timeout =
    global.setTimeout;

  private delayAutoCheck() {
    if (this.autoCheckId) {
      clearInterval(this.autoCheckId);
    }

    this.autoCheckId = this.setTimeout(
      () => this.queueCouldContainSomething(),
      this.autoCheckEvery
    );
  }

  public queueCouldContainSomething() {
    this.delayAutoCheck();
    this.assumption = "filled";

    const promise = this.run();
    this.workingOn.add(promise);
    promise
      .then((result) => {
        this.workingOn.delete(promise);
        if (result === "empty") {
          this.assumption = "empty";
          return;
        }

        if (result === "success") {
          this.assumption = "filled";
          this.queueCouldContainSomething();
          return;
        }

        const [, delayPromise] = result;
        this.assumption = "waiting";
        delayPromise.then(() => this.queueCouldContainSomething());
      })
      .catch((error) => {
        console.error(error);
      });
  }

  get status(): ParallelWorkerStatus {
    return {
      packed: this.isPacked,
      workingOn: this.workingOn.size,
      queueAssumed: this.assumption,
    };
  }

  close(): void | Promise<void> {
    throw new Error("Method not implemented.");
  }
}
