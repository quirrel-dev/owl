import { Logger } from "pino";
import { Closable } from "../Closable";

export class JobDistributor<T> implements Closable {
  private readonly jobs = new Set<T>();

  private isClosed = false;

  get load() {
    return this.jobs.size;
  }

  get isPacked() {
    return this.jobs.size >= this.maxJobs;
  }

  constructor(
    private readonly fetch: () => Promise<
      ["empty"] | ["retry"] | ["success", T] | ["wait", number]
    >,
    private readonly run: (job: T) => Promise<void>,
    private readonly logger?: Logger,
    public readonly maxJobs: number = 100,
    private readonly autoCheckEvery: number = 1000
  ) {}

  public async start() {
    await this.checkForNewJobs();
  }

  private async workOn(job: T) {
    this.jobs.add(job);

    try {
      this.logger?.trace({ job }, "Distributor: Starting work on job");
      await this.run(job);
      this.logger?.trace({ job }, "Distributor: Finished work on job");
    } catch (e) {
      this.logger?.error(e as Error);
      console.error(e);
    }

    this.jobs.delete(job);

    this.checkForNewJobs();
  }

  // DI for testing
  setTimeout: (cb: () => void, timeout: number) => NodeJS.Timeout =
    global.setTimeout;

  private delayAutoCheck() {
    this.checkAgainAfter(this.autoCheckEvery);
  }

  nextCheck: { handle: NodeJS.Timeout; time: number } | null = null;

  private checkAgainAfter(millis: number) {
    if (this.isClosed) {
      return;
    }

    const date = Date.now() + millis;
    const alreadyCheckingSometimeBefore =
      this.nextCheck && this.nextCheck.time < date;
    if (alreadyCheckingSometimeBefore) {
      return;
    }

    if (this.nextCheck) {
      clearTimeout(this.nextCheck.handle);
    }
    this.nextCheck = {
      time: date,
      handle: this.setTimeout(() => {
        this.nextCheck = null;
        this.checkForNewJobs();
      }, millis),
    };
  }

  public async checkForNewJobs() {
    this.logger?.trace("Checking for jobs");
    this.delayAutoCheck();

    while (!this.isPacked) {
      const result = await this.fetch();
      this.logger?.trace({ result }, "Checking for jobs finished");
      switch (result[0]) {
        case "empty": {
          return;
        }

        case "success": {
          const job = result[1];
          this.workOn(job);
          continue;
        }

        case "wait": {
          const waitFor = result[1];
          this.checkAgainAfter(waitFor);
          return;
        }

        case "retry": {
          continue;
        }
      }
    }
  }

  close() {
    this.isClosed = true;
    if (this.nextCheck) {
      clearTimeout(this.nextCheck.handle);
    }
  }
}
