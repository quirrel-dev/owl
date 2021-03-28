import { Closable } from "../Closable";

export class JobDistributor<T> implements Closable {
  private readonly jobs = new Set<T>();

  get load() {
    return this.jobs.size;
  }

  get isPacked() {
    return this.jobs.size >= this.maxJobs;
  }

  constructor(
    private readonly fetch: () => Promise<
      ["empty"] | ["success", T] | ["wait", Promise<void>]
    >,
    private readonly run: (job: T) => Promise<void>,
    public readonly maxJobs: number = 100,
    private readonly autoCheckEvery: number = 1000
  ) {}

  private async workOn(job: T) {
    this.jobs.add(job);
    
    try {
      await this.run(job);
    } catch (e) {
      console.error(e);
    }

    this.jobs.delete(job);

    this.checkForNewJobs();
  }

  autoCheckId?: NodeJS.Timeout;

  // DI for testing
  setTimeout: (cb: () => void, timeout: number) => NodeJS.Timeout =
    global.setTimeout;

  private delayAutoCheck() {
    if (this.autoCheckId) {
      clearInterval(this.autoCheckId);
    }

    this.autoCheckId = this.setTimeout(
      () => this.checkForNewJobs(),
      this.autoCheckEvery
    );
  }

  public async checkForNewJobs() {
    this.delayAutoCheck();

    while (!this.isPacked) {
      const result = await this.fetch();
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
          await waitFor;
          continue;
        }
      }
    }
  }

  async close() {
    if (this.autoCheckId) {
      clearInterval(this.autoCheckId);
    }
  }
}
