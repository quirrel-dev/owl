import { pseudoRandomBytes } from "crypto";
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
    private readonly fetch: () => Promise<T | null>,
    private readonly run: (job: T) => Promise<void>,
    private readonly logger?: Logger,
    public readonly maxJobs: number = 100
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
      this.logger?.error(e as any);
      console.error(e);
    }

    this.jobs.delete(job);

    this.checkForNewJobs();
  }

  public async checkForNewJobs() {
    if (this.isClosed) {
      return;
    }

    this.logger?.trace("Checking for jobs");
    while (!this.isPacked && !this.isClosed) {
      const job = await this.fetch();
      this.logger?.trace({ job }, "Checking for jobs finished");
      if (!job) {
        return;
      }

      this.workOn(job);
    }
  }

  close() {
    this.isClosed = true;
  }
}
