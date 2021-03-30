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
    private readonly fetchInitialTenants: () => AsyncGenerator<string[]>,
    private readonly fetch: (
      tenant: string
    ) => Promise<
      ["empty"] | ["retry"] | ["success", T] | ["wait", Promise<void>]
    >,
    private readonly run: (job: T, tenant: string) => Promise<void>,
    public readonly maxJobs: number = 100,
    private readonly autoCheckEvery: number = 1000
  ) {}

  public async start() {
    const promises: Promise<void>[] = [];
    for await (const tenants of this.fetchInitialTenants()) {
      promises.push(...tenants.map((t) => this.checkForNewJobs(t)));
    }
    await Promise.all(promises);
  }

  private async workOn(job: T, tenant: string) {
    this.jobs.add(job);

    try {
      await this.run(job, tenant);
    } catch (e) {
      console.error(e);
    }

    this.jobs.delete(job);

    this.checkForNewJobs(tenant);
  }

  // DI for testing
  setTimeout: (cb: () => void, timeout: number) => NodeJS.Timeout =
    global.setTimeout;

  autoCheckIds: Record<string, NodeJS.Timeout> = {};

  private delayAutoCheck(tenant: string) {
    if (this.autoCheckIds[tenant]) {
      clearInterval(this.autoCheckIds[tenant]);
    }

    this.autoCheckIds[tenant] = this.setTimeout(
      () => this.checkForNewJobs(tenant),
      this.autoCheckEvery
    );
  }

  public async checkForNewJobs(tenant: string) {
    this.delayAutoCheck(tenant);

    while (!this.isPacked) {
      const result = await this.fetch(tenant);
      switch (result[0]) {
        case "empty": {
          return;
        }

        case "success": {
          const job = result[1];
          this.workOn(job, tenant);
          continue;
        }

        case "wait": {
          const waitFor = result[1];
          await waitFor;
          continue;
        }

        case "retry": {
          continue;
        }
      }
    }
  }

  close() {
    Object.values(this.autoCheckIds).forEach(clearInterval);
  }
}
