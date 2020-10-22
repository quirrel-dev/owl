const child_process = require("child_process");
const path = require("path");
const rxjs = require("rxjs");

// "redis" or "inmemory"
const version = process.argv[2] || "redis";

/**
 * @param {{ id: string, queue: string, payload: string }[]} jobs
 * @param {{ [key: string]: { $: ($: rxjs.Observable<{ id: string, queue: string, payload: string, time: number }>) => rxjs.Observable<any>), expect: (v: any) => boolean } }} expectations
 */
async function test(jobs, expectations) {
  const $ = new rxjs.Subject();
  Object.entries(expectations).forEach(([description, expectation]) => {
    const value = expectation.$($);
    value.forEach((v) => {
      const isCorrect = expectation.expect(v);
      if (isCorrect) {
        console.log(`✅ ${description}`);
      } else {
        console.log(`❌ ${description}: ${v}`);
        process.exitCode = 1;
      }
    });
  });

  let closeWorker;
  let producer;

  if (version === "redis") {
    const owl = require("./shared");

    const redis = owl.redisFactory();
    await redis.flushall();

    const worker = child_process.fork(path.join(__dirname, "worker.js"));
    worker.on("message", (m) => {
      $.next(m);
    });
    closeWorker = () => {
      worker.kill("SIGINT");
      redis.quit();
    };

    producer = owl.createProducer()
  } else {
    const { MockOwl } = require("../../dist")
    const owl = new MockOwl()

    const worker = owl.createWorker(async (job) => {
      $.next({ ...job, time: Date.now() });
    });
    closeWorker = () => worker.close();

    producer = owl.createProducer()
  }

  await Promise.all(jobs.map((job) => producer.enqueue(job)));

  setTimeout(() => {
    producer.close();
    closeWorker();
    $.complete();
  }, 1000);
}

module.exports = test;
