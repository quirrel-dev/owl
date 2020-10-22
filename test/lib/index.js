const child_process = require("child_process");
const owl = require("./shared");
const path = require("path");
const rxjs = require("rxjs");

/**
 * @param {{ id: string, queue: string, payload: string }[]} jobs
 * @param {{ [key: string]: { $: ($: rxjs.Observable<{ id: string, queue: string, payload: string, time: number }>) => rxjs.Observable<any>), expect: (v: any) => boolean } }} expectations
 */
async function test(jobs, expectations) {
  const redis = owl.redisFactory();
  await redis.flushall();

  const $ = new rxjs.Subject();
  Object.entries(expectations).forEach(([ description, expectation ]) => {
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

  const worker = child_process.fork(path.join(__dirname, "worker.js"));
  worker.on("message", (m) => {
    $.next(m);
  });

  const producer = owl.createProducer();

  await Promise.all(jobs.map((job) => producer.enqueue(job)));

  setTimeout(async () => {
    await producer.close();
    await redis.quit();
    worker.kill("SIGINT");
    $.complete();
  }, 1000);
}

module.exports = test;
