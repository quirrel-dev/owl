const child_process = require("child_process");
const path = require("path");
const rxjs = require("rxjs");

// "redis" or "inmemory"
const version = process.argv[2] || "redis";

/**
 * @param {{ id: string, queue: string, payload: string }[]} jobs
 * @param {{ [key: string]: { transform: any[], expect: (v: any) => boolean } }} expectations
 */
async function test(jobs, expectations) {
  const $ = new rxjs.Subject();

  let remainingTests = new Set(Object.keys(expectations));
  Object.entries(expectations).forEach(([testName, expectation]) => {
    const value = $.pipe(...expectation.transform);
    value.forEach((v) => {
      const isCorrect = expectation.expect(v);
      remainingTests.delete(testName);
      if (isCorrect) {
        console.log(`✅ ${testName}`);
      } else {
        console.log(`❌ ${testName}: ${v}`);
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

    producer = owl.createProducer();
  } else {
    const { MockOwl } = require("../../dist");
    const owl = new MockOwl();

    const worker = owl.createWorker(async (job) => {
      const [enqueueTime, originalPayload] = job.payload.split(";");
      $.next({
        ...job,
        payload: originalPayload,
        time: Date.now(),
        delay: Date.now() - +enqueueTime,
      });
    });
    closeWorker = () => worker.close();

    producer = owl.createProducer();
  }

  console.time("⏱  time spent on enqueueing");
  await Promise.all(
    jobs.map((job) =>
      producer.enqueue({ ...job, payload: Date.now() + ";" + job.payload })
    )
  );
  console.timeEnd("⏱  time spent on enqueueing");

  setTimeout(() => {
    producer.close();
    closeWorker();
    $.complete();

    remainingTests.forEach((testName) => {
      console.log(`👀 result is missing: "${testName}"`);
    });
  }, 1000);
}

module.exports = test;
