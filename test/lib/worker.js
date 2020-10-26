const owl = require("./shared");

const worker = owl.createWorker(async (job) => {
  const [enqueueTime, originalPayload] = job.payload.split(";");
  process.send({
    ...job,
    payload: originalPayload,
    time: Date.now(),
    delay: Date.now() - +enqueueTime,
  });
});

process.on("SIGINT", async () => {
  await worker.close();
});
