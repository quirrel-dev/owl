const owl = require("./shared");

owl
  .createWorker(async (job) => {
    const [enqueueTime, originalPayload] = job.payload.split(";");
    process.send({
      ...job,
      payload: originalPayload,
      time: Date.now(),
      delay: Date.now() - +enqueueTime,
    });
  })
  .then((worker) => {
    process.send("ready");

    process.on("SIGINT", async () => {
      await worker.close();
    });
  });
