const owl = require("./shared");

const worker = owl.createWorker(async (job) => {
  process.send({
    ...job,
    time: Date.now(),
  });
});

process.on("SIGINT", async () => {
  await worker.close();
});
