import owl from "./shared";

owl
  .createWorker(async (job) => {
    console.log("Delay: ", Date.now() - +job.payload);
  })
  .then((worker) => {
    process.on("SIGINT", () => worker.close());
  });
