import owl from "./shared";

const worker = owl.createWorker(
  async (job) => {
    console.log("Delay: ", Date.now() - +job.payload)
  }
);

process.on("SIGINT", () => worker.close());
