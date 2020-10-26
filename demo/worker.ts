import owl from "./shared";

const worker = owl.createWorker(
  (job) =>
    new Promise((resolve) => {
      console.log("Delay: ", Date.now() - +job.payload)
      // console.log("start", job, Date.now());
      setTimeout(() => {
        // console.log("end", job, Date.now());
        resolve();
      }, 1000);
    })
);

process.on("SIGINT", () => worker.close());
