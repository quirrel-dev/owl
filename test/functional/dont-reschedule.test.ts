import { expect } from "chai";
import { Worker } from "../../src/worker/worker";
import { delay, describeAcrossBackends, makeSignal } from "../util";
import { makeProducerEnv } from "./support";

describeAcrossBackends("dontReschedule", (backend) => {
  const producerEnv = makeProducerEnv(backend);
  before(producerEnv.setup);
  after(async () => {
    await producerEnv.teardown();
    await worker.close();
  });
  let worker: Worker;

  it("works", async () => {
    const acknowledged = makeSignal();
    worker = await producerEnv.owl.createWorker(async (job, meta) => {
      await worker.acknowledger.acknowledge(meta, {
        dontReschedule: true,
      });
      acknowledged.signal();
    });

    await producerEnv.producer.enqueue({
      tenant: "",
      id: "a",
      queue: "q",
      payload: "p",
      schedule: {
        type: "every",
        meta: "1000",
      },
    });

    await acknowledged;

    const job = await producerEnv.producer.findById("", "q", "a");
    expect(job).to.be.null;
  });
});
