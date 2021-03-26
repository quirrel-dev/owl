import { expect } from "chai";
import { againstAllBackends } from "../util";
import { makeProducerEnv } from "./support";

type Signal = Promise<void> & { signal(): void };

export function makeSignal(): Signal {
  let _resolve: () => void;

  const promise = new Promise<void>((resolve) => {
    _resolve = resolve;
  }) as Signal;

  promise.signal = _resolve;

  return promise;
}

againstAllBackends("dontReschedule", (backend) => {
  it("works", async () => {
    const producerEnv = makeProducerEnv(backend);
    await producerEnv.setup();

    const acknowledged = makeSignal();
    const worker = producerEnv.owl.createWorker(async (job, meta) => {
      await worker.acknowledger.acknowledge(meta, {
        dontReschedule: true,
      });
      acknowledged.signal();
    });

    await producerEnv.producer.enqueue({
      id: "a",
      queue: "q",
      payload: "p",
      schedule: {
        type: "every",
        meta: "1000",
      },
    });

    await acknowledged;

    const job = await producerEnv.producer.findById("q", "a");
    expect(job).to.be.null;

    await producerEnv.teardown();
    await worker.close();
  });
});
