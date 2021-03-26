import { expect } from "chai";
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

function test(backend: "Redis" | "In-Memory") {
  it(backend + " > dontReschedule", async () => {
    const producerEnv = makeProducerEnv(backend === "In-Memory");
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
}

test("In-Memory");
test("Redis");
