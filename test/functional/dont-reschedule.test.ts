import { expect } from "chai";
import { makeProducerEnv, delay } from "./support";

function test(backend: "Redis" | "In-Memory") {
  it(backend + " > dontReschedule", async () => {
    const producerEnv = makeProducerEnv(backend === "In-Memory");
    await producerEnv.setup();

    const worker = producerEnv.owl.createWorker(async (job, meta) => {
      meta.dontReschedule();
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

    await delay(50);

    const job = await producerEnv.producer.findById("q", "a");
    expect(job).to.be.null;

    await producerEnv.teardown();
    await worker.close();
  });
}

test("In-Memory");
test("Redis");
