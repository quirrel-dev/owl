import { expect } from "chai";
import { Closable } from "../../src";
import { Worker } from "../../src/worker/worker";
import { delay, describeAcrossBackends, makeSignal, waitUntil } from "../util";
import { makeProducerEnv, makeWorkerEnv } from "./support";

describeAcrossBackends("Cluster", (backend) => {
  describe("when workers already exist", () => {
    const env = makeWorkerEnv(backend);

    beforeEach(env.setup);
    afterEach(async () => {
      await env.teardown();
      await Promise.all(closables.map((c) => c.close()));
      closables = [];
    });

    let closables: Closable[] = [];

    it("works with two tenants", async () => {
      const eventsA: any[] = [];
      const activityA = env.owl.createActivity("a", (ev) => {
        eventsA.push(ev);
      });
      closables.push(activityA);

      const eventsB: any[] = [];
      const activityB = env.owl.createActivity("b", (ev) => {
        eventsB.push(ev);
      });
      closables.push(activityB);

      await delay(10);

      await env.producer.enqueue({
        tenant: "a",
        queue: "a1",
        id: "a1.1",
        payload: "p:a1.1",
      });

      await env.producer.enqueue({
        tenant: "b",
        queue: "b1",
        id: "b1.1",
        payload: "p:b1.1",
      });

      await waitUntil(() => env.jobs.length === 2, 200);

      expect(eventsA).to.have.length(3);
      expect(eventsB).to.have.length(3);
      expect(eventsA[1]).to.eql({
        type: "requested",
        tenant: "a",
        queue: "a1",
        id: "a1.1",
      });
    });

    it("supports invocation", async () => {
      await env.producer.enqueue({
        tenant: "a",
        queue: "a1",
        id: "a1.1",
        payload: "p:a1.1",
        runAt: new Date(Date.now() + 10000),
      });

      const job = await env.producer.findById("a", "a1", "a1.1");
      expect(job).not.to.be.null;

      expect(env.jobs).to.have.length(0);

      await env.producer.invoke("a", "a1", "a1.1");
      await delay(10);
      expect(env.jobs).to.have.length(1);
    });

    it("supports deletion", async () => {
      await env.producer.enqueue({
        tenant: "a",
        queue: "a1",
        id: "a1.1",
        payload: "p:a1.1",
        runAt: new Date(Date.now() + 20),
      });

      const status = await env.producer.delete("a", "a1", "a1.1");
      expect(status).to.eq("deleted");

      const status2 = await env.producer.delete("a", "a1", "a1.1");
      expect(status2).to.eq("not_found");

      await delay(100);
      expect(env.jobs).to.have.length(0);
    });
  });

  describe("when workers spawn later", () => {
    const env = makeProducerEnv(backend);
    before(env.setup);
    after(async () => {
      env.teardown();
      await worker.close();
    });

    let worker: Worker;
    it("still executes all jobs", async () => {
      await env.producer.enqueue({
        tenant: "a",
        queue: "a1",
        id: "a1.1",
        payload: "p:a1.1",
        runAt: new Date(Date.now() + 20),
      });

      await delay(10);

      const executed = makeSignal();
      let job;
      worker = await env.owl.createWorker(async (_job, ack) => {
        await worker.acknowledger.acknowledge(ack);
        job = _job;
        executed.signal();
      });

      await executed;
      expect(job.id).to.eq("a1.1");
    });
  });
});
