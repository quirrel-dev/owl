import { expect } from "chai";
import { delay, describeAcrossBackends, waitUntil } from "../util";
import { makeWorkerEnv } from "./support";

describeAcrossBackends("Cluster", (backend) => {
  const env = makeWorkerEnv(backend);

  beforeEach(env.setup);
  afterEach(env.teardown);

  it("works with two tenants", async () => {
    const eventsA: any[] = [];
    const activityA = env.owl.createActivity("a", (ev) => {
      eventsA.push(ev);
    });

    const eventsB: any[] = [];
    const activityB = env.owl.createActivity("b", (ev) => {
      eventsB.push(ev);
    });

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

    await activityA.close();
    await activityB.close();
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
