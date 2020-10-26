import owl from "./shared";

async function main() {
  const producer = owl.createProducer();

  for (let i = 0; i < 100; i++) {
    console.log("enq", Date.now());
    await producer.enqueue({
      id: "hallo-" + i,
      queue: "hallo",
      payload: "" + Date.now(),
    });
  }

  producer.close();
}

main();
