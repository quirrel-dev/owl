import owl from "./shared";

const producer = owl.createProducer();

for (let i = 0; i < 100; i++) {
  console.log("enq", Date.now());
  producer.enqueue({
    id: "hallo-" + i,
    queue: "hallo",
    payload: "hallo",
  });
}

producer.close();
