export type Backend = "In-Memory" | "Redis";

export function againstAllBackends(
  topic: string,
  runTests: (backend: Backend) => void
) {
  describe(topic, () => {
    describe("Redis", () => {
      runTests("Redis");
    });

    describe("In-Memory", () => {
      runTests("In-Memory");
    });
  });
}
