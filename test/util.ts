import { AssertionError } from "chai";

export type Backend = "In-Memory" | "Redis";

export function describeAcrossBackends(
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

export function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type Signal = Promise<void> & { signal(): void };

export function makeSignal(): Signal {
  let _resolve: () => void;

  const promise = new Promise<void>((resolve) => {
    _resolve = resolve;
  }) as Signal;

  promise.signal = _resolve;

  return promise;
}

function removeFirstStackLine(string: string): string {
  return string.replace(/\n.*\n/, "");
}

export function waitUntil(
  predicate: () => boolean,
  butMax: number,
  interval = 20
) {
  const potentialError = new AssertionError(
    `Predicate was not fulfilled on time (${predicate.toString()})`,
    {
      showDiff: false,
    }
  );
  potentialError.stack = removeFirstStackLine(potentialError.stack);

  return new Promise<void>((resolve, reject) => {
    const check = setInterval(() => {
      if (predicate()) {
        clearInterval(check);
        clearTimeout(max);
        resolve();
      }
    }, interval);

    const max = setTimeout(() => {
      clearInterval(check);
      reject(potentialError);
    }, butMax);
  });
}
