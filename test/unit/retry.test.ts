import { expect } from "chai";
import { computeTimestampForNextRetry } from "../../src/worker/retry";

it("computeTimestampForNextRetry", () => {
  const schedule = [10, 100, 1000];
  const firstRun = 10000;
  const secondRun = computeTimestampForNextRetry(
    new Date(firstRun),
    schedule,
    1
  );
  const thirdRun = computeTimestampForNextRetry(
    new Date(secondRun!),
    schedule,
    2
  );
  const fourthRun = computeTimestampForNextRetry(
    new Date(thirdRun!),
    schedule,
    3
  );
  const fifthRun = computeTimestampForNextRetry(
    new Date(fourthRun!),
    schedule,
    4
  );

  expect(secondRun).to.equal(10010);
  expect(thirdRun).to.equal(10100);
  expect(fourthRun).to.equal(11000);
  expect(fifthRun).to.be.undefined;
});
