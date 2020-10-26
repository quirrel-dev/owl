const test = require("./lib");
const {
  map,
  reduce,
  count,
  every,
  min,
  max,
  tap,
  distinct,
} = require("rxjs/operators");

const average = reduce((acc, val, i) => {
  if (i === 0) {
    return val;
  }
  if (i === 1) {
    return (acc + val) / 2;
  }

  const oldSum = acc * (i - 1);
  const newSum = oldSum + val;
  return newSum / i;
}, 0);

function repeat(valMaker, number) {
  return Array(number)
    .fill(0)
    .map((_, i) => valMaker(i));
}

const mapExecutionDelay = map((v) => v.delay);

const log = tap(console.log);

test(
  repeat(
    (i) => ({
      id: "x-" + i,
      payload: "",
      queue: "any",
    }),
    1000
  ),
  {
    "correct shape": {
      transform: [
        every(
          (v) =>
            v.id.startsWith("x-") &&
            typeof v.payload === "string" &&
            isFinite(+v.payload) &&
            v.queue === "any"
        ),
      ],
      expect: (v) => v === true,
    },
    "all jobs arrive": {
      transform: [count()],
      expect: (v) => v == 1000,
    },
    "no jobs arrive double": {
      transform: [distinct((j) => j.id), count()],
      expect: (v) => v == 1000,
    },
    "average delay is bearable": {
      transform: [mapExecutionDelay, average],
      expect: (v) => v < 80,
    },
    "minimum delay is bearable": {
      transform: [mapExecutionDelay, min()],
      expect: (v) => v < 50,
    },
    "maximum delay is bearable": {
      transform: [mapExecutionDelay, max()],
      expect: (v) => v < 100,
    },
  }
);
