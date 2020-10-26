const test = require("./lib");
const { map, reduce, count, every } = require("rxjs/operators");

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

test(
  repeat(
    (i) => ({
      id: "x-" + i,
      payload: "" + Date.now(),
      queue: "any",
    }),
    100
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
      expect: (v) => v == 100,
    },
    "average delay is berable": {
      transform: [map((v) => v.time - +v.payload), average],
      expect: (v) => v < 500,
    },
  }
);
