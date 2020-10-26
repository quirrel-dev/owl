const test = require("./lib");
const { map, toArray } = require("rxjs/operators");

test(
  [
    {
      id: "x",
      payload: "a",
      queue: "q",
    },
    {
      id: "x",
      payload: "b",
      queue: "q",
    },
  ],
  {
    "only the first one arrives": {
      transform: [map((j) => j.payload), toArray()],
      expect: (v) => v.length === 1 && v[0] === "a",
    },
    "delay is bearable": {
      transform: [map((j) => j.delay)],
      expect: (delay) => delay < 100,
    },
  }
);
