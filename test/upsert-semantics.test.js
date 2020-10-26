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
      upsert: true,
    },
  ],
  {
    "only the second one arrives": {
      transform: [map((j) => j.payload), toArray()],
      expect: (v) => v.length === 1 && v[0] === "b",
    },
  }
);
