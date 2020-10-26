const test = require("./lib");
const { map } = require("rxjs/operators");

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
      transform: [map((j) => j.payload)],
      expect: (v) => v.length === 1 && v[0] === "b",
    },
  }
);
