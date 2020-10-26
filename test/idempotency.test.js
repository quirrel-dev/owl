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
    },
  ],
  {
    "only the first one arrives": {
      transform: [map((j) => j.payload)],
      expect: (v) => v.length === 1 && v[0] === "a",
    },
  }
);
