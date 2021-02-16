import { expect } from "chai";
import { encodeRedisKey, decodeRedisKey } from "../../src/encodeRedisKey";

const cases = [
  {
    value: "hello",
    encoded: "hello",
  },
  {
    value: "a:b",
    encoded: "a%3Ab",
  },
  {
    value: "a:b:c",
    encoded: "a%3Ab%3Ac",
  },
  {
    value: "a:b%c",
    encoded: "a%3Ab%25c",
  },
];

describe("encodeRedisKey", () => {
  cases.forEach(({ value, encoded }) => {
    it(`"${value}" ↔ "${encoded}"`, () => {
      expect(encodeRedisKey(value)).to.eq(encoded);
      expect(decodeRedisKey(encodeRedisKey(value))).to.eq(value);
    });
  });
});
