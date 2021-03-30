import { expect } from "chai";
import Redis from "ioredis";
import {
  migrate,
  processingToSortedSet,
} from "../../src/shared/migrator/migrator";

describe("Migrations", () => {
  let redis: Redis.Redis;

  beforeEach(async () => {
    await redis.flushall();
  });

  before(() => {
    redis = new Redis(process.env.REDIS_URL);
  });

  after(async () => {
    redis.disconnect();
  });

  it("migrate", async () => {
    expect(await redis.smembers("owl-migrations")).to.deep.eq([]);

    await migrate(redis);

    expect(await redis.smembers("owl-migrations")).to.deep.equal([
      "processingToSortedSet",
    ]);
  });

  it("processingToSortedSet", async () => {
    await redis.sadd("processing", "foo", "bar", "baz");

    await processingToSortedSet.run(redis);

    const afterWards = await redis.zrange(
      "processing",
      0,
      Number.MAX_SAFE_INTEGER,
      "WITHSCORES"
    );
    expect(afterWards).to.have.length(6);
  });
});
