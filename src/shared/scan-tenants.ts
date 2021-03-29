import type { Redis } from "ioredis";

export async function* scanTenants(redis: Redis) {
  let cursor = 0;

  yield [""];

  do {
    const [newCursor, queueKeys] = await redis.scan(
      cursor,
      "MATCH",
      "{*}:queue"
    );

    yield queueKeys.map((k) => k.slice(1, k.indexOf("}")));

    cursor = +newCursor;
  } while (cursor !== 0);
}
