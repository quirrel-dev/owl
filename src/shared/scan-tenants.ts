import type { Redis } from "ioredis";

async function* scanForTenants(redis: Redis, keyAfterTenant: string) {
  let cursor = 0;
  do {
    const [newCursor, queueKeys] = await redis.scan(
      cursor,
      "MATCH",
      "{*}" + keyAfterTenant
    );

    yield queueKeys.map((k) => k.slice(1, k.indexOf("}")));

    cursor = +newCursor;
  } while (cursor !== 0);
}

export async function* scanTenants(redis: Redis) {
  if (await redis.exists("queue", "processing")) {
    yield [""];
  }

  yield* scanForTenants(redis, "queue");
  yield* scanForTenants(redis, "processing");
}

export async function* scanTenantsForProcessing(redis: Redis) {
  if (await redis.exists("processing")) {
    yield [""];
  }

  yield* scanForTenants(redis, "processing");
}
