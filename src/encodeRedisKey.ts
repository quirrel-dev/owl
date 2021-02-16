export function encodeRedisKey(decoded: string): string {
  return decoded.replace(/:/g, "%3A");
}

export function decodeRedisKey(encoded: string): string {
  return encoded.replace(/%3A/g, ":");
}
