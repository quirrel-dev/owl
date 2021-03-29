export function encodeRedisKey(decoded: string): string {
  return decoded.replace(/:/g, "%3A");
}

export function decodeRedisKey(encoded: string): string {
  return encoded.replace(/%3A/g, ":");
}

export function tenantToRedisPrefix(tenant: string) {
  if (tenant.includes("{") || tenant.includes("}")) {
    throw new Error("tenant shall not include {}!");
  }
  
  if (tenant === "") {
    return "";
  }

  return "{" + tenant + "}";
}
