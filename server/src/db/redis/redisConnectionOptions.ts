import type { RedisOptions } from "ioredis";

const DEFAULT_REDIS_PORT = 6379;
const DEFAULT_SENTINEL_PORT = 26379;
const DEFAULT_SENTINEL_MASTER_NAME = "mymaster";

function parsePort(value: string | undefined, fallback: number, label: string): number {
  if (value === undefined || value === "") return fallback;
  if (!/^\d+$/.test(value)) throw new Error(`${label} must be a valid port`);
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`${label} must be between 1 and 65535`);
  }
  return port;
}

function parseSentinelAddress(value: string): { host: string; port: number } {
  const address = value.trim();
  if (!address) throw new Error("REDIS_SENTINEL_HOSTS contains an empty address");

  if (address.startsWith("[")) {
    const match = /^\[([^\]]+)\](?::([^:]+))?$/.exec(address);
    if (!match?.[1]) throw new Error(`Invalid Redis Sentinel address: ${address}`);
    return {
      host: match[1],
      port: parsePort(match[2], DEFAULT_SENTINEL_PORT, `Redis Sentinel port for ${address}`),
    };
  }

  const firstColon = address.indexOf(":");
  const lastColon = address.lastIndexOf(":");
  if (firstColon !== -1 && firstColon === lastColon) {
    const host = address.slice(0, firstColon);
    const port = address.slice(firstColon + 1);
    if (!host || !port) throw new Error(`Invalid Redis Sentinel address: ${address}`);
    return {
      host,
      port: parsePort(port, DEFAULT_SENTINEL_PORT, `Redis Sentinel port for ${address}`),
    };
  }

  return { host: address, port: DEFAULT_SENTINEL_PORT };
}

export function getRedisConnectionOptions(env: NodeJS.ProcessEnv = process.env): RedisOptions {
  const password = env.REDIS_PASSWORD || undefined;
  const sentinelHosts = env.REDIS_SENTINEL_HOSTS?.trim();

  if (!sentinelHosts) {
    return {
      host: env.REDIS_HOST || "localhost",
      port: parsePort(env.REDIS_PORT, DEFAULT_REDIS_PORT, "REDIS_PORT"),
      ...(password ? { password } : {}),
    };
  }

  const sentinels = sentinelHosts.split(",").map(parseSentinelAddress);
  const masterName = env.REDIS_SENTINEL_MASTER_NAME?.trim() || DEFAULT_SENTINEL_MASTER_NAME;

  return {
    sentinels,
    name: masterName,
    role: "master",
    ...(password ? { password } : {}),
    ...(env.REDIS_SENTINEL_USERNAME ? { sentinelUsername: env.REDIS_SENTINEL_USERNAME } : {}),
    ...(env.REDIS_SENTINEL_PASSWORD ? { sentinelPassword: env.REDIS_SENTINEL_PASSWORD } : {}),
  };
}
