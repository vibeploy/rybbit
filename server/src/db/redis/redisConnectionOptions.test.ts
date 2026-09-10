import { describe, expect, it } from "vitest";
import { getRedisConnectionOptions } from "./redisConnectionOptions.js";

describe("getRedisConnectionOptions", () => {
  it("keeps standalone Redis as the default", () => {
    expect(
      getRedisConnectionOptions({
        REDIS_HOST: "redis.internal",
        REDIS_PORT: "6380",
        REDIS_PASSWORD: "secret",
      })
    ).toEqual({ host: "redis.internal", port: 6380, password: "secret" });
  });

  it("configures a master connection through multiple Sentinels", () => {
    expect(
      getRedisConnectionOptions({
        REDIS_PASSWORD: "redis-secret",
        REDIS_SENTINEL_HOSTS: "sentinel-0.redis:26379, sentinel-1.redis:26380",
        REDIS_SENTINEL_MASTER_NAME: "analytics-primary",
      })
    ).toEqual({
      sentinels: [
        { host: "sentinel-0.redis", port: 26379 },
        { host: "sentinel-1.redis", port: 26380 },
      ],
      name: "analytics-primary",
      role: "master",
      password: "redis-secret",
    });
  });

  it("defaults the Sentinel port and master name", () => {
    expect(getRedisConnectionOptions({ REDIS_SENTINEL_HOSTS: "sentinel.redis" })).toEqual({
      sentinels: [{ host: "sentinel.redis", port: 26379 }],
      name: "mymaster",
      role: "master",
    });
  });

  it("supports bracketed IPv6 Sentinel addresses and separate Sentinel auth", () => {
    expect(
      getRedisConnectionOptions({
        REDIS_SENTINEL_HOSTS: "[2001:db8::10]:26380",
        REDIS_SENTINEL_USERNAME: "sentinel-user",
        REDIS_SENTINEL_PASSWORD: "sentinel-secret",
      })
    ).toEqual({
      sentinels: [{ host: "2001:db8::10", port: 26380 }],
      name: "mymaster",
      role: "master",
      sentinelUsername: "sentinel-user",
      sentinelPassword: "sentinel-secret",
    });
  });

  it.each(["sentinel:", ":26379", "sentinel:not-a-port", "sentinel:70000", "a,,b"])(
    "rejects an invalid Sentinel list: %s",
    REDIS_SENTINEL_HOSTS => {
      expect(() => getRedisConnectionOptions({ REDIS_SENTINEL_HOSTS })).toThrow();
    }
  );
});
