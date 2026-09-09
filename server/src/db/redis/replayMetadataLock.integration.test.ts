import RedisMock from "ioredis-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { RELEASE_REPLAY_METADATA_LOCK_LUA } from "./replayMetadataLockLua.js";

interface ReplayMetadataLockRedis {
  del(key: string): Promise<number>;
  exists(key: string): Promise<number>;
  get(key: string): Promise<string | null>;
  releaseReplayMetadataLock(key: string, token: string): Promise<number>;
  set(key: string, value: string, ...args: Array<string | number>): Promise<string | null>;
}

let redis: ReplayMetadataLockRedis;

describe("replay metadata lock Lua script", () => {
  beforeEach(async () => {
    const client = new RedisMock();
    client.defineCommand("releaseReplayMetadataLock", {
      numberOfKeys: 1,
      lua: RELEASE_REPLAY_METADATA_LOCK_LUA,
    });
    redis = client as unknown as ReplayMetadataLockRedis;
    await redis.del("replay-lock");
  });

  it("allows only one worker to acquire a session lock", async () => {
    expect(await redis.set("replay-lock", "worker-a", "PX", 120_000, "NX")).toBe("OK");
    expect(await redis.set("replay-lock", "worker-b", "PX", 120_000, "NX")).toBeNull();
    expect(await redis.get("replay-lock")).toBe("worker-a");
  });

  it("releases a lock only when the owner token matches", async () => {
    await redis.set("replay-lock", "worker-a", "PX", 120_000, "NX");

    expect(await redis.releaseReplayMetadataLock("replay-lock", "worker-b")).toBe(0);
    expect(await redis.exists("replay-lock")).toBe(1);
    expect(await redis.releaseReplayMetadataLock("replay-lock", "worker-a")).toBe(1);
    expect(await redis.exists("replay-lock")).toBe(0);
  });
});
