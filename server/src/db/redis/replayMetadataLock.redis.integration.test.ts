import { Redis } from "ioredis";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RELEASE_REPLAY_METADATA_LOCK_LUA } from "./replayMetadataLockLua.js";

const redisUrl = process.env.TEST_REDIS_URL;
const describeWithRedis = redisUrl ? describe : describe.skip;

interface LockRedis extends Redis {
  releaseReplayMetadataLock(key: string, token: string): Promise<number>;
}

describeWithRedis("replay metadata lock against Redis", () => {
  let workerA: LockRedis;
  let workerB: LockRedis;
  const lockKey = "test:replay-metadata-lock:site-42:session-1";

  beforeAll(() => {
    workerA = new Redis(redisUrl!) as LockRedis;
    workerB = new Redis(redisUrl!) as LockRedis;

    for (const worker of [workerA, workerB]) {
      worker.defineCommand("releaseReplayMetadataLock", {
        numberOfKeys: 1,
        lua: RELEASE_REPLAY_METADATA_LOCK_LUA,
      });
    }
  });

  beforeEach(async () => {
    await workerA.del(lockKey);
  });

  afterAll(async () => {
    if (!workerA || !workerB) return;
    await workerA.del(lockKey);
    await Promise.all([workerA.quit(), workerB.quit()]);
  });

  it("serializes acquisition across independent connections", async () => {
    expect(await workerA.set(lockKey, "worker-a", "PX", 120_000, "NX")).toBe("OK");
    expect(await workerB.set(lockKey, "worker-b", "PX", 120_000, "NX")).toBeNull();

    expect(await workerA.releaseReplayMetadataLock(lockKey, "worker-a")).toBe(1);
    expect(await workerB.set(lockKey, "worker-b", "PX", 120_000, "NX")).toBe("OK");
  });

  it("does not let an expired owner delete a replacement lock", async () => {
    expect(await workerA.set(lockKey, "stale-owner", "PX", 20, "NX")).toBe("OK");
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(await workerB.set(lockKey, "replacement-owner", "PX", 120_000, "NX")).toBe("OK");

    expect(await workerA.releaseReplayMetadataLock(lockKey, "stale-owner")).toBe(0);
    expect(await workerB.get(lockKey)).toBe("replacement-owner");
  });
});
