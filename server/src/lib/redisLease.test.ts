import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { acquireRedisLease, type LeaseRedisClient } from "./redisLease.js";

function fakeRedis() {
  const store = new Map<string, string>();
  const client = {
    set: vi.fn(async (key: string, value: string) => {
      if (store.has(key)) return null;
      store.set(key, value);
      return "OK";
    }),
    eval: vi.fn(async (script: string, _numKeys: number, key: string, token: string) => {
      if (store.get(key) !== token) return 0;
      if (script.includes("del")) store.delete(key);
      return 1;
    }),
  };
  return { client: client as unknown as LeaseRedisClient & typeof client, store };
}

describe("acquireRedisLease", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("lets only one holder in and renews with its own token", async () => {
    const { client, store } = fakeRedis();
    const lease = await acquireRedisLease(client, "k", { ttlMs: 60_000, renewEveryMs: 20_000 });
    expect(lease).not.toBeNull();
    expect(await acquireRedisLease(client, "k", { ttlMs: 60_000, renewEveryMs: 20_000 })).toBeNull();

    await vi.advanceTimersByTimeAsync(20_000);
    expect(client.eval).toHaveBeenCalledWith(expect.stringContaining("pexpire"), 1, "k", store.get("k"), 60_000);
    expect(lease!.held()).toBe(true);
  });

  it("marks the lease lost when the key now belongs to someone else", async () => {
    const { client, store } = fakeRedis();
    const onLost = vi.fn();
    const lease = await acquireRedisLease(client, "k", { ttlMs: 60_000, renewEveryMs: 20_000, onLost });
    store.set("k", "other-holder");

    await vi.advanceTimersByTimeAsync(20_000);

    expect(lease!.held()).toBe(false);
    expect(onLost).toHaveBeenCalledWith("taken", undefined);
  });

  it("marks the lease lost when renewal errors", async () => {
    const { client } = fakeRedis();
    const lease = await acquireRedisLease(client, "k", { ttlMs: 60_000, renewEveryMs: 20_000 });
    client.eval.mockRejectedValueOnce(new Error("redis down"));

    await vi.advanceTimersByTimeAsync(20_000);

    expect(lease!.held()).toBe(false);
  });

  it("releases only its own key and stops renewing", async () => {
    const { client, store } = fakeRedis();
    const lease = await acquireRedisLease(client, "k", { ttlMs: 60_000, renewEveryMs: 20_000 });
    await lease!.release();
    expect(store.has("k")).toBe(false);

    store.set("k", "next-holder");
    await lease!.release();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(store.get("k")).toBe("next-holder");
    expect(client.eval).toHaveBeenCalledTimes(1);
  });

  it("does not delete a key it lost to another holder", async () => {
    const { client, store } = fakeRedis();
    const lease = await acquireRedisLease(client, "k", { ttlMs: 60_000, renewEveryMs: 20_000 });
    store.set("k", "other-holder");
    await vi.advanceTimersByTimeAsync(20_000);

    await lease!.release();

    expect(store.get("k")).toBe("other-holder");
  });

  it("propagates acquire errors so callers fail closed", async () => {
    const { client } = fakeRedis();
    client.set.mockRejectedValueOnce(new Error("redis down"));
    await expect(acquireRedisLease(client, "k", { ttlMs: 60_000, renewEveryMs: 20_000 })).rejects.toThrow("redis down");
  });
});
