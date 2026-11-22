import { randomUUID } from "node:crypto";

/** The subset of the ioredis client a lease needs; injectable for tests. */
export interface LeaseRedisClient {
  set(key: string, value: string, px: "PX", ttlMs: number, nx: "NX"): Promise<string | null>;
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
}

// Only the holder's token may extend or release the key, so a run that
// outlived its lease can never extend or delete a lease another replica now holds.
const RENEW_LUA = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`;
const RELEASE_LUA = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`;

export interface RedisLease {
  /** False once a renewal failed or found the key taken; callers must stop at the next safe point. */
  held(): boolean;
  release(): Promise<void>;
}

export interface LeaseOptions {
  ttlMs: number;
  renewEveryMs: number;
  onLost?: (reason: "taken" | "error", error?: unknown) => void;
}

/**
 * Acquires `key` with a random token and keeps it alive in the background.
 * Returns null when another holder has it. Redis errors on acquire propagate
 * so the caller can fail closed.
 */
export async function acquireRedisLease(
  client: LeaseRedisClient,
  key: string,
  options: LeaseOptions
): Promise<RedisLease | null> {
  if (options.renewEveryMs >= options.ttlMs) {
    throw new Error("Lease renewal interval must be shorter than its TTL");
  }
  const token = randomUUID();
  if ((await client.set(key, token, "PX", options.ttlMs, "NX")) !== "OK") return null;

  let held = true;
  let renewing = false;
  const lose = (reason: "taken" | "error", error?: unknown) => {
    if (!held) return;
    held = false;
    clearInterval(timer);
    options.onLost?.(reason, error);
  };
  const timer = setInterval(() => {
    if (renewing || !held) return;
    renewing = true;
    client
      .eval(RENEW_LUA, 1, key, token, options.ttlMs)
      .then(result => {
        if (Number(result) !== 1) lose("taken");
      })
      // A failed renewal means the lease may lapse before the next attempt;
      // treat it as lost rather than keep working on an unconfirmed lease.
      .catch(error => lose("error", error))
      .finally(() => {
        renewing = false;
      });
  }, options.renewEveryMs);
  timer.unref?.();

  return {
    held: () => held,
    async release() {
      const wasHeld = held;
      held = false;
      clearInterval(timer);
      if (!wasHeld) return;
      try {
        await client.eval(RELEASE_LUA, 1, key, token);
      } catch {
        // The TTL bounds how long a stale key can block the next run.
      }
    },
  };
}
