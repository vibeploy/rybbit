import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  query: vi.fn(),
  command: vi.fn(),
  deleteObjects: vi.fn(),
}));

vi.mock("../../db/clickhouse/clickhouse.js", () => ({
  clickhouse: { query: mocks.query, command: mocks.command },
}));
vi.mock("../../db/redis/redis.js", () => ({ redis: { set: vi.fn() } }));
vi.mock("../storage/r2StorageService.js", () => ({
  r2Storage: { isEnabled: () => true, deleteObjects: mocks.deleteObjects },
}));
vi.mock("../../api/analytics/utils/utils.js", () => ({
  processResults: async (result: { rows: unknown[] }) => result.rows,
}));

import { enforceSiteQuota, planSiteEviction, runSiteQuotaOnce } from "./sessionReplaySiteQuotaService.js";

const GiB = 1024 ** 3;

describe("planSiteEviction", () => {
  it("keeps the newest sessions that fit and evicts the rest oldest first", () => {
    const plan = planSiteEviction(
      [
        { sessionId: "old", bytes: 0.4 * GiB, lastAt: "2026-11-22 01:00:00.000" },
        { sessionId: "new", bytes: 0.5 * GiB, lastAt: "2026-11-22 03:00:00.000" },
        { sessionId: "mid", bytes: 0.4 * GiB, lastAt: "2026-11-22 02:00:00.000" },
      ],
      GiB
    );
    expect(plan.map(session => session.sessionId)).toEqual(["old"]);
  });

  it("does not back-fill a gap with an older small session once a newer one does not fit", () => {
    const plan = planSiteEviction(
      [
        { sessionId: "tiny-old", bytes: 1, lastAt: "2026-11-22 01:00:00.000" },
        { sessionId: "big", bytes: 0.9 * GiB, lastAt: "2026-11-22 02:00:00.000" },
        { sessionId: "newest", bytes: 0.2 * GiB, lastAt: "2026-11-22 03:00:00.000" },
      ],
      GiB
    );
    expect(plan.map(session => session.sessionId)).toEqual(["tiny-old", "big"]);
  });

  it("evicts a single session larger than the whole quota", () => {
    expect(planSiteEviction([{ sessionId: "huge", bytes: 2 * GiB, lastAt: "2026-11-22 01:00:00.000" }], GiB)).toHaveLength(1);
  });

  it("evicts nothing when the site fits", () => {
    expect(planSiteEviction([{ sessionId: "a", bytes: GiB, lastAt: "2026-11-22 01:00:00.000" }], GiB)).toEqual([]);
  });
});

function routeQueries(rows: { sessions: unknown[]; keys: unknown[]; sites?: unknown[] }) {
  mocks.query.mockImplementation(async ({ query }: { query: string }) => {
    if (query.includes("HAVING used_bytes")) return { rows: rows.sites ?? [] };
    if (query.includes("UNION DISTINCT")) return { rows: rows.keys };
    return { rows: rows.sessions };
  });
}

describe("enforceSiteQuota", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.command.mockResolvedValue(undefined);
  });

  it("deletes evicted objects and then their rows, objects table last", async () => {
    routeQueries({
      sessions: [
        { session_id: "s-old", bytes: 0.8 * GiB, last_at: "2026-11-22 01:00:00.000" },
        { session_id: "s-new", bytes: 0.8 * GiB, last_at: "2026-11-22 02:00:00.000" },
      ],
      keys: [
        { session_id: "s-old", key: "7/s-old/1.json.zst" },
        { session_id: "s-old", key: "7/s-old/2.json.zst" },
      ],
    });
    mocks.deleteObjects.mockResolvedValue({ deleted: 2, failedKeys: [] });

    const result = await enforceSiteQuota(7, 1.6 * GiB, GiB);

    expect(mocks.deleteObjects).toHaveBeenCalledWith(["7/s-old/1.json.zst", "7/s-old/2.json.zst"]);
    const tables = mocks.command.mock.calls.map(([args]) => args.query.match(/DELETE FROM (\w+)/)?.[1]);
    expect(tables).toEqual(["session_replay_metadata_v2", "session_replay_events", "session_replay_objects"]);
    for (const [args] of mocks.command.mock.calls) {
      expect(args.query_params).toEqual({ siteId: 7, sessionIds: ["s-old"] });
    }
    expect(result).toMatchObject({ evictedSessions: 1, deletedObjects: 2, failedSessions: 0 });
  });

  it("never deletes a key outside the session's own prefix", async () => {
    routeQueries({
      sessions: [{ session_id: "s", bytes: 2 * GiB, last_at: "2026-11-22 01:00:00.000" }],
      keys: [
        { session_id: "s", key: "7/s/1.json.zst" },
        { session_id: "s", key: "8/other/1.json.zst" },
        { session_id: "s", key: "7/s-sibling/1.json.zst" },
      ],
    });
    mocks.deleteObjects.mockResolvedValue({ deleted: 1, failedKeys: [] });

    await enforceSiteQuota(7, 2 * GiB, GiB);

    expect(mocks.deleteObjects).toHaveBeenCalledWith(["7/s/1.json.zst"]);
  });

  it("keeps the rows of a session whose objects were not all deleted so it is retried", async () => {
    routeQueries({
      sessions: [
        { session_id: "a", bytes: GiB, last_at: "2026-11-22 01:00:00.000" },
        { session_id: "b", bytes: GiB, last_at: "2026-11-22 02:00:00.000" },
        { session_id: "c", bytes: GiB, last_at: "2026-11-22 03:00:00.000" },
      ],
      keys: [
        { session_id: "a", key: "7/a/1.json.zst" },
        { session_id: "b", key: "7/b/1.json.zst" },
      ],
    });
    mocks.deleteObjects.mockResolvedValue({ deleted: 1, failedKeys: ["7/a/1.json.zst"] });

    const result = await enforceSiteQuota(7, 3 * GiB, GiB);

    for (const [args] of mocks.command.mock.calls) {
      expect(args.query_params.sessionIds).toEqual(["b"]);
    }
    expect(result).toMatchObject({ evictedSessions: 1, failedSessions: 1 });
  });

  it("deletes no rows when the object store rejects every key", async () => {
    routeQueries({
      sessions: [{ session_id: "a", bytes: 2 * GiB, last_at: "2026-11-22 01:00:00.000" }],
      keys: [{ session_id: "a", key: "7/a/1.json.zst" }],
    });
    mocks.deleteObjects.mockResolvedValue({ deleted: 0, failedKeys: ["7/a/1.json.zst"] });

    await enforceSiteQuota(7, 2 * GiB, GiB);

    expect(mocks.command).not.toHaveBeenCalled();
  });
});

describe("runSiteQuotaOnce", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.command.mockResolvedValue(undefined);
  });

  it("keeps enforcing other sites when one site fails", async () => {
    mocks.query.mockImplementation(async ({ query, query_params }: { query: string; query_params: { siteId?: number } }) => {
      if (query.includes("HAVING used_bytes")) {
        return { rows: [{ site_id: 1, used_bytes: 2 * GiB }, { site_id: 2, used_bytes: 2 * GiB }] };
      }
      if (query_params.siteId === 1) throw new Error("clickhouse down");
      if (query.includes("UNION DISTINCT")) return { rows: [{ session_id: "x", key: "2/x/1.json.zst" }] };
      return { rows: [{ session_id: "x", bytes: 2 * GiB, last_at: "2026-11-22 01:00:00.000" }] };
    });
    mocks.deleteObjects.mockResolvedValue({ deleted: 1, failedKeys: [] });

    const results = await runSiteQuotaOnce(GiB);

    expect(results.map(result => result.siteId)).toEqual([2]);
  });
});
