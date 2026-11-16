import { beforeEach, describe, expect, it, vi } from "vitest";
import type { RecordSessionReplayRequest } from "../../types/sessionReplay.js";

const mocks = vi.hoisted(() => ({
  generateUserId: vi.fn(),
  insert: vi.fn(),
  query: vi.fn(),
  releaseReplayMetadataLock: vi.fn(),
  isStorageEnabled: vi.fn(),
  storeBatch: vi.fn(),
  tryAcquireReplayMetadataLock: vi.fn(),
  updateSession: vi.fn(),
}));

vi.mock("../../db/clickhouse/clickhouse.js", () => ({
  clickhouse: {
    insert: mocks.insert,
    query: mocks.query,
  },
}));

vi.mock("../../db/redis/redis.js", () => ({
  releaseReplayMetadataLock: mocks.releaseReplayMetadataLock,
  tryAcquireReplayMetadataLock: mocks.tryAcquireReplayMetadataLock,
}));

vi.mock("../sessions/sessionsService.js", () => ({
  sessionsService: {
    updateSession: mocks.updateSession,
  },
}));

vi.mock("../userId/userIdService.js", () => ({
  userIdService: {
    generateUserId: mocks.generateUserId,
  },
}));

vi.mock("../storage/r2StorageService.js", () => ({
  r2Storage: {
    isEnabled: mocks.isStorageEnabled,
    storeBatch: mocks.storeBatch,
  },
}));

vi.mock("../../lib/siteConfig.js", () => ({
  siteConfig: {},
}));

import { ReplayObjectStorageUnavailableError, SessionReplayIngestService } from "./sessionReplayIngestService.js";

const requestMeta = {
  ipAddress: "198.51.100.10",
  userAgent: "Standardized Corporate Browser/1.0",
  origin: "https://internal.example",
  referrer: "",
};

function replayRequest(identifiedUserId: string): RecordSessionReplayRequest {
  return {
    userId: identifiedUserId,
    events: [{ type: 2, data: { user: identifiedUserId }, timestamp: 1_700_000_000_000 }],
  };
}

describe("SessionReplayIngestService identity", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.generateUserId.mockResolvedValue("shared-fingerprint");
    mocks.updateSession.mockImplementation(
      async ({ userId, identifiedUserId }: { userId: string; identifiedUserId?: string }) => ({
        sessionId: `session-${userId}-${identifiedUserId || "anonymous"}`,
      })
    );
    mocks.insert.mockResolvedValue(undefined);
    mocks.isStorageEnabled.mockReturnValue(false);
    mocks.query.mockResolvedValue({ json: async () => [] });
    mocks.tryAcquireReplayMetadataLock.mockResolvedValue(true);
    mocks.releaseReplayMetadataLock.mockResolvedValue(1);
  });

  it("separates identified replay users behind a shared proxy", async () => {
    const service = new SessionReplayIngestService();

    await service.recordEvents(42, replayRequest("employee-alice"), requestMeta);
    await service.recordEvents(42, replayRequest("employee-bob"), requestMeta);

    expect(mocks.updateSession).toHaveBeenNthCalledWith(1, {
      userId: "shared-fingerprint",
      identifiedUserId: "employee-alice",
      siteId: 42,
    });
    expect(mocks.updateSession).toHaveBeenNthCalledWith(2, {
      userId: "shared-fingerprint",
      identifiedUserId: "employee-bob",
      siteId: 42,
    });

    const insertedRows = mocks.insert.mock.calls.flatMap(call => call[0].values);
    expect(new Set(insertedRows.map(row => row.user_id))).toEqual(new Set(["shared-fingerprint"]));
    expect(new Set(insertedRows.map(row => row.identified_user_id))).toEqual(
      new Set(["employee-alice", "employee-bob"])
    );
    expect(new Set(insertedRows.map(row => row.session_id)).size).toBe(2);
  });

  it("retains the existing anonymous replay session key", async () => {
    const service = new SessionReplayIngestService();

    await service.recordEvents(42, replayRequest(""), requestMeta);

    expect(mocks.generateUserId).toHaveBeenCalledWith(requestMeta.ipAddress, requestMeta.userAgent, 42);
    expect(mocks.updateSession).toHaveBeenCalledWith({
      userId: "shared-fingerprint",
      identifiedUserId: "",
      siteId: 42,
    });
  });
});

describe("SessionReplayIngestService metadata aggregation", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.generateUserId.mockResolvedValue("fingerprint");
    mocks.updateSession.mockResolvedValue({ sessionId: "session-1" });
    mocks.insert.mockResolvedValue(undefined);
    mocks.isStorageEnabled.mockReturnValue(false);
    mocks.query.mockResolvedValue({ json: async () => [] });
    mocks.tryAcquireReplayMetadataLock.mockResolvedValue(true);
    mocks.releaseReplayMetadataLock.mockResolvedValue(1);
  });

  it("adds the current batch to the latest metadata snapshot without scanning replay events", async () => {
    mocks.query.mockResolvedValue({
      json: async () => [
        {
          start_time: "2023-11-14 22:13:10",
          end_time: "2023-11-14 22:13:15",
          event_count: "4",
          compressed_size_bytes: "100",
          screen_width: "1024",
          screen_height: "768",
        },
      ],
    });

    const service = new SessionReplayIngestService();
    await service.recordEvents(
      42,
      {
        userId: "",
        events: [
          { type: 2, data: { a: 1 }, timestamp: 1_700_000_000_000 },
          { type: 3, data: { b: 2 }, timestamp: 1_700_000_010_000 },
        ],
        metadata: {
          pageUrl: "https://example.com/dashboard",
          viewportWidth: 1440,
          viewportHeight: 900,
        },
      },
      requestMeta
    );

    expect(mocks.query).toHaveBeenCalledTimes(1);
    const query = mocks.query.mock.calls[0][0].query as string;
    expect(query).toContain("FROM session_replay_metadata");
    expect(query).not.toContain("FROM session_replay_events");

    const metadataInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_metadata");
    expect(metadataInsert?.[0].values[0]).toMatchObject({
      event_count: 6,
      compressed_size_bytes: 114,
      screen_width: 1440,
      screen_height: 900,
    });
    expect(mocks.releaseReplayMetadataLock).toHaveBeenCalledTimes(1);
  });

  it("builds the first metadata snapshot entirely from the incoming batch", async () => {
    const service = new SessionReplayIngestService();
    await service.recordEvents(
      42,
      {
        userId: "",
        events: [{ type: 2, data: { first: true }, timestamp: 1_700_000_000_000 }],
        metadata: { pageUrl: "https://example.com/", viewportWidth: 1280, viewportHeight: 720 },
      },
      requestMeta
    );

    const metadataInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_metadata");
    expect(metadataInsert?.[0].values[0]).toMatchObject({
      start_time: "2023-11-14 22:13:20",
      end_time: "2023-11-14 22:13:20",
      duration_ms: 0,
      event_count: 1,
      screen_width: 1280,
      screen_height: 720,
    });
  });

  it("falls back to the authoritative event aggregate when Redis locking is unavailable", async () => {
    mocks.tryAcquireReplayMetadataLock.mockRejectedValue(new Error("redis unavailable"));
    mocks.query.mockResolvedValue({
      json: async () => [
        {
          start_time: "2023-11-14 22:13:20",
          end_time: "2023-11-14 22:13:25",
          event_count: "8",
          compressed_size_bytes: "512",
          screen_width: "1280",
          screen_height: "720",
        },
      ],
    });

    const service = new SessionReplayIngestService();
    await service.recordEvents(
      42,
      {
        userId: "",
        events: [{ type: 2, data: { first: true }, timestamp: 1_700_000_000_000 }],
        metadata: { pageUrl: "https://example.com/" },
      },
      requestMeta
    );

    expect(mocks.query.mock.calls[0][0].query).toContain("FROM session_replay_events");
    expect(mocks.releaseReplayMetadataLock).not.toHaveBeenCalled();
    const metadataInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_metadata");
    expect(metadataInsert?.[0].values[0]).toMatchObject({ event_count: 8, compressed_size_bytes: 512 });
  });

  it("does not fail a durable replay write when releasing the lock fails", async () => {
    mocks.releaseReplayMetadataLock.mockRejectedValue(new Error("redis disconnected"));

    const service = new SessionReplayIngestService();
    await expect(
      service.recordEvents(
        42,
        {
          userId: "",
          events: [{ type: 2, data: { first: true }, timestamp: 1_700_000_000_000 }],
          metadata: { pageUrl: "https://example.com/" },
        },
        requestMeta
      )
    ).resolves.toBeUndefined();
  });

  it("releases the lock when the metadata query fails", async () => {
    mocks.query.mockRejectedValue(new Error("clickhouse query failed"));

    const service = new SessionReplayIngestService();
    await expect(
      service.recordEvents(
        42,
        {
          userId: "",
          events: [{ type: 2, data: { first: true }, timestamp: 1_700_000_000_000 }],
          metadata: { pageUrl: "https://example.com/" },
        },
        requestMeta
      )
    ).rejects.toThrow("clickhouse query failed");

    expect(mocks.releaseReplayMetadataLock).toHaveBeenCalledTimes(1);
  });

  it("releases the lock when inserting the metadata snapshot fails", async () => {
    mocks.insert.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error("metadata insert failed"));

    const service = new SessionReplayIngestService();
    await expect(
      service.recordEvents(
        42,
        {
          userId: "",
          events: [{ type: 2, data: { first: true }, timestamp: 1_700_000_000_000 }],
          metadata: { pageUrl: "https://example.com/" },
        },
        requestMeta
      )
    ).rejects.toThrow("metadata insert failed");

    expect(mocks.releaseReplayMetadataLock).toHaveBeenCalledTimes(1);
  });

  it("uses the event aggregate fallback after lock contention exceeds the wait budget", async () => {
    vi.useFakeTimers();
    mocks.tryAcquireReplayMetadataLock.mockResolvedValue(false);
    mocks.query.mockResolvedValue({
      json: async () => [
        {
          start_time: "2023-11-14 22:13:20",
          end_time: "2023-11-14 22:13:25",
          event_count: "8",
          compressed_size_bytes: "512",
          screen_width: "1280",
          screen_height: "720",
        },
      ],
    });

    try {
      const service = new SessionReplayIngestService();
      const pending = service.recordEvents(
        42,
        {
          userId: "",
          events: [{ type: 2, data: { first: true }, timestamp: 1_700_000_000_000 }],
          metadata: { pageUrl: "https://example.com/" },
        },
        requestMeta
      );

      await vi.runAllTimersAsync();
      await pending;

      expect(mocks.tryAcquireReplayMetadataLock).toHaveBeenCalledTimes(80);
      expect(mocks.query.mock.calls[0][0].query).toContain("FROM session_replay_events");
    } finally {
      vi.useRealTimers();
    }
  });

  it("aggregates out-of-order event timestamps", async () => {
    const service = new SessionReplayIngestService();
    await service.recordEvents(
      42,
      {
        userId: "",
        events: [
          { type: 3, data: { order: 3 }, timestamp: 1_700_000_020_000 },
          { type: 2, data: { order: 1 }, timestamp: 1_700_000_000_000 },
          { type: 3, data: { order: 2 }, timestamp: 1_700_000_010_000 },
        ],
        metadata: { pageUrl: "https://example.com/" },
      },
      requestMeta
    );

    const metadataInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_metadata");
    expect(metadataInsert?.[0].values[0]).toMatchObject({
      start_time: "2023-11-14 22:13:20",
      end_time: "2023-11-14 22:13:40",
      duration_ms: 20_000,
      event_count: 3,
    });
  });

  it("does not create metadata for an empty first batch", async () => {
    const service = new SessionReplayIngestService();
    await service.recordEvents(
      42,
      { userId: "", events: [], metadata: { pageUrl: "https://example.com/" } },
      requestMeta
    );

    expect(mocks.insert).not.toHaveBeenCalled();
    expect(mocks.releaseReplayMetadataLock).toHaveBeenCalledTimes(1);
  });

  it("stores replay bodies in S3 while accumulating the same metadata stats", async () => {
    mocks.isStorageEnabled.mockReturnValue(true);
    mocks.storeBatch.mockResolvedValue("42/session-1/batch.json.zst");

    const service = new SessionReplayIngestService();
    await service.recordEvents(
      42,
      {
        userId: "",
        events: [{ type: 2, data: { stored: "in-s3" }, timestamp: 1_700_000_000_000 }],
        metadata: { pageUrl: "https://example.com/" },
      },
      requestMeta
    );

    const eventInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_events");
    expect(eventInsert?.[0].values[0]).toMatchObject({
      event_data: "",
      event_data_key: "42/session-1/batch.json.zst",
      batch_index: 0,
    });
    const metadataInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_metadata");
    expect(metadataInsert?.[0].values[0].event_count).toBe(1);
  });

  it("drops the batch without writing ClickHouse when S3 upload fails", async () => {
    mocks.isStorageEnabled.mockReturnValue(true);
    mocks.storeBatch.mockRejectedValue(Object.assign(new Error("quota exceeded"), { name: "QuotaExceeded" }));

    const service = new SessionReplayIngestService();
    const recording = service.recordEvents(
      42,
      {
        userId: "",
        events: [{ type: 2, data: { dropped: true }, timestamp: 1_700_000_000_000 }],
        metadata: { pageUrl: "https://example.com/" },
      },
      requestMeta
    );

    await expect(recording).rejects.toBeInstanceOf(ReplayObjectStorageUnavailableError);
    await expect(recording).rejects.toMatchObject({ storageErrorName: "QuotaExceeded", eventCount: 1 });
    expect(mocks.insert).not.toHaveBeenCalled();
    expect(mocks.query).not.toHaveBeenCalled();
    expect(mocks.tryAcquireReplayMetadataLock).not.toHaveBeenCalled();
  });

  it("treats a missing object key from enabled S3 storage as a failed upload", async () => {
    mocks.isStorageEnabled.mockReturnValue(true);
    mocks.storeBatch.mockResolvedValue(null);

    const service = new SessionReplayIngestService();
    await expect(
      service.recordEvents(
        42,
        { userId: "", events: [{ type: 2, data: {}, timestamp: 1_700_000_000_000 }] },
        requestMeta
      )
    ).rejects.toBeInstanceOf(ReplayObjectStorageUnavailableError);
    expect(mocks.insert).not.toHaveBeenCalled();
  });

  it("keeps inline ClickHouse bodies when S3 storage is disabled", async () => {
    mocks.isStorageEnabled.mockReturnValue(false);

    const service = new SessionReplayIngestService();
    await service.recordEvents(
      42,
      { userId: "", events: [{ type: 2, data: { inline: true }, timestamp: 1_700_000_000_000 }] },
      requestMeta
    );

    expect(mocks.storeBatch).not.toHaveBeenCalled();
    const eventInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_events");
    expect(eventInsert?.[0].values[0]).toMatchObject({
      event_data: JSON.stringify({ inline: true }),
      event_data_key: null,
    });
  });

  it("serializes concurrent metadata updates for the same session", async () => {
    let lockOwner: string | null = null;
    let snapshot: Record<string, unknown> | null = null;

    mocks.tryAcquireReplayMetadataLock.mockImplementation(async (_key: string, token: string) => {
      if (lockOwner !== null) return false;
      lockOwner = token;
      return true;
    });
    mocks.releaseReplayMetadataLock.mockImplementation(async (_key: string, token: string) => {
      if (lockOwner !== token) return 0;
      lockOwner = null;
      return 1;
    });
    mocks.query.mockImplementation(async () => ({ json: async () => (snapshot ? [snapshot] : []) }));
    mocks.insert.mockImplementation(async ({ table, values }: { table: string; values: unknown[] }) => {
      if (table === "session_replay_metadata") snapshot = values[0] as Record<string, unknown>;
    });

    const service = new SessionReplayIngestService();
    await Promise.all([
      service.recordEvents(
        42,
        {
          userId: "",
          events: [{ type: 2, data: { batch: 1 }, timestamp: 1_700_000_000_000 }],
          metadata: { pageUrl: "https://example.com/" },
        },
        requestMeta
      ),
      service.recordEvents(
        42,
        {
          userId: "",
          events: [{ type: 3, data: { batch: 2 }, timestamp: 1_700_000_010_000 }],
          metadata: { pageUrl: "https://example.com/" },
        },
        requestMeta
      ),
    ]);

    expect(snapshot).toMatchObject({ event_count: 2, duration_ms: 10_000 });
  });
});
