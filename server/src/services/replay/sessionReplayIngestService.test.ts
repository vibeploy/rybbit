import { beforeEach, describe, expect, it, vi } from "vitest";
import type { RecordSessionReplayRequest } from "../../types/sessionReplay.js";

const mocks = vi.hoisted(() => ({
  generateUserId: vi.fn(),
  insert: vi.fn(),
  updateSession: vi.fn(),
  isStorageEnabled: vi.fn(() => false),
  storeBatch: vi.fn(),
}));

vi.mock("../../db/clickhouse/clickhouse.js", () => ({
  clickhouse: {
    insert: mocks.insert,
  },
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

describe("SessionReplayIngestService object storage failures", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.generateUserId.mockResolvedValue("fingerprint");
    mocks.updateSession.mockResolvedValue({ sessionId: "session-1" });
    mocks.insert.mockResolvedValue(undefined);
    mocks.isStorageEnabled.mockReturnValue(false);
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

  it("accounts each stored object with its real size before the events that reference it", async () => {
    mocks.isStorageEnabled.mockReturnValue(true);
    mocks.storeBatch.mockResolvedValue({ key: "42/session-1/1.json.zst", sizeBytes: 321 });

    const service = new SessionReplayIngestService();
    await service.recordEvents(
      42,
      { userId: "", events: [{ type: 2, data: { stored: true }, timestamp: 1_700_000_000_000 }] },
      requestMeta
    );

    const tables = mocks.insert.mock.calls.map(call => call[0].table);
    expect(tables.indexOf("session_replay_objects")).toBeLessThan(tables.indexOf("session_replay_events"));
    const objectInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_objects");
    expect(objectInsert?.[0].values).toEqual([
      expect.objectContaining({
        site_id: 42,
        session_id: "session-1",
        object_key: "42/session-1/1.json.zst",
        size_bytes: 321,
      }),
    ]);
    const eventInsert = mocks.insert.mock.calls.find(call => call[0].table === "session_replay_events");
    expect(eventInsert?.[0].values[0]).toMatchObject({ event_data: "", event_data_key: "42/session-1/1.json.zst" });
  });

  it("keeps inline ClickHouse bodies when S3 storage is disabled", async () => {
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
});
