import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  query: vi.fn(),
  processResults: vi.fn(),
}));

vi.mock("../../db/clickhouse/clickhouse.js", () => ({
  clickhouse: { query: mocks.query },
}));

vi.mock("../../api/analytics/utils/utils.js", () => ({
  processResults: mocks.processResults,
}));

vi.mock("../storage/r2StorageService.js", () => ({
  r2Storage: { isEnabled: () => false },
}));

describe("SessionReplayQueryService retention", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.stubEnv("SESSION_REPLAY_RETENTION_HOURS", "48");
    mocks.query.mockReset().mockResolvedValue({});
    mocks.processResults.mockReset().mockResolvedValue([]);
  });

  it("hides expired replay metadata before ClickHouse finishes TTL merges", async () => {
    const { SessionReplayQueryService } = await import("./sessionReplayQueryService.js");
    await new SessionReplayQueryService().getSessionReplayMetadata(1, "session-1");

    expect(mocks.query).toHaveBeenCalledWith(
      expect.objectContaining({
        query: expect.stringContaining("start_time >= now64(3) - INTERVAL 48 HOUR"),
      })
    );
  });

  it("bounds both event and metadata scans when listing replays", async () => {
    const { SessionReplayQueryService } = await import("./sessionReplayQueryService.js");
    await new SessionReplayQueryService().getSessionReplayList(1, {
      start_date: "",
      end_date: "",
      time_zone: "UTC",
      filters: "",
    });

    const query = mocks.query.mock.calls[0][0].query as string;
    expect(query).toContain("start_time >= now64(3) - INTERVAL 48 HOUR");
    expect(query).toContain("timestamp >= now64(3) - INTERVAL 48 HOUR");
  });
});
