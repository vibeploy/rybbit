import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  exec: vi.fn(),
  logger: { debug: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

vi.mock("./client.js", () => ({
  CLICKHOUSE_REQUEST_TIMEOUT_MS: 1000,
  clickhouse: { exec: mocks.exec },
}));

vi.mock("../../lib/logger/logger.js", () => ({
  createServiceLogger: () => mocks.logger,
}));

import { execClickhouseInitStep, modifyClickhouseTableTtl } from "./initUtils.js";

describe("ClickHouse TTL init steps", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.exec.mockResolvedValue(undefined);
  });

  it("never materializes a modified TTL", async () => {
    await modifyClickhouseTableTtl("ttl", "session_replay_events", "toDateTime(timestamp) + INTERVAL 24 HOUR");

    expect(mocks.exec).toHaveBeenCalledWith({
      query: "ALTER TABLE session_replay_events MODIFY TTL toDateTime(timestamp) + INTERVAL 24 HOUR",
      clickhouse_settings: { materialize_ttl_after_modify: 0 },
    });
  });

  it("keeps the materialize guard when a lock timeout is requested", async () => {
    await modifyClickhouseTableTtl("ttl", "events", "timestamp + INTERVAL 1 DAY", { lockAcquireTimeoutSeconds: 5 });

    expect(mocks.exec.mock.calls[0][0].clickhouse_settings).toEqual({
      materialize_ttl_after_modify: 0,
      lock_acquire_timeout: 5,
    });
  });

  it("rejects raw MODIFY TTL through the generic step before reaching ClickHouse, even when optional", async () => {
    await expect(
      execClickhouseInitStep("raw", "ALTER TABLE events modify  ttl timestamp + INTERVAL 1 DAY", { optional: true })
    ).rejects.toThrow(/modifyClickhouseTableTtl/);
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it("rejects table identifiers that could inject SQL", async () => {
    await expect(modifyClickhouseTableTtl("ttl", "events; DROP TABLE events", "timestamp")).rejects.toThrow(
      /invalid table identifier/
    );
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it("still swallows failures of optional TTL steps after logging them", async () => {
    mocks.exec.mockRejectedValueOnce(new Error("boom"));

    await expect(
      modifyClickhouseTableTtl("ttl", "events", "timestamp + INTERVAL 1 DAY", { optional: true })
    ).resolves.toBeUndefined();
    expect(mocks.logger.error).toHaveBeenCalledOnce();
  });

  it("leaves ordinary steps without extra settings", async () => {
    await execClickhouseInitStep("create", "CREATE TABLE IF NOT EXISTS t (x UInt8) ENGINE = Memory");

    expect(mocks.exec).toHaveBeenCalledWith({
      query: "CREATE TABLE IF NOT EXISTS t (x UInt8) ENGINE = Memory",
      clickhouse_settings: undefined,
    });
  });
});
