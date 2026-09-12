import { describe, expect, it } from "vitest";
import { parseSessionReplayRetentionHours } from "./sessionReplayRetention.js";

describe("parseSessionReplayRetentionHours", () => {
  it("preserves the upstream 30-day default", () => {
    expect(parseSessionReplayRetentionHours(undefined)).toBe(720);
    expect(parseSessionReplayRetentionHours("  ")).toBe(720);
  });

  it("accepts a 48-hour retention window", () => {
    expect(parseSessionReplayRetentionHours("48")).toBe(48);
  });

  it.each(["0", "1.5", "-1", "abc", "8761"])("rejects an invalid retention value: %s", value => {
    expect(() => parseSessionReplayRetentionHours(value)).toThrow(/SESSION_REPLAY_RETENTION_HOURS/);
  });
});
