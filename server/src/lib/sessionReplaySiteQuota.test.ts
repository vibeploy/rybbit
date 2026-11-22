import { describe, expect, it } from "vitest";
import {
  parseSessionReplaySiteQuotaBytes,
  parseSessionReplaySiteQuotaIntervalMinutes,
} from "./sessionReplaySiteQuota.js";

describe("parseSessionReplaySiteQuotaBytes", () => {
  it("is disabled when unset or blank", () => {
    expect(parseSessionReplaySiteQuotaBytes(undefined)).toBeNull();
    expect(parseSessionReplaySiteQuotaBytes("  ")).toBeNull();
  });

  it("accepts a whole byte count inside the bounds", () => {
    expect(parseSessionReplaySiteQuotaBytes("1073741824")).toBe(1073741824);
  });

  it.each(["1G", "-1", "1.5", "1000", String(2 * 1024 ** 4)])("rejects %s", value => {
    expect(() => parseSessionReplaySiteQuotaBytes(value)).toThrow();
  });
});

describe("parseSessionReplaySiteQuotaIntervalMinutes", () => {
  it("defaults to five minutes", () => {
    expect(parseSessionReplaySiteQuotaIntervalMinutes(undefined)).toBe(5);
  });

  it.each(["0", "61", "x"])("rejects %s", value => {
    expect(() => parseSessionReplaySiteQuotaIntervalMinutes(value)).toThrow();
  });
});
