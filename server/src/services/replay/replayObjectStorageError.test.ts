import { describe, expect, it } from "vitest";
import { DroppedReplayBatchReporter, ReplayObjectStorageUnavailableError } from "./replayObjectStorageError.js";

describe("ReplayObjectStorageUnavailableError", () => {
  it("names the storage failure without carrying the raw cause", () => {
    const cause = Object.assign(new Error("secret-bearing message"), { name: "AccessDenied" });
    const error = new ReplayObjectStorageUnavailableError(7, 3, cause);
    expect(error.storageErrorName).toBe("AccessDenied");
    expect(error.message).not.toContain("secret-bearing");
    expect(error.storageErrorCode).toBeNull();
    expect(new ReplayObjectStorageUnavailableError(7, 3, null).storageErrorName).toBe("MissingObjectKey");
  });

  it("keeps only identifier-like error codes", () => {
    const refused = Object.assign(new Error("connect ECONNREFUSED 10.0.0.1:9000"), { code: "ECONNREFUSED" });
    expect(new ReplayObjectStorageUnavailableError(1, 1, refused).storageErrorCode).toBe("ECONNREFUSED");
    const noisy = Object.assign(new Error("x"), { code: "token=abc def" });
    expect(new ReplayObjectStorageUnavailableError(1, 1, noisy).storageErrorCode).toBeNull();
    const numeric = Object.assign(new Error("x"), { code: 503 });
    expect(new ReplayObjectStorageUnavailableError(1, 1, numeric).storageErrorCode).toBeNull();
  });
});

describe("DroppedReplayBatchReporter", () => {
  it("reports the first drop immediately and aggregates drops inside the window", () => {
    let now = 1_000;
    const reporter = new DroppedReplayBatchReporter(60_000, () => now);
    const drop = (events: number) => new ReplayObjectStorageUnavailableError(1, events, new Error("x"));

    expect(reporter.record(drop(2))).toMatchObject({ droppedBatches: 1, droppedEvents: 2 });
    now += 30_000;
    expect(reporter.record(drop(5))).toBeNull();
    now += 10_000;
    expect(reporter.record(drop(1))).toBeNull();
    now += 25_000;
    expect(reporter.record(drop(4))).toMatchObject({ droppedBatches: 3, droppedEvents: 10 });
  });
});
