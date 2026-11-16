/**
 * Raised when replay object storage is enabled but a batch could not be stored
 * there. The batch is not written anywhere else; the ingest route reports it
 * as a dropped batch (503) instead of a success.
 */
const SAFE_ERROR_CODE = /^[A-Za-z0-9_.-]{1,64}$/;

/** Only a short, identifier-like code is kept (e.g. ECONNREFUSED); never messages. */
function safeErrorCode(cause: unknown): string | null {
  const code = (cause as { code?: unknown } | null)?.code;
  return typeof code === "string" && SAFE_ERROR_CODE.test(code) ? code : null;
}

export class ReplayObjectStorageUnavailableError extends Error {
  readonly storageErrorName: string;
  readonly storageErrorCode: string | null;

  constructor(
    readonly siteId: number,
    readonly eventCount: number,
    cause: unknown
  ) {
    const storageErrorName =
      cause instanceof Error ? cause.name || "Error" : cause === null ? "MissingObjectKey" : "UnknownError";
    super(`Session replay object storage unavailable (${storageErrorName})`);
    this.name = "ReplayObjectStorageUnavailableError";
    this.storageErrorName = storageErrorName;
    this.storageErrorCode = safeErrorCode(cause);
  }
}

export interface DroppedReplaySummary {
  droppedBatches: number;
  droppedEvents: number;
  lastStorageErrorName: string;
  lastStorageErrorCode: string | null;
  lastSiteId: number;
}

/**
 * Aggregates dropped replay batches so an object-storage outage produces one
 * log line per window instead of one per request. The first drop after a quiet
 * window is reported immediately.
 */
export class DroppedReplayBatchReporter {
  private lastReportAt: number | null = null;
  private pendingBatches = 0;
  private pendingEvents = 0;

  constructor(
    private readonly windowMs: number,
    private readonly now: () => number = Date.now
  ) {}

  record(error: ReplayObjectStorageUnavailableError): DroppedReplaySummary | null {
    this.pendingBatches += 1;
    this.pendingEvents += error.eventCount;
    const now = this.now();
    if (this.lastReportAt !== null && now - this.lastReportAt < this.windowMs) {
      return null;
    }
    const summary: DroppedReplaySummary = {
      droppedBatches: this.pendingBatches,
      droppedEvents: this.pendingEvents,
      lastStorageErrorName: error.storageErrorName,
      lastStorageErrorCode: error.storageErrorCode,
      lastSiteId: error.siteId,
    };
    this.lastReportAt = now;
    this.pendingBatches = 0;
    this.pendingEvents = 0;
    return summary;
  }
}
