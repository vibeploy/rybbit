const MIN_SITE_QUOTA_BYTES = 64 * 1024 * 1024;
const MAX_SITE_QUOTA_BYTES = 1024 * 1024 * 1024 * 1024;
const DEFAULT_INTERVAL_MINUTES = 5;

/**
 * Per-site cap on replay bytes held in object storage. Unset means no cap:
 * the quota job never starts and only the time-based retention applies.
 */
export function parseSessionReplaySiteQuotaBytes(value: string | undefined): number | null {
  if (value === undefined || value.trim() === "") return null;
  if (!/^\d+$/.test(value.trim())) {
    throw new Error("SESSION_REPLAY_SITE_QUOTA_BYTES must be a whole number of bytes");
  }
  const bytes = Number(value.trim());
  if (!Number.isSafeInteger(bytes) || bytes < MIN_SITE_QUOTA_BYTES || bytes > MAX_SITE_QUOTA_BYTES) {
    throw new Error(
      `SESSION_REPLAY_SITE_QUOTA_BYTES must be between ${MIN_SITE_QUOTA_BYTES} and ${MAX_SITE_QUOTA_BYTES}`
    );
  }
  return bytes;
}

export function parseSessionReplaySiteQuotaIntervalMinutes(value: string | undefined): number {
  if (value === undefined || value.trim() === "") return DEFAULT_INTERVAL_MINUTES;
  if (!/^\d+$/.test(value.trim())) {
    throw new Error("SESSION_REPLAY_SITE_QUOTA_INTERVAL_MINUTES must be a whole number");
  }
  const minutes = Number(value.trim());
  if (minutes < 1 || minutes > 60) {
    throw new Error("SESSION_REPLAY_SITE_QUOTA_INTERVAL_MINUTES must be between 1 and 60");
  }
  return minutes;
}

export const SESSION_REPLAY_SITE_QUOTA_BYTES = parseSessionReplaySiteQuotaBytes(
  process.env.SESSION_REPLAY_SITE_QUOTA_BYTES
);

export const SESSION_REPLAY_SITE_QUOTA_INTERVAL_MINUTES = parseSessionReplaySiteQuotaIntervalMinutes(
  process.env.SESSION_REPLAY_SITE_QUOTA_INTERVAL_MINUTES
);
