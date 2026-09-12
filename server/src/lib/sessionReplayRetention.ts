const DEFAULT_SESSION_REPLAY_RETENTION_HOURS = 30 * 24;

export function parseSessionReplayRetentionHours(value: string | undefined): number {
  if (value === undefined || value.trim() === "") return DEFAULT_SESSION_REPLAY_RETENTION_HOURS;
  if (!/^\d+$/.test(value)) {
    throw new Error("SESSION_REPLAY_RETENTION_HOURS must be a positive whole number");
  }

  const hours = Number(value);
  if (!Number.isSafeInteger(hours) || hours < 1 || hours > 365 * 24) {
    throw new Error("SESSION_REPLAY_RETENTION_HOURS must be between 1 and 8760");
  }
  return hours;
}

export const SESSION_REPLAY_RETENTION_HOURS = parseSessionReplayRetentionHours(
  process.env.SESSION_REPLAY_RETENTION_HOURS
);

export const SESSION_REPLAY_RETENTION_DAYS = Math.ceil(SESSION_REPLAY_RETENTION_HOURS / 24);
