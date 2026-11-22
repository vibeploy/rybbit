import * as cron from "node-cron";
import { clickhouse } from "../../db/clickhouse/clickhouse.js";
import { redis } from "../../db/redis/redis.js";
import { acquireRedisLease } from "../../lib/redisLease.js";
import { processResults } from "../../api/analytics/utils/utils.js";
import { createServiceLogger } from "../../lib/logger/logger.js";
import {
  SESSION_REPLAY_SITE_QUOTA_BYTES,
  SESSION_REPLAY_SITE_QUOTA_INTERVAL_MINUTES,
} from "../../lib/sessionReplaySiteQuota.js";
import { r2Storage } from "../storage/r2StorageService.js";

const logger = createServiceLogger("replay-site-quota");

const LOCK_KEY = "replay:site-quota:lock";
// Bounds one run so a huge backlog is worked off over several ticks instead
// of one unbounded ClickHouse DELETE or bucket request storm.
const MAX_SESSIONS_PER_SITE_PER_RUN = 2000;
const SESSION_ID_CHUNK = 500;
const QUERY_SETTINGS = { max_execution_time: 60 } as const;
// A run can outlast the interval, so the lock is a short lease the holder
// keeps renewing instead of a fixed TTL that expires mid-run and lets another
// replica start evicting the same sessions.
const LEASE_TTL_MS = 60_000;
const LEASE_RENEW_MS = 20_000;

/** Checked before each unit of work; false means stop without starting more. */
export type ContinueCheck = () => boolean;
const always: ContinueCheck = () => true;

export interface ReplaySessionUsage {
  sessionId: string;
  bytes: number;
  lastAt: string;
}

/**
 * Keeps the most recently active sessions whose combined size fits the quota
 * and returns the rest, oldest first. Sessions are evicted whole: deleting the
 * front of a recording loses its full snapshot and leaves nothing playable.
 * Once one session does not fit, everything older is evicted too, so the
 * kept set is always "the newest N sessions", never a gap-filled mix.
 */
export function planSiteEviction(sessions: ReplaySessionUsage[], quotaBytes: number): ReplaySessionUsage[] {
  const newestFirst = [...sessions].sort((a, b) =>
    a.lastAt === b.lastAt ? a.sessionId.localeCompare(b.sessionId) : a.lastAt < b.lastAt ? 1 : -1
  );
  let kept = 0;
  let cutoff = newestFirst.length;
  for (let index = 0; index < newestFirst.length; index += 1) {
    if (kept + newestFirst[index].bytes > quotaBytes) {
      cutoff = index;
      break;
    }
    kept += newestFirst[index].bytes;
  }
  return newestFirst.slice(cutoff).reverse();
}

export interface SiteQuotaRunResult {
  siteId: number;
  usedBytes: number;
  evictedSessions: number;
  evictedBytes: number;
  deletedObjects: number;
  failedSessions: number;
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let offset = 0; offset < items.length; offset += size) chunks.push(items.slice(offset, offset + size));
  return chunks;
}

async function sitesOverQuota(quotaBytes: number): Promise<{ siteId: number; usedBytes: number }[]> {
  const result = await clickhouse.query({
    query: `
      SELECT site_id, sum(size_bytes) AS used_bytes
      FROM session_replay_objects
      GROUP BY site_id
      HAVING used_bytes > {quota:UInt64}
      ORDER BY used_bytes DESC
    `,
    query_params: { quota: quotaBytes },
    format: "JSONEachRow",
    clickhouse_settings: QUERY_SETTINGS,
  });
  const rows = await processResults<{ site_id: number; used_bytes: number | string }>(result);
  return rows.map(row => ({ siteId: Number(row.site_id), usedBytes: Number(row.used_bytes) }));
}

async function siteSessions(siteId: number): Promise<ReplaySessionUsage[]> {
  const result = await clickhouse.query({
    query: `
      SELECT session_id, sum(size_bytes) AS bytes, toString(max(created_at)) AS last_at
      FROM session_replay_objects
      WHERE site_id = {siteId:UInt16}
      GROUP BY session_id
    `,
    query_params: { siteId },
    format: "JSONEachRow",
    clickhouse_settings: QUERY_SETTINGS,
  });
  const rows = await processResults<{ session_id: string; bytes: number | string; last_at: string }>(result);
  return rows.map(row => ({ sessionId: row.session_id, bytes: Number(row.bytes), lastAt: row.last_at }));
}

/**
 * Every object key the session references: the accounted ones plus any batch
 * written before accounting existed, which only the events table knows about.
 */
async function sessionObjectKeys(siteId: number, sessionIds: string[]): Promise<Map<string, Set<string>>> {
  const result = await clickhouse.query({
    query: `
      SELECT session_id, object_key AS key
      FROM session_replay_objects
      WHERE site_id = {siteId:UInt16} AND session_id IN {sessionIds:Array(String)}
      UNION DISTINCT
      SELECT session_id, assumeNotNull(event_data_key) AS key
      FROM session_replay_events
      WHERE site_id = {siteId:UInt16} AND session_id IN {sessionIds:Array(String)} AND event_data_key IS NOT NULL
    `,
    query_params: { siteId, sessionIds },
    format: "JSONEachRow",
    clickhouse_settings: QUERY_SETTINGS,
  });
  const rows = await processResults<{ session_id: string; key: string }>(result);
  const keys = new Map<string, Set<string>>();
  for (const row of rows) {
    // Only ever delete inside the session's own prefix, whatever a row says.
    if (!row.key.startsWith(`${siteId}/${row.session_id}/`)) continue;
    const set = keys.get(row.session_id) ?? new Set<string>();
    set.add(row.key);
    keys.set(row.session_id, set);
  }
  return keys;
}

async function deleteSessionRows(siteId: number, sessionIds: string[]): Promise<void> {
  // Objects table last: if an earlier delete fails, the session is still
  // accounted and the next run retries the whole eviction.
  for (const table of ["session_replay_metadata_v2", "session_replay_events", "session_replay_objects"]) {
    await clickhouse.command({
      query: `DELETE FROM ${table} WHERE site_id = {siteId:UInt16} AND session_id IN {sessionIds:Array(String)}`,
      query_params: { siteId, sessionIds },
      clickhouse_settings: QUERY_SETTINGS,
    });
  }
}

export async function enforceSiteQuota(
  siteId: number,
  usedBytes: number,
  quotaBytes: number,
  shouldContinue: ContinueCheck = always
): Promise<SiteQuotaRunResult> {
  const evictions = planSiteEviction(await siteSessions(siteId), quotaBytes).slice(0, MAX_SESSIONS_PER_SITE_PER_RUN);
  const result: SiteQuotaRunResult = {
    siteId,
    usedBytes,
    evictedSessions: 0,
    evictedBytes: 0,
    deletedObjects: 0,
    failedSessions: 0,
  };

  for (const group of chunk(evictions, SESSION_ID_CHUNK)) {
    if (!shouldContinue()) break;
    const sessionIds = group.map(session => session.sessionId);
    const keysBySession = await sessionObjectKeys(siteId, sessionIds);
    const allKeys = [...keysBySession.values()].flatMap(keys => [...keys]);
    const { deleted, failedKeys } = await r2Storage.deleteObjects(allKeys);
    result.deletedObjects += deleted;

    // A session whose objects were not all confirmed deleted keeps its rows,
    // so it stays accounted and is retried on the next run.
    const failed = new Set(failedKeys);
    const cleared = group.filter(session => ![...(keysBySession.get(session.sessionId) ?? [])].some(key => failed.has(key)));
    result.failedSessions += group.length - cleared.length;
    if (cleared.length === 0) continue;

    await deleteSessionRows(
      siteId,
      cleared.map(session => session.sessionId)
    );
    result.evictedSessions += cleared.length;
    result.evictedBytes += cleared.reduce((total, session) => total + session.bytes, 0);
  }
  return result;
}

export async function runSiteQuotaOnce(
  quotaBytes: number,
  shouldContinue: ContinueCheck = always
): Promise<SiteQuotaRunResult[]> {
  const results: SiteQuotaRunResult[] = [];
  for (const site of await sitesOverQuota(quotaBytes)) {
    if (!shouldContinue()) {
      logger.warn({ siteId: site.siteId }, "Session replay site quota lease lost; stopping run");
      break;
    }
    try {
      const result = await enforceSiteQuota(site.siteId, site.usedBytes, quotaBytes, shouldContinue);
      results.push(result);
      logger.info({ ...result, quotaBytes }, "Session replay site quota enforced");
      if (result.failedSessions > 0) {
        logger.warn({ siteId: site.siteId, failedSessions: result.failedSessions }, "Replay objects not deleted; retrying next run");
      }
    } catch (error) {
      logger.error({ err: error, siteId: site.siteId }, "Session replay site quota enforcement failed");
    }
  }
  return results;
}

class SessionReplaySiteQuotaService {
  private task: cron.ScheduledTask | null = null;
  private running = false;

  start(): void {
    const quotaBytes = SESSION_REPLAY_SITE_QUOTA_BYTES;
    if (quotaBytes === null) return;
    if (!r2Storage.isEnabled()) {
      logger.warn("SESSION_REPLAY_SITE_QUOTA_BYTES is set but replay object storage is disabled; quota job not started");
      return;
    }
    const minutes = SESSION_REPLAY_SITE_QUOTA_INTERVAL_MINUTES;
    this.task = cron.schedule(`*/${minutes} * * * *`, () => void this.tick(quotaBytes), { timezone: "UTC" });
    void this.tick(quotaBytes);
    logger.info({ quotaBytes, intervalMinutes: minutes }, "Session replay site quota job started");
  }

  stop(): void {
    this.task?.stop();
    this.task = null;
  }

  /** Visible for tests. */
  async tick(quotaBytes: number): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      // One replica runs at a time. Without Redis we skip rather than let
      // every replica evict at once; the next tick tries again.
      let lease;
      try {
        lease = await acquireRedisLease(redis, LOCK_KEY, {
          ttlMs: LEASE_TTL_MS,
          renewEveryMs: LEASE_RENEW_MS,
          onLost: (reason, error) =>
            logger.warn({ err: error, reason }, "Session replay site quota lease lost; run will stop"),
        });
      } catch (error) {
        logger.warn({ err: error }, "Session replay site quota lock unavailable; skipping run");
        return;
      }
      if (!lease) return;
      try {
        await runSiteQuotaOnce(quotaBytes, lease.held);
      } finally {
        await lease.release();
      }
    } catch (error) {
      logger.error({ err: error }, "Session replay site quota run failed");
    } finally {
      this.running = false;
    }
  }
}

export const sessionReplaySiteQuotaService = new SessionReplaySiteQuotaService();
