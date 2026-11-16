import { DateTime } from "luxon";
import { nanoid } from "nanoid";
import { clickhouse } from "../../db/clickhouse/clickhouse.js";
import { releaseReplayMetadataLock, tryAcquireReplayMetadataLock } from "../../db/redis/redis.js";
import { RecordSessionReplayRequest } from "../../types/sessionReplay.js";
import { processResults } from "../../api/analytics/utils/utils.js";
import { parseTrackingData } from "./trackingUtils.js";
import { sessionsService } from "../sessions/sessionsService.js";
import { userIdService } from "../userId/userIdService.js";
import { r2Storage } from "../storage/r2StorageService.js";
import { ReplayObjectStorageUnavailableError } from "./replayObjectStorageError.js";
import { siteConfig } from "../../lib/siteConfig.js";
import { LATEST_REPLAY_METADATA_QUERY, REPLAY_EVENT_AGGREGATE_QUERY } from "./sessionReplayMetadataQueries.js";

export { ReplayObjectStorageUnavailableError };

export interface RequestMetadata {
  userAgent: string;
  ipAddress: string;
  origin: string;
  referrer: string;
}

interface ReplayBatchStats {
  startTimeMs: number;
  endTimeMs: number;
  eventCount: number;
  compressedSizeBytes: number;
  screenWidth: number;
  screenHeight: number;
}

interface StoredReplayStats {
  start_time: string;
  end_time: string | null;
  event_count: number;
  compressed_size_bytes: number;
  screen_width: number | null;
  screen_height: number | null;
}

const METADATA_LOCK_TTL_MS = 120_000;
const METADATA_LOCK_RETRY_MS = 25;
const METADATA_LOCK_ATTEMPTS = 80;

function parseClickhouseDateTime(value: string | null): number | null {
  if (!value) return null;
  const normalized = value.includes("T") ? value : value.replace(" ", "T");
  const withZone = /(?:Z|[+-]\d{2}:?\d{2})$/.test(normalized) ? normalized : `${normalized}Z`;
  const timestamp = Date.parse(withZone);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function formatClickhouseDateTime(timestamp: number): string {
  return DateTime.fromMillis(timestamp, { zone: "utc" }).toFormat("yyyy-MM-dd HH:mm:ss");
}

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Service responsible for ingesting session replay data
 * Handles recording events and updating metadata
 */
export class SessionReplayIngestService {
  async recordEvents(
    siteId: number,
    request: RecordSessionReplayRequest,
    requestMeta?: RequestMetadata
  ): Promise<void> {
    const { userId: clientUserId, events, metadata } = request;

    // Always generate device fingerprint (anonymous user ID) server-side
    const deviceFingerprint = await userIdService.generateUserId(
      requestMeta?.ipAddress || "",
      requestMeta?.userAgent || "",
      siteId
    );

    // Check if client provided an identified user ID (different from device fingerprint)
    const trimmedClientUserId = clientUserId?.trim() || "";
    const identifiedUserId =
      trimmedClientUserId && trimmedClientUserId !== deviceFingerprint ? trimmedClientUserId : "";

    // Keep the device fingerprint as the stored user_id. The identified ID only
    // scopes session assignment so stored user identity semantics stay unchanged.
    const userId = deviceFingerprint;

    // Get or create a session ID from the sessions service
    const { sessionId } = await sessionsService.updateSession({
      userId,
      identifiedUserId,
      siteId,
    });

    // When S3-compatible object storage is enabled it is the only place replay
    // bodies may go. A failed upload drops the batch instead of inlining the
    // bodies into ClickHouse: the inline fallback turned an object-storage
    // outage into unbounded ClickHouse growth and backend OOMs.
    let r2BatchKey: string | null = null;

    if (r2Storage.isEnabled()) {
      const eventDataArray = events.map(event => event.data);
      let storageError: unknown = null;
      try {
        r2BatchKey = await r2Storage.storeBatch(siteId, sessionId, eventDataArray);
      } catch (error) {
        storageError = error;
      }
      if (!r2BatchKey) {
        throw new ReplayObjectStorageUnavailableError(siteId, events.length, storageError);
      }
    }

    // Prepare events for batch insert
    const eventsToInsert = events.map((event, index) => {
      const serializedData = JSON.stringify(event.data);

      if (r2BatchKey) {
        // Object storage: store metadata only in ClickHouse.
        return {
          site_id: siteId,
          session_id: sessionId,
          user_id: userId,
          identified_user_id: identifiedUserId,
          timestamp: event.timestamp,
          event_type: event.type,
          event_data: "", // Empty string when using R2
          event_data_key: r2BatchKey,
          batch_index: index,
          sequence_number: index,
          event_size_bytes: serializedData.length,
          viewport_width: metadata?.viewportWidth || null,
          viewport_height: metadata?.viewportHeight || null,
          is_complete: 0,
        };
      } else {
        // Traditional storage: store everything in ClickHouse
        return {
          site_id: siteId,
          session_id: sessionId,
          user_id: userId,
          identified_user_id: identifiedUserId,
          timestamp: event.timestamp,
          event_type: event.type,
          event_data: serializedData,
          event_data_key: null,
          batch_index: null,
          sequence_number: index,
          event_size_bytes: serializedData.length,
          viewport_width: metadata?.viewportWidth || null,
          viewport_height: metadata?.viewportHeight || null,
          is_complete: 0,
        };
      }
    });

    // Batch insert events
    if (eventsToInsert.length > 0) {
      await clickhouse.insert({
        table: "session_replay_events",
        values: eventsToInsert,
        format: "JSONEachRow",
      });
    }

    // Update or insert metadata
    if (metadata) {
      const timestamps = events.map(event => event.timestamp);
      const batchStats: ReplayBatchStats | null =
        timestamps.length > 0
          ? {
              startTimeMs: Math.min(...timestamps),
              endTimeMs: Math.max(...timestamps),
              eventCount: events.length,
              compressedSizeBytes: eventsToInsert.reduce((total, event) => total + event.event_size_bytes, 0),
              screenWidth: metadata.viewportWidth || 0,
              screenHeight: metadata.viewportHeight || 0,
            }
          : null;

      await this.updateSessionMetadata(siteId, sessionId, userId, identifiedUserId, metadata, batchStats, requestMeta);
    }
  }

  private async updateSessionMetadata(
    siteId: number,
    sessionId: string,
    userId: string,
    identifiedUserId: string,
    metadata: any,
    batchStats: ReplayBatchStats | null,
    requestMeta?: RequestMetadata
  ): Promise<void> {
    const lockKey = `replay-metadata-lock:${siteId}:${sessionId}`;
    const lockToken = nanoid();
    let lockAcquired = false;

    try {
      for (let attempt = 0; attempt < METADATA_LOCK_ATTEMPTS; attempt += 1) {
        if (await tryAcquireReplayMetadataLock(lockKey, lockToken, METADATA_LOCK_TTL_MS)) {
          lockAcquired = true;
          break;
        }
        await wait(METADATA_LOCK_RETRY_MS);
      }
    } catch (error) {
      console.error("Failed to acquire replay metadata lock; using event aggregate fallback:", error);
    }

    if (!lockAcquired) {
      await this.updateSessionMetadataFromEvents(siteId, sessionId, userId, identifiedUserId, metadata, requestMeta);
      return;
    }

    try {
      await this.updateSessionMetadataFromSnapshot(
        siteId,
        sessionId,
        userId,
        identifiedUserId,
        metadata,
        batchStats,
        requestMeta
      );
    } finally {
      try {
        await releaseReplayMetadataLock(lockKey, lockToken);
      } catch (error) {
        // The lock has a TTL, so a release failure must not fail an otherwise
        // durable replay write and trigger a duplicate client retry.
        console.error("Failed to release replay metadata lock:", error);
      }
    }
  }

  private async updateSessionMetadataFromSnapshot(
    siteId: number,
    sessionId: string,
    userId: string,
    identifiedUserId: string,
    metadata: any,
    batchStats: ReplayBatchStats | null,
    requestMeta?: RequestMetadata
  ): Promise<void> {
    // The previous implementation rescanned every event in the session after
    // every batch. Read only the latest narrow metadata snapshot and add this
    // batch while a cross-worker Redis lock prevents lost updates.
    const previousMetadata = await clickhouse.query({
      query: LATEST_REPLAY_METADATA_QUERY,
      query_params: { siteId, sessionId },
      format: "JSONEachRow",
    });

    const previousResults = await processResults<StoredReplayStats>(previousMetadata);
    const previous = previousResults[0];
    if (!previous && !batchStats) return;

    const previousStartMs = parseClickhouseDateTime(previous?.start_time ?? null);
    const previousEndMs = parseClickhouseDateTime(previous?.end_time ?? null) ?? previousStartMs;
    const startTimeMs = Math.min(
      previousStartMs ?? Number.POSITIVE_INFINITY,
      batchStats?.startTimeMs ?? Number.POSITIVE_INFINITY
    );
    const endTimeMs = Math.max(
      previousEndMs ?? Number.NEGATIVE_INFINITY,
      batchStats?.endTimeMs ?? Number.NEGATIVE_INFINITY
    );

    if (!Number.isFinite(startTimeMs) || !Number.isFinite(endTimeMs)) return;

    await this.insertSessionMetadata(
      siteId,
      sessionId,
      userId,
      identifiedUserId,
      metadata,
      {
        start_time: formatClickhouseDateTime(startTimeMs),
        end_time: formatClickhouseDateTime(endTimeMs),
        event_count: Number(previous?.event_count || 0) + (batchStats?.eventCount || 0),
        compressed_size_bytes: Number(previous?.compressed_size_bytes || 0) + (batchStats?.compressedSizeBytes || 0),
        screen_width: Math.max(Number(previous?.screen_width || 0), batchStats?.screenWidth || 0),
        screen_height: Math.max(Number(previous?.screen_height || 0), batchStats?.screenHeight || 0),
      },
      requestMeta
    );
  }

  private async updateSessionMetadataFromEvents(
    siteId: number,
    sessionId: string,
    userId: string,
    identifiedUserId: string,
    metadata: any,
    requestMeta?: RequestMetadata
  ): Promise<void> {
    const sessionInfo = await clickhouse.query({
      query: REPLAY_EVENT_AGGREGATE_QUERY,
      query_params: { siteId, sessionId },
      format: "JSONEachRow",
    });

    type SessionInfoResult = {
      start_time: string;
      end_time: string | null;
      event_count: number;
      compressed_size_bytes: number;
      screen_width: number | null;
      screen_height: number | null;
    };

    const sessionResults = await processResults<SessionInfoResult>(sessionInfo);

    if (!sessionResults || sessionResults.length === 0) return;

    await this.insertSessionMetadata(
      siteId,
      sessionId,
      userId,
      identifiedUserId,
      metadata,
      sessionResults[0],
      requestMeta
    );
  }

  private async insertSessionMetadata(
    siteId: number,
    sessionId: string,
    userId: string,
    identifiedUserId: string,
    metadata: any,
    sessionReplayData: StoredReplayStats,
    requestMeta?: RequestMetadata
  ): Promise<void> {
    // Parse tracking data from request metadata
    let trackingData: any = {};
    if (requestMeta?.userAgent) {
      try {
        // Extract hostname from the page URL
        const urlObj = new URL(metadata.pageUrl);
        const hostname = urlObj.hostname;

        trackingData = await parseTrackingData(
          requestMeta.userAgent,
          requestMeta.ipAddress,
          requestMeta.referrer || "",
          urlObj.search || "", // querystring from URL
          hostname,
          metadata.language || "", // language from client
          sessionReplayData.screen_width || metadata.viewportWidth || 0,
          sessionReplayData.screen_height || metadata.viewportHeight || 0
        );
      } catch (error) {
        console.error("Error parsing tracking data for session replay:", error);
      }
    }

    // Calculate duration
    const startTime = new Date(parseClickhouseDateTime(sessionReplayData.start_time) ?? 0);
    const endTimeMs = parseClickhouseDateTime(sessionReplayData.end_time);
    const endTime = endTimeMs === null ? null : new Date(endTimeMs);
    const durationMs = endTime ? endTime.getTime() - startTime.getTime() : null;

    // Insert or update metadata
    await clickhouse.insert({
      table: "session_replay_metadata",
      values: [
        {
          site_id: siteId,
          session_id: sessionId,
          user_id: userId,
          identified_user_id: identifiedUserId,
          start_time: formatClickhouseDateTime(startTime.getTime()),
          end_time: endTime ? formatClickhouseDateTime(endTime.getTime()) : null,
          duration_ms: durationMs,
          event_count: sessionReplayData.event_count || 0,
          compressed_size_bytes: sessionReplayData.compressed_size_bytes || 0,
          page_url: metadata.pageUrl || "",
          country: trackingData.country || "",
          region: trackingData.region || "",
          city: trackingData.city || "",
          lat: trackingData.lat || 0,
          lon: trackingData.lon || 0,
          browser: trackingData.browser || "",
          browser_version: trackingData.browserVersion || "",
          operating_system: trackingData.operatingSystem || "",
          operating_system_version: trackingData.operatingSystemVersion || "",
          language: trackingData.language || "",
          screen_width: sessionReplayData.screen_width || metadata?.viewportWidth || 0,
          screen_height: sessionReplayData.screen_height || metadata?.viewportHeight || 0,
          device_type: trackingData.deviceType || "",
          channel: trackingData.channel || "",
          hostname: trackingData.hostname || "",
          referrer: trackingData.referrer || "",
          has_replay_data: 1,
        },
      ],
      format: "JSONEachRow",
    });
  }
}
