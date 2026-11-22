import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetBucketLifecycleConfigurationCommand,
  GetObjectCommandOutput,
  LifecycleRule,
  PutBucketLifecycleConfigurationCommand,
} from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Readable } from "stream";
import { gunzipSync } from "zlib";
import { compress as zstdCompress, decompress as zstdDecompress } from "@mongodb-js/zstd";
import { IS_CLOUD } from "../../lib/const.js";
import { createServiceLogger } from "../../lib/logger/logger.js";
import { SESSION_REPLAY_RETENTION_DAYS, SESSION_REPLAY_RETENTION_HOURS } from "../../lib/sessionReplayRetention.js";

const REPLAY_RETENTION_RULE_ID = "rybbit-session-replay-retention";

// Bounded so a slow or unreachable object store fails a replay upload quickly
// instead of holding ingest requests (and their bodies) in memory.
const S3_CONNECTION_TIMEOUT_MS = 3_000;
const S3_REQUEST_TIMEOUT_MS = 15_000;
const S3_MAX_ATTEMPTS = 2;

const DELETE_OBJECTS_CHUNK = 1000;

export interface StoredReplayBatch {
  key: string;
  sizeBytes: number;
}

class R2StorageService {
  private client: S3Client | null = null;
  private bucketName: string = "";
  private enabled: boolean = false;
  private logger = createServiceLogger("r2-storage");

  constructor() {
    const explicitlyEnabled = process.env.SESSION_REPLAY_S3_ENABLED === "true";
    const explicitlyDisabled = process.env.SESSION_REPLAY_S3_ENABLED === "false";
    const accessKeyId = process.env.SESSION_REPLAY_S3_ACCESS_KEY_ID || process.env.R2_ACCESS_KEY_ID;
    const secretAccessKey = process.env.SESSION_REPLAY_S3_SECRET_ACCESS_KEY || process.env.R2_SECRET_ACCESS_KEY;
    const legacyR2Enabled = !explicitlyDisabled && IS_CLOUD && accessKeyId && secretAccessKey;

    if ((explicitlyEnabled || legacyR2Enabled) && accessKeyId && secretAccessKey) {
      // Create a custom HTTP handler that strips checksum headers
      const httpHandler = new NodeHttpHandler({
        connectionTimeout: S3_CONNECTION_TIMEOUT_MS,
        requestTimeout: S3_REQUEST_TIMEOUT_MS,
      });
      const originalHandle = httpHandler.handle.bind(httpHandler);

      httpHandler.handle = async (request: any, options?: any) => {
        const response = await originalHandle(request, options);

        // Wrap the response to strip checksum headers
        return {
          ...response,
          response: {
            ...response.response,
            headers: Object.fromEntries(
              Object.entries(response.response.headers).filter(([key]) => !key.toLowerCase().includes("checksum"))
            ),
          },
        };
      };

      const endpoint =
        process.env.SESSION_REPLAY_S3_ENDPOINT ||
        (process.env.R2_ACCOUNT_ID ? `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com` : undefined);
      const forcePathStyle = process.env.SESSION_REPLAY_S3_FORCE_PATH_STYLE === "false" ? false : Boolean(endpoint);

      this.client = new S3Client({
        region: process.env.SESSION_REPLAY_S3_REGION || "auto",
        ...(endpoint ? { endpoint } : {}),
        credentials: {
          accessKeyId,
          secretAccessKey,
        },
        forcePathStyle,
        maxAttempts: S3_MAX_ATTEMPTS,
        // Use our custom HTTP handler
        requestHandler: httpHandler,
      });

      this.bucketName = process.env.SESSION_REPLAY_S3_BUCKET || process.env.R2_BUCKET_NAME || "rybbit";
      this.enabled = true;
      this.logger.info(
        { bucket: this.bucketName, endpoint: endpoint || "AWS" },
        "Session replay object storage initialized"
      );
    } else {
      this.logger.debug("Session replay object storage not enabled - set SESSION_REPLAY_S3_ENABLED and credentials");
    }
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  async ensureRetentionLifecycle(): Promise<void> {
    if (process.env.SESSION_REPLAY_S3_MANAGE_LIFECYCLE !== "true") return;
    if (!this.enabled || !this.client) {
      throw new Error("Cannot manage replay lifecycle because object storage is disabled");
    }

    let existingRules: LifecycleRule[] = [];
    try {
      const current = await this.client.send(new GetBucketLifecycleConfigurationCommand({ Bucket: this.bucketName }));
      existingRules = current.Rules || [];
    } catch (error: unknown) {
      const lifecycleError = error as { name?: string; Code?: string; code?: string };
      const code = lifecycleError.name || lifecycleError.Code || lifecycleError.code;
      if (code !== "NoSuchLifecycleConfiguration" && code !== "NoSuchLifecycle") throw error;
    }

    const desiredRule = {
      ID: REPLAY_RETENTION_RULE_ID,
      Status: "Enabled" as const,
      Prefix: "",
      Expiration: { Days: SESSION_REPLAY_RETENTION_DAYS },
      AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
    };
    const currentRule = existingRules.find(rule => rule.ID === REPLAY_RETENTION_RULE_ID);
    if (
      currentRule?.Status === desiredRule.Status &&
      currentRule?.Prefix === "" &&
      currentRule?.Expiration?.Days === SESSION_REPLAY_RETENTION_DAYS &&
      currentRule?.AbortIncompleteMultipartUpload?.DaysAfterInitiation === 1
    ) {
      this.logger.info(
        { bucket: this.bucketName, retentionHours: SESSION_REPLAY_RETENTION_HOURS },
        "Session replay object lifecycle is up to date"
      );
      return;
    }

    await this.client.send(
      new PutBucketLifecycleConfigurationCommand({
        Bucket: this.bucketName,
        LifecycleConfiguration: {
          Rules: [...existingRules.filter(rule => rule.ID !== REPLAY_RETENTION_RULE_ID), desiredRule],
        },
      })
    );
    this.logger.info(
      { bucket: this.bucketName, retentionHours: SESSION_REPLAY_RETENTION_HOURS },
      "Session replay object lifecycle configured"
    );
  }

  /**
   * Store a batch of event data in R2
   * Returns the storage key and stored (compressed) size, or null if R2 is disabled
   */
  async storeBatch(siteId: number, sessionId: string, eventDataArray: any[]): Promise<StoredReplayBatch | null> {
    if (!this.enabled || !this.client) {
      return null;
    }

    const timestamp = Date.now();
    const key = `${siteId}/${sessionId}/${timestamp}.json.zst`;

    // Failures propagate to the ingest service, which drops the batch and
    // reports it as unavailable storage (aggregated, without the raw error).
    // Compress with zstd - much faster decompression than brotli
    const jsonBuffer = Buffer.from(JSON.stringify(eventDataArray));
    const compressed = await zstdCompress(jsonBuffer, 3); // level 3 = good balance

    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucketName,
        Key: key,
        Body: compressed,
        ContentType: "application/octet-stream", // Binary data, not JSON
        // Don't set ContentEncoding - that triggers automatic decompression
        Metadata: {
          siteId: siteId.toString(),
          sessionId: sessionId,
          eventCount: eventDataArray.length.toString(),
          compression: "zstd",
        },
      })
    );

    return { key, sizeBytes: compressed.length };
  }

  /**
   * Delete many objects, at most 1000 per request. Never throws: the result
   * names every key that was not confirmed deleted so the caller can keep the
   * rows that reference them and retry later.
   */
  async deleteObjects(keys: string[]): Promise<{ deleted: number; failedKeys: string[] }> {
    if (!this.enabled || !this.client) {
      return { deleted: 0, failedKeys: [...keys] };
    }
    let deleted = 0;
    const failedKeys: string[] = [];
    for (let offset = 0; offset < keys.length; offset += DELETE_OBJECTS_CHUNK) {
      const chunk = keys.slice(offset, offset + DELETE_OBJECTS_CHUNK);
      try {
        const result = await this.client.send(
          new DeleteObjectsCommand({
            Bucket: this.bucketName,
            Delete: { Objects: chunk.map(Key => ({ Key })), Quiet: true },
          })
        );
        const errors = result.Errors || [];
        const errored = new Set(errors.map(error => error.Key).filter((key): key is string => !!key));
        for (const key of chunk) {
          if (errored.has(key)) failedKeys.push(key);
          else deleted += 1;
        }
        if (errors.length > 0) {
          this.logger.warn(
            { bucket: this.bucketName, failed: errors.length, firstCode: errors[0]?.Code ?? null },
            "Session replay objects not deleted"
          );
        }
      } catch (error) {
        // A request-level failure (wrong endpoint, auth, outage) will fail every
        // remaining chunk the same way; stop instead of issuing hundreds of doomed calls.
        this.logger.warn(
          {
            err: error,
            bucket: this.bucketName,
            httpStatus: (error as { $metadata?: { httpStatusCode?: number } })?.$metadata?.httpStatusCode ?? null,
            skippedKeys: keys.length - offset,
          },
          "Session replay object batch delete failed; aborting remaining chunks"
        );
        failedKeys.push(...keys.slice(offset));
        break;
      }
    }
    return { deleted, failedKeys };
  }

  /**
   * Retrieve a batch of event data from R2
   * Returns the decompressed event data array
   */
  async getBatch(key: string): Promise<any[]> {
    if (!this.enabled || !this.client) {
      throw new Error("R2 storage is not enabled");
    }

    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucketName,
          Key: key,
        })
      );

      if (!response.Body) {
        throw new Error("Empty response body");
      }

      // Convert stream to buffer
      const chunks: Uint8Array[] = [];
      const stream = response.Body as Readable;

      for await (const chunk of stream) {
        chunks.push(chunk);
      }

      const buffer = Buffer.concat(chunks);

      // Check if data is already JSON (uncompressed)
      // This happens when ContentEncoding was set to "zstd" and R2 auto-decompressed it
      const bufferStr = buffer.toString("utf8", 0, Math.min(buffer.length, 100));
      const isLikelyJSON = bufferStr.trimStart().startsWith("[") || bufferStr.trimStart().startsWith("{");

      if (isLikelyJSON) {
        try {
          // Try parsing as JSON first
          return JSON.parse(buffer.toString());
        } catch (e) {
          // Not valid JSON, proceed with decompression
        }
      }

      // Try to decompress based on file extension
      let decompressed: Buffer;

      try {
        if (key.endsWith(".zst")) {
          decompressed = await zstdDecompress(buffer);
        } else if (key.endsWith(".gz")) {
          decompressed = gunzipSync(buffer);
        } else {
          // Assume zstd for unknown extensions
          decompressed = await zstdDecompress(buffer);
        }
        return JSON.parse(decompressed.toString());
      } catch (decompressionError: any) {
        // If decompression fails and we haven't tried JSON yet, try it now
        if (!isLikelyJSON) {
          try {
            return JSON.parse(buffer.toString());
          } catch (jsonError) {
            // Data is truly corrupted, throw the original error
            throw decompressionError;
          }
        }
        throw decompressionError;
      }
    } catch (error) {
      console.error("[ReplayStorage] Failed to retrieve batch:", error);
      throw error;
    }
  }

  /**
   * Delete a batch from R2 (for cleanup)
   */
  async deleteBatch(key: string): Promise<void> {
    if (!this.enabled || !this.client) {
      return;
    }

    try {
      await this.client.send(
        new DeleteObjectCommand({
          Bucket: this.bucketName,
          Key: key,
        })
      );
    } catch (error) {
      console.error("[ReplayStorage] Failed to delete batch:", error);
      // Non-critical error, log but don't throw
    }
  }
}

// Singleton instance
export const r2Storage = new R2StorageService();
