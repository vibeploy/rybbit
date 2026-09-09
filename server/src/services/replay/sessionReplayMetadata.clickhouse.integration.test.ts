import { createClient, type ClickHouseClient } from "@clickhouse/client";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { LATEST_REPLAY_METADATA_QUERY } from "./sessionReplayMetadataQueries.js";

const clickhouseUrl = process.env.TEST_CLICKHOUSE_URL;
const describeWithClickhouse = clickhouseUrl ? describe : describe.skip;
const table = "session_replay_metadata_query_integration";

describeWithClickhouse("latest replay metadata query against ClickHouse", () => {
  let client: ClickHouseClient;
  const query = LATEST_REPLAY_METADATA_QUERY.replace("session_replay_metadata", table);

  beforeAll(async () => {
    client = createClient({ url: clickhouseUrl });
    await client.command({ query: `DROP TABLE IF EXISTS ${table}` });
    await client.command({
      query: `
        CREATE TABLE ${table} (
          site_id UInt16,
          session_id String,
          start_time DateTime,
          end_time Nullable(DateTime),
          event_count UInt32,
          compressed_size_bytes UInt32,
          screen_width UInt16,
          screen_height UInt16,
          created_at DateTime
        )
        ENGINE = ReplacingMergeTree(created_at)
        ORDER BY (site_id, session_id)
      `,
    });
  });

  afterAll(async () => {
    if (!client) return;
    await client.command({ query: `DROP TABLE IF EXISTS ${table}` });
    await client.close();
  });

  beforeEach(async () => {
    await client.command({ query: `TRUNCATE TABLE ${table}` });
  });

  async function latest() {
    const result = await client.query({
      query,
      query_params: { siteId: 42, sessionId: "session-1" },
      format: "JSONEachRow",
    });
    return (await result.json<Record<string, string | number | null>>())[0];
  }

  async function insertSnapshots(includeNewest = true) {
    await client.insert({
      table,
      values: [
        {
          site_id: 42,
          session_id: "session-1",
          start_time: "2026-09-09 15:00:00",
          end_time: "2026-09-09 15:00:05",
          event_count: 4,
          compressed_size_bytes: 100,
          screen_width: 1024,
          screen_height: 768,
          created_at: "2026-09-09 15:01:00",
        },
      ],
      format: "JSONEachRow",
    });

    await client.insert({
      table,
      values: [
        {
          site_id: 42,
          session_id: "session-1",
          start_time: "2026-09-09 15:00:00",
          end_time: "2026-09-09 15:00:10",
          event_count: 6,
          compressed_size_bytes: 160,
          screen_width: 1440,
          screen_height: 900,
          created_at: "2026-09-09 15:01:00",
        },
      ],
      format: "JSONEachRow",
    });

    if (includeNewest) {
      await client.insert({
        table,
        values: [
          {
            site_id: 42,
            session_id: "session-1",
            start_time: "2026-09-09 15:00:00",
            end_time: "2026-09-09 15:00:15",
            event_count: 8,
            compressed_size_bytes: 220,
            screen_width: 1440,
            screen_height: 900,
            created_at: "2026-09-09 15:01:01",
          },
        ],
        format: "JSONEachRow",
      });
    }
  }

  it("selects the newest snapshot and breaks same-second ties by cumulative count", async () => {
    await insertSnapshots(false);

    expect(await latest()).toMatchObject({ event_count: 6, compressed_size_bytes: 160 });
  });

  it("returns the same cumulative snapshot after a ReplacingMergeTree merge", async () => {
    await insertSnapshots();
    await client.command({ query: `OPTIMIZE TABLE ${table} FINAL` });
    expect(await latest()).toMatchObject({ event_count: 8, compressed_size_bytes: 220 });
  });

  it("keeps the later cumulative snapshot when two inserts share one-second created_at precision", async () => {
    await insertSnapshots(false);
    await client.command({ query: `OPTIMIZE TABLE ${table} FINAL` });
    expect(await latest()).toMatchObject({ event_count: 6, compressed_size_bytes: 160 });
  });
});
