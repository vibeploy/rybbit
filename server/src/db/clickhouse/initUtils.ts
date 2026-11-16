import { createServiceLogger } from "../../lib/logger/logger.js";
import { CLICKHOUSE_REQUEST_TIMEOUT_MS, clickhouse } from "./client.js";

export const clickhouseInitLogger = createServiceLogger("clickhouse");

type ClickhouseInitStepOptions = { optional?: boolean; lockAcquireTimeoutSeconds?: number };

// ClickHouse defaults to materialize_ttl_after_modify=1, which turns MODIFY TTL into a
// mutation that rewrites every existing part. On a large replay table that mutation
// exhausted server memory (2026-11-16). TTL changes must go through
// modifyClickhouseTableTtl so expired rows are removed by background TTL merges instead.
const MODIFY_TTL_PATTERN = /\bMODIFY\s+TTL\b/i;
const TABLE_IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

async function runClickhouseInitStep(
  step: string,
  query: string,
  options: ClickhouseInitStepOptions | undefined,
  settings: { materialize_ttl_after_modify?: 0 }
) {
  const clickhouseSettings = {
    ...settings,
    ...(options?.lockAcquireTimeoutSeconds ? { lock_acquire_timeout: options.lockAcquireTimeoutSeconds } : {}),
  };
  try {
    await clickhouse.exec({
      query,
      clickhouse_settings: Object.keys(clickhouseSettings).length > 0 ? clickhouseSettings : undefined,
    });
  } catch (error) {
    clickhouseInitLogger.error(
      { err: error, step, requestTimeoutMs: CLICKHOUSE_REQUEST_TIMEOUT_MS },
      "ClickHouse initialization step failed"
    );
    if (!options?.optional) {
      throw error;
    }
  }
}

export async function execClickhouseInitStep(step: string, query: string, options?: ClickhouseInitStepOptions) {
  if (MODIFY_TTL_PATTERN.test(query)) {
    throw new Error(`ClickHouse init step "${step}" must use modifyClickhouseTableTtl for MODIFY TTL`);
  }
  await runClickhouseInitStep(step, query, options, {});
}

export async function modifyClickhouseTableTtl(
  step: string,
  table: string,
  ttlExpression: string,
  options?: ClickhouseInitStepOptions
) {
  if (!TABLE_IDENTIFIER_PATTERN.test(table)) {
    throw new Error(`ClickHouse init step "${step}" has an invalid table identifier`);
  }
  await runClickhouseInitStep(step, `ALTER TABLE ${table} MODIFY TTL ${ttlExpression}`, options, {
    materialize_ttl_after_modify: 0,
  });
}

export async function getTableColumns(table: string) {
  const result = await clickhouse.query({
    query: `
      SELECT name
      FROM system.columns
      WHERE database = currentDatabase()
        AND table = {table:String}
    `,
    query_params: { table },
    format: "JSONEachRow",
  });

  const rows = await result.json<{ name: string }>();
  return new Set(rows.map(row => row.name));
}

export async function getTableCreateQuery(table: string) {
  const result = await clickhouse.query({
    query: `
      SELECT create_table_query
      FROM system.tables
      WHERE database = currentDatabase()
        AND name = {table:String}
      LIMIT 1
    `,
    query_params: { table },
    format: "JSONEachRow",
  });

  const rows = await result.json<{ create_table_query: string }>();
  return rows[0]?.create_table_query;
}
