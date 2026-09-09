/**
 * Replay payloads can contain large DOM snapshots. Keep this override scoped
 * to the ingest route so unrelated API endpoints retain Fastify's lower global
 * limit.
 */
export const SESSION_REPLAY_BODY_LIMIT_BYTES = 50 * 1024 * 1024;
