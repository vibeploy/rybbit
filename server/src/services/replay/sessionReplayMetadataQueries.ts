export const LATEST_REPLAY_METADATA_QUERY = `
  SELECT
    start_time,
    end_time,
    event_count,
    compressed_size_bytes,
    screen_width,
    screen_height
  FROM session_replay_metadata
  PREWHERE site_id = {siteId:UInt16}
    AND session_id = {sessionId:String}
  ORDER BY created_at DESC, event_count DESC
  LIMIT 1
`;

export const REPLAY_EVENT_AGGREGATE_QUERY = `
  SELECT
    MIN(timestamp) as start_time,
    MAX(timestamp) as end_time,
    COUNT() as event_count,
    SUM(event_size_bytes) as compressed_size_bytes,
    MAX(viewport_width) as screen_width,
    MAX(viewport_height) as screen_height
  FROM session_replay_events
  WHERE site_id = {siteId:UInt16}
    AND session_id = {sessionId:String}
`;
