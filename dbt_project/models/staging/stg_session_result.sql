SELECT
  schema_version,
  source_endpoint,
  meeting_key,
  session_key,
  driver_number,
  event_time,
  ingest_time,
  replay_run_id,
  payload,
  -- Nested payload columns
  payload['position'] AS position,
  payload['points'] AS points,
  payload['status'] AS status,
  payload['classification'] AS classification,
  payload['grid_position'] AS grid_position,
  payload['num_laps'] AS num_laps,
  payload['fastest_lap_rank'] AS fastest_lap_rank,
  payload['fastest_lap_time'] AS fastest_lap_time
FROM
  {{ source('f1_lakehouse', 'session_result') }}
WHERE
  driver_number IS NOT NULL
