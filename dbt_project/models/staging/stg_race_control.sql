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
  payload['date'] AS date,
  payload['category'] AS category,
  payload['flag'] AS flag,
  payload['scope'] AS scope,
  payload['sector'] AS sector,
  payload['message'] AS message
FROM
  {{ source('f1_lakehouse', 'race_control') }}
WHERE
  driver_number IS NULL OR driver_number IS NOT NULL
