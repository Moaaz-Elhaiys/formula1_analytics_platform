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
  payload['air_temp'] AS air_temp,
  payload['track_temp'] AS track_temp,
  payload['humidity'] AS humidity,
  payload['pressure'] AS pressure,
  payload['wind_speed'] AS wind_speed,
  payload['wind_direction'] AS wind_direction,
  payload['rainfall'] AS rainfall
FROM
  {{ source('f1_lakehouse', 'weather') }}
WHERE
  driver_number IS NULL OR driver_number IS NOT NULL
