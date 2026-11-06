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
  payload['lap_number'] AS lap_number,
  payload['lap_duration'] AS lap_duration,
  payload['sector_1_duration'] AS sector_1_duration,
  payload['sector_2_duration'] AS sector_2_duration,
  payload['sector_3_duration'] AS sector_3_duration,
  payload['speed_trap'] AS speed_trap,
  payload['st_speed'] AS st_speed,
  payload['date_start'] AS date_start,
  payload['is_pit_out_lap'] AS is_pit_out_lap,
  payload['compound'] AS compound,
  payload['tyre_life'] AS tyre_life,
  payload['fresh_tyre'] AS fresh_tyre
FROM
  {{ source('f1_lakehouse', 'laps') }}
WHERE
  driver_number IS NOT NULL