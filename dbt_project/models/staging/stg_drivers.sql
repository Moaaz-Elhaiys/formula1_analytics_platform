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
  payload['broadcast_name'] AS broadcast_name,
  payload['full_name'] AS full_name,
  payload['name_acronym'] AS name_acronym,
  payload['team_name'] AS team_name,
  payload['team_colour'] AS team_colour,
  payload['headshot_url'] AS headshot_url,
  payload['country_code'] AS country_code
FROM
  {{ source('f1_lakehouse', 'drivers') }}
WHERE
  driver_number IS NOT NULL
