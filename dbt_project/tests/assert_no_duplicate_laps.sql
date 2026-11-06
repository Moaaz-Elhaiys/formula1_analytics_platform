SELECT
  lap_number,
  driver_number,
  COUNT(*) as duplicate_count
FROM
  {{ ref('stg_laps') }}
GROUP BY
  driver_number,
  lap_number
HAVING
  COUNT(*) > 1