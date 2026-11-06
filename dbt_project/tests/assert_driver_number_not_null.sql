{{ config(materialized='test') }}

{% set table_name = 'stg_laps' %}
{% set column_name = 'driver_number' %}

SELECT
  COUNT(*) as null_count
FROM
  {{ ref(table_name) }}
WHERE
  {{ column_name }} IS NULL