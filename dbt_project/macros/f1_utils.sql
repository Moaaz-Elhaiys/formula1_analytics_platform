{{ config(materialized='ephemeral') }}

{% macro f1_partition_cols() %}
  ['year', 'month', 'day', 'hour']
{% endmacro %}

-- Usage example in other macros:
-- {{ f1_partition_cols() }}