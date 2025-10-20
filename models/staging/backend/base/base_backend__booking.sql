-- base_backend__booking.sql
-- Expose raw booking JSON data from BigQuery source table.

{{ config(
    materialized='incremental',
    unique_key='bookingid',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'uploaded_at', 'data_type': 'timestamp'},
    on_schema_change='sync'
) }}

select
  bookingid,
  raw_json,
  uploaded_at
from {{ source('backend', 'bookings') }}

{% if is_incremental() %}
  where uploaded_at >= timestamp_sub(current_timestamp(), interval 7 day)
{% endif %}