-- base_backend__booking.sql
-- Expose raw booking JSON data from BigQuery source table.

select
  bookingid,
  raw_json,
  uploaded_at
from {{ source('backend', 'bookings') }}