-- dim_passenger.sql

SELECT
    passenger_id, -- PK: Master Passenger ID
    passenger_type
FROM {{ ref('int_passenger_master_attributes') }}