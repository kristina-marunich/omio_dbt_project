-- dim_passenger.sql

SELECT
    distinct 
    passenger_id, -- PK
    passenger_type
FROM {{ ref('stg_backend__passenger') }}