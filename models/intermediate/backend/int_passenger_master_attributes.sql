-- models/intermediate/int_passenger_master_attributes.sql


SELECT
    passenger_id, -- PK: Master ID for the Person
    MAX(passenger_type) AS passenger_type
FROM {{ ref('stg_backend__passenger') }}
GROUP BY 1