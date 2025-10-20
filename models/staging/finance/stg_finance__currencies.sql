{{ config(materialized = 'table') }}

select * from unnest(array<struct<
    date date,
    from_currency string,
    to_currency string,
    rate numeric
>>[
    (date '2025-10-01', 'EUR', 'EUR', 1.00),
    (date '2025-10-01', 'USD', 'EUR', 0.93),
    (date '2025-10-01', 'JPY', 'EUR', 0.0062),
    (date '2025-10-01', 'GBP', 'EUR', 1.17),
    (date '2025-10-02', 'EUR', 'EUR', 1.00),
    (date '2025-10-02', 'USD', 'EUR', 0.935),
    (date '2025-10-02', 'JPY', 'EUR', 0.00625),
    (date '2025-10-02', 'GBP', 'EUR', 1.16),
    (date '2025-10-03', 'EUR', 'EUR', 1.00),
    (date '2025-10-03', 'USD', 'EUR', 0.94),
    (date '2025-10-03', 'JPY', 'EUR', 0.0063),
    (date '2025-10-03', 'GBP', 'EUR', 1.15),
    (date '2025-10-04', 'EUR', 'EUR', 1.00),
    (date '2025-10-04', 'USD', 'EUR', 0.94),
    (date '2025-10-04', 'JPY', 'EUR', 0.00635),
    (date '2025-10-04', 'GBP', 'EUR', 1.14),
    (date '2025-10-05', 'EUR', 'EUR', 1.00),
    (date '2025-10-05', 'USD', 'EUR', 0.945),
    (date '2025-10-05', 'JPY', 'EUR', 0.0064),
    (date '2025-10-05', 'GBP', 'EUR', 1.13),
    (date '2025-10-06', 'EUR', 'EUR', 1.00),
    (date '2025-10-06', 'USD', 'EUR', 0.95),
    (date '2025-10-06', 'JPY', 'EUR', 0.00645),
    (date '2025-10-06', 'GBP', 'EUR', 1.12),
    (date '2025-10-07', 'EUR', 'EUR', 1.00),
    (date '2025-10-07', 'USD', 'EUR', 0.955),
    (date '2025-10-07', 'JPY', 'EUR', 0.0065),
    (date '2025-10-07', 'GBP', 'EUR', 1.11)
])