SELECT *
FROM staging.stg_resale_prices
WHERE id > {{ params.min_id }}