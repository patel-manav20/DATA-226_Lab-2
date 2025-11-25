with __dbt__cte__stg_stock_prices as (
-- models/input/stg_stock_prices.sql
-- Staging model on top of RAW.STOCK_PRICES (ephemeral)

SELECT
    SYMBOL AS symbol,
    DATE   AS trade_date,
    OPEN   AS open_price,
    HIGH   AS high_price,
    LOW    AS low_price,
    CLOSE  AS close_price,
    VOLUME AS volume
FROM USER_DB_HYENA.raw.STOCK_PRICES
) select symbol
from __dbt__cte__stg_stock_prices
where symbol is null


