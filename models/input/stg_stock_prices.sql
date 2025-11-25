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
FROM {{ source('raw', 'stock_prices') }}
