-- models/output/stock_features.sql
-- Final analytics table with derived metrics for each (symbol, trade_date)

WITH base AS (
    SELECT
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume
    FROM {{ ref('stg_stock_prices') }}   -- from input layer (ephemeral)
),

features AS (
    SELECT
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,

        -- Previous day's close
        LAG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
        ) AS prev_close_price,

        -- Price difference vs previous day
        close_price - LAG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
        ) AS price_diff,

        -- Daily return (%)
        CASE
            WHEN LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date) IS NULL
                THEN NULL
            ELSE (close_price
                  / LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date)
                 ) - 1
        END AS daily_return,

        -- 7-day moving average of close price
        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7_close,

        -- 7-day moving average of volume
        AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7_volume,

        -- Surrogate key for snapshots & tests
        CONCAT(symbol, '_', TO_CHAR(trade_date, 'YYYY-MM-DD')) AS symbol_trade_date_key,

        -- Updated timestamp for snapshot strategy=timestamp
        CURRENT_TIMESTAMP() AS updated_at

    FROM base
)

SELECT * FROM features
