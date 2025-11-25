
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
    select symbol_trade_date_key
from USER_DB_HYENA.analytics_analytics.stock_features
where symbol_trade_date_key is null
 ) dbt_internal_test