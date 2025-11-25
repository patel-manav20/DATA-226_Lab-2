
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
select
    symbol_trade_date_key as unique_field,
    count(*) as n_records

from USER_DB_HYENA.analytics_analytics.stock_features
where symbol_trade_date_key is not null
group by symbol_trade_date_key
having count(*) > 1

 ) dbt_internal_test