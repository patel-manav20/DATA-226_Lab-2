
      begin;
    merge into "USER_DB_HYENA"."SNAPSHOT"."SNAPSHOT_STOCK_FEATURES" as DBT_INTERNAL_DEST
    using "USER_DB_HYENA"."SNAPSHOT"."SNAPSHOT_STOCK_FEATURES__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     
       and DBT_INTERNAL_DEST.dbt_valid_to is null
     
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("SYMBOL", "TRADE_DATE", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE", "VOLUME", "PREV_CLOSE_PRICE", "PRICE_DIFF", "DAILY_RETURN", "MA_7_CLOSE", "MA_7_VOLUME", "SYMBOL_TRADE_DATE_KEY", "UPDATED_AT", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("SYMBOL", "TRADE_DATE", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE", "VOLUME", "PREV_CLOSE_PRICE", "PRICE_DIFF", "DAILY_RETURN", "MA_7_CLOSE", "MA_7_VOLUME", "SYMBOL_TRADE_DATE_KEY", "UPDATED_AT", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;
  