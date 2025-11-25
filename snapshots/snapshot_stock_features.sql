{% snapshot snapshot_stock_features %}

{{
  config(
    target_schema='SNAPSHOT',
    unique_key='symbol_trade_date_key',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
  )
}}

select *
from {{ ref('stock_features') }}

{% endsnapshot %}
